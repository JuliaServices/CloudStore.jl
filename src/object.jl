import CloudBase: AbstractStore, CloudCredentials, AWS, Azure

const DEFAULT_PREFETCH_SIZE = 32 * 1024 * 1024
const DEFAULT_PREFETCH_MULTIPART_SIZE = 8 * 1024 * 1024

struct Object{T <: AbstractStore}
    store::T
    credentials::Union{Nothing, AWS.Credentials, Azure.Credentials}
    key::String
    size::Int
    eTag::String
end

Object(
    store::AbstractStore,
    creds::Union{Nothing, AWS.Credentials, Azure.Credentials},
    key::AbstractString,
    size::Integer,
    eTag::AbstractString) = Object(store, creds, String(key), Int(size), String(eTag))

function Object(store::AbstractStore, key::String; credentials::Union{CloudCredentials, Nothing}=nothing)
    url = makeURL(store, key)
    resp = API.headObject(store, url, HTTP.Headers(); credentials=credentials)
    # The ArgumentError will be caused by the HTTP error to provide more context
    if HTTP.isredirect(resp)
        try
            throw(HTTP.StatusError(resp.status, resp.request.method, resp.request.target, resp))
        catch
            throw(ArgumentError("Invalid object key: $key"))
        end
    end
    size = parse(Int, HTTP.header(resp, "Content-Length", "0"))
    #TODO: get eTag
    et = etag(HTTP.header(resp, "ETag", ""))
    return Object(store, credentials, key, size, String(et))
end

Base.length(x::Object) = x.size

function Base.copyto!(dest::AbstractVector{UInt8}, doff::Integer, src::Object, soff::Integer, n::Integer)
    # validate arguments
    0 < doff <= length(dest) || throw(BoundsError(dest, doff))
    0 < soff <= length(src) || throw(BoundsError(src, soff))
    (soff + n) - 1 <= length(src) || throw(ArgumentError("requested number of bytes (`$n`) would exceed source length"))
    (doff + n) - 1 <= length(dest) || throw(ArgumentError("requested number of bytes (`$n`) would exceed destination length"))
    return unsafe_copyto!(dest, doff, src, soff, n)
end

function getRange(src::Object, soff::Integer, n::Integer)
    headers = HTTP.Headers()
    HTTP.setheader(headers, contentRange((soff - 1):(soff + n - 2)))
    url = makeURL(src.store, src.key)
    return getObject(src.store, url, headers; credentials=src.credentials).body
end

function Base.unsafe_copyto!(dest::AbstractVector{UInt8}, doff::Integer, src::Object, soff::Integer, n::Integer)
    copyto!(dest, doff, getRange(src, soff, n))
    return n
end

mutable struct TaskCondition
    cond_wait::Threads.Condition
    ntasks::Int
end
TaskCondition() = TaskCondition(Threads.Condition(), 0)
mutable struct PrefetchBuffer
    data::Vector{UInt8}
    pos::Int
    len::Int
end
Base.bytesavailable(b::PrefetchBuffer) = b.len - b.pos + 1

function _prefetching_task(io)
    prefetch_size = io.prefetch_size
    len = io.len
    ppos = 0
    download_buffer = Vector{UInt8}(undef, prefetch_size)
    # Don't allocate the second buffer when we can fit everything to the first one
    download_buffer_next = len == prefetch_size ? download_buffer : Vector{UInt8}(undef, prefetch_size)

    try
        while len > ppos
            n = min(len - ppos, prefetch_size)
            off = 0
            # `partition` plays nicely with 1-based ranges, but we need zero based ranges for
            # `contentRange`
            rngs = Iterators.partition(ppos+1:ppos+n, io.prefetch_multipart_size)
            io.cond.ntasks = length(rngs)
            for rng in rngs
                put!(io.download_queue, (off, rng .- 1, download_buffer))
                off += length(rng)
            end
            Base.@lock io.cond.cond_wait begin
                while true
                    io.cond.ntasks == 0 && break
                    wait(io.cond.cond_wait)
                end
            end

            buf = PrefetchBuffer(download_buffer, 1, n)
            ppos += n
            put!(io.prefetch_queue, buf)
            download_buffer, download_buffer_next = download_buffer_next, download_buffer
        end
    catch e
        close(io.download_queue, e)
        close(io.prefetch_queue, e)
        rethrow()
    end
    close(io.download_queue)
    close(io.prefetch_queue)
    return nothing
end

function _download_task(io)
    headers = HTTP.Headers()
    object = io.object
    url = makeURL(object.store, io.object.key)
    credentials = object.credentials
    response_stream = IOBuffer(view(UInt8[], 1:0), write=true, maxsize=io.prefetch_multipart_size)

    try
        while true
            (off, rng, download_buffer) = take!(io.download_queue)
            HTTP.setheader(headers, contentRange(rng))
            buffer_view = view(download_buffer, off + 1:off + length(rng))
            response_stream.data = buffer_view
            response_stream.maxsize = length(buffer_view)
            seekstart(response_stream)
            _ = getObject(object.store, url, headers; credentials, response_stream)

            Base.@lock io.cond.cond_wait begin
                io.cond.ntasks -= 1
                notify(io.cond.cond_wait)
            end
        end
    catch e
        isopen(io.prefetch_queue) && close(io.prefetch_queue, e)
        isopen(io.download_queue) && close(io.download_queue, e)
        Base.@lock io.cond.cond_wait begin
            notify(io.cond.cond_wait, e, all=true, error=true)
        end
    end
    return nothing
end

mutable struct PrefetchedDownloadStream{T <: Object} <: IO
    object::T
    pos::Int
    len::Int
    buf::Union{Nothing,PrefetchBuffer}
    prefetch_size::Int
    prefetch_multipart_size::Int
    prefetch_queue::Channel{PrefetchBuffer}
    download_queue::Channel{Tuple{Int,UnitRange{Int},Vector{UInt8}}}
    cond::TaskCondition

    function PrefetchedDownloadStream(
        object::T,
        prefetch_size::Int=DEFAULT_PREFETCH_SIZE;
        prefetch_multipart_size::Int=DEFAULT_PREFETCH_MULTIPART_SIZE
    ) where {T<:Object}
        len = length(object)
        size = min(prefetch_size, len)
        io = new{T}(
            object,
            1,
            len,
            nothing,
            size,
            min(size, prefetch_multipart_size),
            Channel{PrefetchBuffer}(0),
            Channel{Tuple{Int,UnitRange{Int},Vector{UInt8}}}(Inf),
            TaskCondition()
        )
        prefetch_size > 0 || throw(ArgumentError("`prefetch_size` must be positive, got $prefetch_size"))
        prefetch_multipart_size > 0 || throw(ArgumentError("`prefetch_multipart_size` must be positive, got $prefetch_multipart_size"))
        if size > 0
            for _ in 1:min(Threads.nthreads(), max(1, div(size, io.prefetch_multipart_size)))
                Threads.@spawn _download_task($io)
            end
            Threads.@spawn _prefetching_task($io)
        else
            close(io.download_queue)
            close(io.prefetch_queue)
            io.buf = PrefetchBuffer(UInt8[], 1, 0)
        end
        return io
    end

    function PrefetchedDownloadStream(
        store::AbstractStore,
        key::String,
        prefetch_size::Int=DEFAULT_PREFETCH_SIZE;
        credentials::Union{CloudCredentials, Nothing}=nothing,
        prefetch_multipart_size::Int=DEFAULT_PREFETCH_MULTIPART_SIZE,
    )
        url = makeURL(store, key)
        resp = API.headObject(store, url, HTTP.Headers(); credentials=credentials)
        # The ArgumentError will be caused by the HTTP error to provide more context
        if HTTP.isredirect(resp)
            try
                throw(HTTP.StatusError(resp.status, resp.request.method, resp.request.target, resp))
            catch
                throw(ArgumentError("Invalid object key: $key"))
            end
        end
        len = parse(Int, HTTP.header(resp, "Content-Length", "0"))
        et = etag(HTTP.header(resp, "ETag", ""))
        object = Object(store, credentials, String(key), Int(len), String(et))
        return PrefetchedDownloadStream(object, prefetch_size; prefetch_multipart_size)
    end
end
Base.eof(io::PrefetchedDownloadStream) = io.pos >= io.len
bytesremaining(io::PrefetchedDownloadStream) = io.len - io.pos + 1
function Base.bytesavailable(io::PrefetchedDownloadStream)
    return isnothing(io.buf) ? 0 : bytesavailable(io.buf::PrefetchBuffer)
end
function Base.close(io::PrefetchedDownloadStream)
    close(io.prefetch_queue)
    close(io.download_queue)
    Base.@lock io.cond.cond_wait begin
        Base.notify_error(io.cond.cond_wait, Base.closed_exception())
    end
    if !isnothing(io.buf)
        resize!(io.buf.data, 0)
        io.buf = nothing
    end
    return nothing
end
Base.isopen(io::PrefetchedDownloadStream) = !(isnothing(io.buf) && !isopen(io.prefetch_queue))
Base.iswritable(io::PrefetchedDownloadStream) = false
Base.filesize(io::PrefetchedDownloadStream) = io.len
function Base.peek(io::PrefetchedDownloadStream, ::Type{T}) where {T<:Integer}
    eof(io) && throw(EOFError())
    buf = getbuffer(io)
    # TODO: allow cross-buffer peeking
    bytesavailable(buf) < sizeof(T) && error(string(
        "Cannot peak ahead $(sizeof(T)) bytes for T=$T, there are only",
        "$(bytesavailable(buf)) bytes in the current buffer."
    ))
    GC.@preserve buf begin
        ptr::Ptr{T} = pointer(buf.data, buf.pos)
        x = unsafe_load(ptr)
    end
    return x
end

function getbuffer(io::PrefetchedDownloadStream)
    buf = io.buf
    if isnothing(buf)
        buf = take!(io.prefetch_queue)
        io.buf = buf
    end
    return buf
end

function _unsafe_read(io::PrefetchedDownloadStream, dest::Ptr{UInt8}, bytes_to_read::Int)
    bytes_read = 0
    while bytes_to_read > bytes_read
        buf = getbuffer(io)
        bytes_in_buffer = bytesavailable(buf)

        adv = min(bytes_in_buffer, bytes_to_read - bytes_read)
        GC.@preserve buf unsafe_copyto!(dest + bytes_read, pointer(buf.data, buf.pos), adv)
        buf.pos += adv
        bytes_read += adv
        io.pos += adv

        if buf.pos > buf.len
            io.buf = nothing
        end
    end
    return bytes_read
end

function Base.readbytes!(io::PrefetchedDownloadStream, dest::AbstractVector{UInt8}, n)
    eof(io) && return UInt32(0)
    bytes_to_read = min(bytesremaining(io), Int(n))
    bytes_to_read > length(dest) && resize!(dest, bytes_to_read)
    bytes_read = GC.@preserve dest _unsafe_read(io, pointer(dest), bytes_to_read)
    return UInt32(bytes_read)
end

function Base.unsafe_read(io::PrefetchedDownloadStream, p::Ptr{UInt8}, nb::UInt)
    if eof(io)
        nb > 0 && throw(EOFError())
        return nothing
    end
    avail = bytesremaining(io)
    _unsafe_read(io, p, min(avail, Int(nb)))
    nb > avail && throw(EOFError())
    return nothing
end

# TranscodingStreams.jl are calling this method when Base.bytesavailable is zero
# to trigger buffer refill
function Base.read(io::PrefetchedDownloadStream, ::Type{UInt8})
    eof(io) && throw(EOFError())
    buf = getbuffer(io)
    @inbounds b = buf.data[buf.pos]
    buf.pos += 1
    io.pos += 1

    if buf.pos > buf.len
        io.buf = nothing
    end
    return b
end
