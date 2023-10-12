import CloudBase: AbstractStore, CloudCredentials, AWS, Azure

const DEFAULT_PREFETCH_SIZE = 32 * 1024 * 1024
const DEFAULT_PREFETCH_MULTIPART_SIZE = 8 * 1024 * 1024

struct Object{T <: AbstractStore}
    store::T
    credentials::Union{Nothing, AWS.Credentials, Azure.Credentials}
    key::String
    size::Int
    eTag::String
    properties::Dict{String, Any}
end

Object(
    store::AbstractStore,
    creds::Union{Nothing, AWS.Credentials, Azure.Credentials},
    key::AbstractString,
    size::Integer,
    eTag::AbstractString,
    properties::Dict{String, Any} = Dict{String, Any}()) =
            Object(store, creds, String(key), Int(size), String(eTag), properties)

function Object(store::AbstractStore, key::String; credentials::Union{CloudCredentials, Nothing}=nothing, kw...)
    url = makeURL(store, key)
    resp = API.headObject(store, url, HTTP.Headers(); credentials=credentials, kw...)
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

function getRange(src::Object, soff::Integer, n::Integer; kw...)
    headers = HTTP.Headers()
    HTTP.setheader(headers, contentRange((soff - 1):(soff + n - 2)))
    url = makeURL(src.store, src.key)
    return getObject(src.store, url, headers; credentials=src.credentials, kw...).body
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
        isopen(io.download_queue) && close(io.download_queue, e)
        isopen(io.prefetch_queue) && close(io.prefetch_queue, e)
        rethrow()
    end
    close(io.download_queue)
    close(io.prefetch_queue)
    return nothing
end

function _download_task(io; kw...)
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
            _ = getObject(object.store, url, headers; credentials, response_stream, kw...)

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

# assumes part_size > 0
_ndownload_tasks(total_size, part_size, numthreads=Threads.nthreads()) =
    min(numthreads, max(1, div(total_size, part_size, RoundUp)))

"""
    PrefetchedDownloadStream{T <: Object} <: IO
    PrefetchedDownloadStream(args...; kwargs...) -> PrefetchedDownloadStream{T <: Object}

A buffered, read-only, in-memory IO stream that fetches chunks from remote cloud `Object`.

Data is downloaded to two internal buffers. Once you start reading from the first buffer,
a secondary buffer will begin to be "prefetched" by multiple background tasks. Once you read
past the first buffer, the two buffers are switched and new round of prefetching begins.

To control memory usage and speed, the user can change two parameters when constructing the
stream: `prefetch_multipart_size` and `prefetch_size`. `prefetch_multipart_size` is the max
size of any individual GET request in bytes (default $(Base.format_bytes(DEFAULT_PREFETCH_MULTIPART_SIZE))),
`prefetch_size` is the size of a buffer that stores the fetched bytes and which is iterated
when we consume/read the IO (default $(Base.format_bytes(DEFAULT_PREFETCH_SIZE))).

The number of spawned tasks is also governed by these two parameters, with approximately
`prefetch_size` / `prefetch_multipart_size` tasks spawned for performing the GET requests
(defaults to 4 if those fields aren't specified) + 1 task is spawned to coordinate the
prefetching process. Number of spawned tasks is upper-bounded by the size of the input and
the number of threads available (see [`_ndownload_tasks`](@ref) helper function).

**Reading from this stream is not thread-safe**.

# Arguments
* `store::AbstractStore`: The S3 Bucket / Azure Container object
* `key::String`: S3 key / Azure blob resource name
* `prefetch_size::Int=DEFAULT_PREFETCH_SIZE`: The size of each of the two internal prefetch
    buffers in bytes

# Keywords
* `credentials::Union{CloudCredentials, Nothing}=nothing`: Credentials object used in HTTP
    requests
* `prefetch_multipart_size::Int=DEFAULT_PREFETCH_MULTIPART_SIZE`: The size of each individual
   GET request in bytes
* `kwargs...`: HTTP keyword arguments are forwarded to underlying HTTP requests,

## Examples
```
# Get an IO stream for a remote CSV file `test.csv` living in your S3 bucket
io = PrefetchedDownloadStream(my_bucket, "test.csv"; credentials)

# Integrates with TranscodingStreams; HTTP keyword arguments are forwarded to underlying HTTP requests
using CodecZlib
io = GzipDecompressorStream(
    PrefetchedDownloadStream(my_bucket, "test.csv.gz"; credentials, retries=5)
)

# Up to 8 concurrent download tasks, each fetching 2MiB into 16MiB prefetch buffer.
io = PrefetchedDownloadStream(
    my_bucket, "test.csv", 16*1024*1024; credentials, prefetch_multipart_size=2*1024*1024)
)
```
"""
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
        prefetch_multipart_size::Int=DEFAULT_PREFETCH_MULTIPART_SIZE,
        kw...
    ) where {T<:Object}
        prefetch_size > 0 || throw(ArgumentError("`prefetch_size` must be positive, got $prefetch_size"))
        prefetch_multipart_size > 0 || throw(ArgumentError("`prefetch_multipart_size` must be positive, got $prefetch_multipart_size"))
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
        if size > 0
            for _ in 1:_ndownload_tasks(size, io.prefetch_multipart_size)
                Threads.@spawn _download_task($io; $kw...)
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
        kw...,
    )
        url = makeURL(store, key)
        resp = API.headObject(store, url, HTTP.Headers(); credentials=credentials, kw...)
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
        return PrefetchedDownloadStream(object, prefetch_size; prefetch_multipart_size, kw...)
    end
end

mutable struct MultipartUploadStream <: IO
    store::AbstractStore
    url::String
    credentials::Union{Nothing, AWS.Credentials, Azure.Credentials}
    uploadState
    sync::OrderedSynchronizer
    eTags::Vector{String}
    @atomic cur_part_id::Int

    function MultipartUploadStream(
        store::AbstractStore,
        key::String;
        credentials::Union{CloudCredentials, Nothing}=nothing,
        kw...
    )
        url = makeURL(store, key)
        uploadState = API.startMultipartUpload(store, key; credentials, kw...)
        return new(
            store,
            url,
            credentials,
            uploadState,
            OrderedSynchronizer(1),
            Vector{String}(),
            1
        )
    end
end

function Base.write(x::MultipartUploadStream, bytes::Vector{UInt8}; kw...)
    # upload the part
    @show x.cur_part_id
    parteTag, wb = uploadPart(x.store, x.url, bytes, x.cur_part_id, x.uploadState; x.credentials, kw...)
    # add part eTag to our collection of eTags in the right order
    put!(x.sync, x.cur_part_id) do
        push!(x.eTags, parteTag)
    end
    @show x.eTags
    # atomically increment our part counter
    @atomic x.cur_part_id += 1
    return wb
end

function Base.close(x::MultipartUploadStream; kw...)
    return API.completeMultipartUpload(x.store, x.url, x.eTags, x.uploadState; kw...)
end

Base.eof(io::PrefetchedDownloadStream) = io.pos > io.len
bytesremaining(io::PrefetchedDownloadStream) = io.len - io.pos + 1
function Base.bytesavailable(io::PrefetchedDownloadStream)
    return isnothing(io.buf) ? 0 : bytesavailable(io.buf::PrefetchBuffer)
end
function Base.close(io::PrefetchedDownloadStream)
    isopen(io.prefetch_queue) && close(io.prefetch_queue)
    isopen(io.download_queue) && close(io.download_queue)
    # In case `_prefetching_task` was waiting on `io.task_condition`, we error notify
    # the same way closing a Channel notifies the Channel's conditions.
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
    eof(io) && return 0
    bytes_to_read = min(bytesremaining(io), Int(n))
    bytes_to_read > length(dest) && resize!(dest, bytes_to_read)
    bytes_read = GC.@preserve dest _unsafe_read(io, pointer(dest), bytes_to_read)
    return bytes_read
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
