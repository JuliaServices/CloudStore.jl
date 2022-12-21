import CloudBase: AbstractStore, CloudCredentials, AWS, Azure

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

function Base.unsafe_copyto!(dest::Channel, doff::Integer, src::Object, soff::Integer, n::Integer)
    bytes = getRange(src, soff, n)
    dest_arr, new_n = take!(dest)
    new_n == n || throw(ArgumentError("prefetched number of bytes (`$n`) doesn't matched requested number of bytes (`$new_n`)"))
    copyto!(dest_arr::AbstractVector{UInt8}, doff, bytes)
    put!(dest, n)
    return n
end

mutable struct IOObject{T <: Object} <: IO
    object::T
    pos::Int
    prefetch::Union{Nothing, Task}
    chan::Channel{Any}
end

IOObject(x::Object) = IOObject(x, 1, nothing, Channel{Any}(0))
IOObject(store::AbstractStore, key::String; credentials::Union{CloudCredentials, Nothing}=nothing) =
    IOObject(Object(store, key; credentials))

Base.eof(x::IOObject) = x.pos > length(x.object)

function Base.readbytes!(x::IOObject, dest::AbstractVector{UInt8}, n::Integer=length(dest))
    n = min(n, length(dest))
    n = min(n, length(x.object) - x.pos + 1)
    n == 0 && return dest
    if x.prefetch === nothing
        # no prefetch, request directly
        Base.unsafe_copyto!(dest, 1, x.object, x.pos, n)
        x.pos += n
    else
        put!(x.chan, (dest, n))
        x.pos += take!(x.chan)::Integer
    end
    if !eof(x)
        # start prefetch
        x.prefetch = @async Base.unsafe_copyto!(x.chan, 1, x.object, x.pos, n)
    end
    return dest
end

mutable struct PrefetchBuffer
    data::Vector{UInt8}
    pos::Int
    len::Int
end
Base.bytesavailable(b::PrefetchBuffer) = b.len - b.pos + 1

function _prefetching_task(io)
    prefetch_size = io.prefetch_size
    len = io.len
    object = io.object
    ppos = 1

    while len > ppos
        n = min(bytesavailable(io), prefetch_size)
        buf = PrefetchBuffer(getRange(object, ppos, n), 1, n)
        ppos += n
        # unbuffered, blocks until we're done with the previous buffer and `take!` the next one
        put!(io.queue, buf)
    end
    close(io.queue)
    return nothing
end

mutable struct PrefechedDownloadStream{T <: Object} <: IO
    object::T
    pos::Int
    len::Int
    buf::Union{Nothing,PrefetchBuffer}
    prefetch_size::Int
    queue::Channel{PrefetchBuffer}

    function PrefechedDownloadStream(
        object::T,
        prefetch_size::Int=MULTIPART_SIZE;
    ) where {T<:Object}
        len = length(object)
        size = min(prefetch_size, len)
        io = new{T}(object, 1, len, nothing, size, Channel{PrefetchBuffer}(0))
        Threads.@spawn _prefetching_task(io)
        return io
    end

    function PrefechedDownloadStream(
        store::AbstractStore,
        key::String,
        prefetch_size::Int=MULTIPART_SIZE;
        credentials::Union{CloudCredentials, Nothing}=nothing
    )
        url = makeURL(store, key)
        resp = API.headObject(store, url, HTTP.Headers(); credentials=credentials)
        len = parse(Int, HTTP.header(resp, "Content-Length", "0"))
        et = etag(HTTP.header(resp, "ETag", ""))
        object = Object(store, credentials, String(key), Int(len), String(et))
        return PrefechedDownloadStream(object, prefetch_size)
    end
end
Base.eof(io::PrefechedDownloadStream) = io.pos >= io.len
Base.bytesavailable(io::PrefechedDownloadStream) = io.len - io.pos + 1
Base.isopen(io::PrefechedDownloadStream) = !eof(io)
function getbuffer(io::PrefechedDownloadStream)
    buf = io.buf
    if isnothing(buf)
        buf = take!(io.queue)
        io.buf = buf
    end
    return buf
end

function _unsafe_read(io::PrefechedDownloadStream, dest::Ptr{UInt8}, bytes_to_read::Int)
    bytes_read = 1
    while bytes_to_read > bytes_read
        buf = getbuffer(io)::PrefetchBuffer
        bytes_in_buffer = bytesavailable(buf)

        adv = min(bytes_in_buffer, bytes_to_read - bytes_read + 1)
        GC.@preserve buf unsafe_copyto!(dest + bytes_read - 1, pointer(buf.data) + buf.pos - 1, adv)
        buf.pos += adv
        bytes_read += adv
        io.pos += adv

        if buf.pos > buf.len
            io.buf = nothing
        end
    end
    return bytes_read - 1
end

function Base.readbytes!(io::PrefechedDownloadStream, dest::AbstractVector{UInt8}, n)
    eof(io) && return UInt32(0)
    bytes_to_read = min(bytesavailable(io), Int(n))
    bytes_to_read > length(dest) && resize!(dest, bytes_to_read)
    bytes_read = GC.@preserve dest _unsafe_read(io, pointer(dest), bytes_to_read)
    return UInt32(bytes_read)
end

function Base.unsafe_read(io::PrefechedDownloadStream, p::Ptr{UInt8}, nb::UInt)
    if eof(io)
        nb > 0 && throw(EOFError())
        return nothing
    end
    avail = bytesavailable(io)
    _unsafe_read(io, p, min(avail, Int(nb)))
    nb > avail && throw(EOFError())
    return nothing
end
