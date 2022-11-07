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
