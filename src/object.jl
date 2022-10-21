import CloudBase: AbstractStore, CloudCredentials

struct Object{T <: AbstractStore, C <: Union{CloudCredentials, Nothing}}
    store::T
    credentials::C
    key::String
    size::Int
end

Object(x::AbstractStore, k::String, lm::AbstractString, etag::AbstractString, sz::Int, sc::AbstractString) =
    Object(x, nothing, k, sz)

function Object(store::AbstractStore, key::String; credentials::Union{CloudCredentials, Nothing}=nothing)
    url = makeURL(store, key)
    resp = API.headObject(store, url, HTTP.Headers(); credentials=credentials)
    size = parse(Int, HTTP.header(resp, "Content-Length", "0"))
    return Object(store, credentials, key, size)
end

Base.length(x::Object) = x.size

function copyto!(dest::AbstractVector{UInt8}, doff::Integer, src::Object, soff::Integer, n::Integer)
    # validate arguments
    0 < doff <= length(dest) || throw(BoundsError(dest, doff))
    0 < soff <= length(src) || throw(BoundsError(src, soff))
    (soff + n) - 1 <= length(src) || throw(ArgumentError("requested number of bytes (`$n`) would exceed source length"))
    (doff + n) - 1 <= length(dest) || throw(ArgumentError("requested number of bytes (`$n`) would exceed destination length"))
    headers = HTTP.Headers()
    HTTP.setheader(headers, contentRange((soff - 1):(soff + n - 2)))
    url = makeURL(src.store, src.key)
    # avoid extra copy here by passing dest to be written to directly
    resp = getObject(src.store, url, headers; credentials=src.credentials)
    copyto!(dest, doff, resp.body)
    return n
end
