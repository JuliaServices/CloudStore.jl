mutable struct ObjectByteWindow
    bytes::Vector{UInt8}
    offset::Int
    lock::ReentrantLock
end

"""
    CloudStore.ObjectBytes(object::CloudStore.Object; blocksize=64 * 1024, kwargs...)

A read-only `AbstractVector{UInt8}` over one version of a remote object. Indexing,
`view`, and `copyto!` can read selected bytes without downloading the whole object.
Range requests use the object's size and strong ETag; changed, short, or malformed
responses throw. Missing ETag metadata is refreshed once when constructing the array.
The array contains stored bytes, without decompression.

Scalar reads share one buffer of at most `blocksize` bytes. Contiguous `view`s share
that buffer and the same object version. Bulk `copyto!` fetches the requested range
unless it is already buffered. Contiguous strided destinations receive directly;
other destination types may use a temporary vector. A failed copy may leave partial
bytes in the destination. `copy` and ordinary range indexing return owned vectors.
Materializing the entire array requires object-sized output memory.

Reads from multiple tasks are supported: scalar reads serialize while filling the
shared buffer, and independent bulk copies can run concurrently. The array owns no
open connection and does not require closing. Retained views keep the shared buffer
alive. Additional keywords are forwarded to each range request; do not mutate any
objects passed as keyword arguments while the array is in use.
"""
struct ObjectBytes{O<:Object,K} <: AbstractVector{UInt8}
    object::O
    offset::Int
    count::Int
    window::ObjectByteWindow
    options::K
end

function ObjectBytes(object::Object; blocksize::Integer=64 * 1024, headers=nothing, kw...)
    0 < blocksize <= typemax(Int) || throw(ArgumentError("blocksize must be a positive Int"))
    length(object) >= 0 || throw(ArgumentError("object size must be nonnegative"))
    options = (; headers=API.transferheaders(headers), kw...)
    snapshot = API.rangeObject(object; options...)
    window = ObjectByteWindow(Vector{UInt8}(undef, min(Int(blocksize), length(snapshot))),
        -1, ReentrantLock())
    return ObjectBytes(snapshot, 0, length(snapshot), window, options)
end

Base.size(bytes::ObjectBytes) = (bytes.count,)
Base.IndexStyle(::Type{<:ObjectBytes}) = IndexLinear()

function Base.view(bytes::ObjectBytes, indices::AbstractUnitRange{<:Integer})
    checkbounds(bytes, indices)
    isempty(indices) && return ObjectBytes(bytes.object, bytes.offset, 0, bytes.window, bytes.options)
    return ObjectBytes(bytes.object, bytes.offset + Int(first(indices) - 1),
        Int(length(indices)), bytes.window, bytes.options)
end

function _objectbytes_cached(bytes::ObjectBytes, offset::Int, count::Int)
    window = bytes.window
    return window.offset >= 0 && window.offset <= offset &&
        count <= min(length(window.bytes), length(bytes.object) - window.offset) - (offset - window.offset)
end

function Base.getindex(bytes::ObjectBytes, index::Int)
    checkbounds(bytes, index)
    offset = bytes.offset + (index - 1)
    window = bytes.window
    # ponytail: one locked window; use independent arrays if scalar readers contend.
    return lock(window.lock) do
        if !_objectbytes_cached(bytes, offset, 1)
            first = div(offset, length(window.bytes)) * length(window.bytes)
            count = min(length(window.bytes), length(bytes.object) - first)
            window.offset = -1
            API.getRange!(view(window.bytes, 1:count), bytes.object, first + 1, count; bytes.options...)
            window.offset = first
        end
        return window.bytes[offset - window.offset + 1]
    end
end

function Base.copyto!(dest::AbstractVector{UInt8}, doff::Integer,
        src::ObjectBytes, soff::Integer, count::Integer)
    count == 0 && return dest
    count > 0 || throw(ArgumentError("number of bytes must be nonnegative"))
    checkbounds(dest, doff)
    checkbounds(src, soff)
    count <= lastindex(dest) - doff + 1 || throw(BoundsError(dest, (doff, count)))
    count <= length(src) - soff + 1 || throw(BoundsError(src, (soff, count)))
    offset = src.offset + (Int(soff) - 1)
    n = Int(count)
    copied = lock(src.window.lock) do
        _objectbytes_cached(src, offset, n) || return false
        copyto!(dest, doff, src.window.bytes, offset - src.window.offset + 1, n)
        return true
    end
    copied || API.getRange!(view(dest, doff:doff + n - 1), src.object, offset + 1, n; src.options...)
    return dest
end

Base.copy(bytes::ObjectBytes) = copyto!(Vector{UInt8}(undef, length(bytes)), 1, bytes, 1, length(bytes))
Base.getindex(bytes::ObjectBytes, indices::AbstractUnitRange{<:Integer}) = copy(view(bytes, indices))
