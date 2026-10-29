import CloudBase: AbstractStore, CloudCredentials, AWS, Azure

const DEFAULT_PREFETCH_SIZE = 32 * 1024 * 1024
const DEFAULT_PREFETCH_MULTIPART_SIZE = 8 * 1024 * 1024

"""
    CloudStore.Object(store, key; credentials=nothing, kwargs...)

A remote object in an S3 bucket or Azure Blob container.

Constructing an `Object` from a store and key sends a metadata request. Upload and list
operations also return `Object` values. The fields include `store`, `credentials`, `key`,
`size`, `eTag`, and provider-specific `properties`.

`copyto!` and `PrefetchedDownloadStream` use the size and strong ETag to validate
each range. If the ETag is missing, they refresh metadata before reading.
"""
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

function Object(store::AbstractStore, key::String; credentials::Union{CloudCredentials, Nothing}=nothing, headers=nothing, kw...)
    url = makeURL(store, key)
    resp = API.headObject(store, url, transferheaders(headers); credentials=credentials, kw...)
    # The ArgumentError will be caused by the HTTP error to provide more context
    if is_redirect_response(resp)
        try
            throw(status_error(resp))
        catch
            throw(ArgumentError("Invalid object key: $key"))
        end
    end
    resp.status == 200 || throw(status_error(resp))
    size = parse(Int, HTTP.header(resp, "Content-Length", ""))
    size >= 0 || throw(ArgumentError("negative object Content-Length"))
    et = etag(HTTP.header(resp, "ETag", ""))
    return Object(store, credentials, key, size, String(et))
end

Base.length(x::Object) = x.size

function Base.copyto!(dest::AbstractVector{UInt8}, doff::Integer, src::Object, soff::Integer, n::Integer)
    # validate arguments
    n == 0 && return 0
    n > 0 || throw(ArgumentError("requested number of bytes must be nonnegative"))
    0 < doff <= length(dest) || throw(BoundsError(dest, doff))
    0 < soff <= length(src) || throw(BoundsError(src, soff))
    n <= length(src) - (soff - 1) || throw(ArgumentError("requested number of bytes (`$n`) would exceed source length"))
    n <= length(dest) - (doff - 1) || throw(ArgumentError("requested number of bytes (`$n`) would exceed destination length"))
    return unsafe_copyto!(dest, doff, src, soff, n)
end

function rangeObject(src::Object; kw...)
    if isempty(src.eTag)
        snapshot = Object(src.store, src.key; credentials=src.credentials, kw...)
        length(snapshot) == length(src) || throw(ArgumentError("object size changed before ranged download"))
        src = snapshot
    end
    rangeETag(src.eTag)
    return src
end

function getRange!(dest, src::Object, soff::Integer, n::Integer; headers=nothing, kw...)
    n == 0 && return 0
    src = rangeObject(src; headers, kw...)
    return getRange!(dest, src.store, makeURL(src.store, src.key), headers,
        (soff - 1):(soff + n - 2), length(src), rangeETag(src.eTag);
        credentials=src.credentials, kw...)
end

function getRange(src::Object, soff::Integer, n::Integer; kw...)
    dest = Vector{UInt8}(undef, n)
    getRange!(dest, src, soff, n; kw...)
    return dest
end

Base.unsafe_copyto!(dest::AbstractVector{UInt8}, doff::Integer, src::Object, soff::Integer, n::Integer) =
    getRange!(view(dest, doff:(doff + n - 1)), src, soff, n)

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
                    isopen(io.download_queue) || return nothing
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

function _download_task(io; headers=nothing, kw...)
    object = io.object
    url = makeURL(object.store, io.object.key)
    credentials = object.credentials
    tag = rangeETag(object.eTag)

    try
        while true
            (off, rng, download_buffer) = take!(io.download_queue)
            buffer_view = view(download_buffer, off + 1:off + length(rng))
            getRange!(buffer_view, object.store, url, headers, rng, length(object), tag;
                credentials, kw...)

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
    PrefetchedDownloadStream(object::CloudStore.Object, prefetch_size=32 * 1024^2; prefetch_multipart_size=8 * 1024^2, kwargs...)
    PrefetchedDownloadStream(store, key, prefetch_size=32 * 1024^2; credentials=nothing, prefetch_multipart_size=8 * 1024^2, kwargs...)

A read-only `IO` over a remote object that downloads ahead of the reader. Two buffers of
`prefetch_size` bytes alternate: while you read one, background tasks fill the other with
range requests of at most `prefetch_multipart_size` bytes, using up to
`Threads.nthreads()` tasks.

Every range must match the object's size and strong ETag, so a changed object raises an
error. The stream returns the stored bytes; wrap it in a decompressor such as
`CodecZlib.GzipDecompressorStream` for gzip objects. Do not read from several tasks at
once. Other keyword arguments are passed to the HTTP requests.

```julia
io = PrefetchedDownloadStream(bucket, "data.csv.gz"; credentials)
text = read(GzipDecompressorStream(io), String)
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
        len > 0 && (object = rangeObject(object; kw...))
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
        object = Object(store, key; credentials, kw...)
        return PrefetchedDownloadStream(object, prefetch_size; prefetch_multipart_size, kw...)
    end
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

function _upload_task(io; kw...)
    try
        (part_n, upload_buffer) = take!(io.upload_queue)
        # upload the part
        parteTag, _ = uploadPart(io.store, io.url, upload_buffer, part_n, io.uploadState; io.credentials, kw...)
        # add part eTag to our collection of eTags
        Base.@lock io.cond_wait begin
            if length(io.eTags) < part_n
                resize!(io.eTags, part_n)
            end
            io.eTags[part_n] = parteTag
        end
    catch e
        isopen(io.upload_queue) && close(io.upload_queue, e)
        Base.@lock io.cond_wait begin
            io.exc = e
            notify(io.cond_wait, all=true)
        end
    finally
        Base.release(io.sem)
        Base.@lock io.cond_wait begin
            io.ntasks -= 1
            notify(io.cond_wait, all=true)
        end
    end
    return nothing
end

"""
    MultipartUploadStream(store, key; credentials=nothing, concurrent_writes_to_channel=4 * Threads.nthreads(), kwargs...)
    MultipartUploadStream(f, store, key; kwargs...)

An `IO` that uploads each `write(io, bytes::Vector{UInt8})` as the next part of a multipart
upload. Parts upload in background tasks; when `concurrent_writes_to_channel` parts are in
flight, `write` waits. Do not modify `bytes` after passing it to `write`.

`close(io)` waits for every part, completes the upload, and returns the object's ETag.
`CloudStore.abort(io)` discards the upload instead. If a part or the completion fails,
`close` aborts the upload and rethrows. The `do`-block form calls `close` after `f(io)`
returns, or aborts if `f` throws.

`headers` and `contentType` describe the object, as for `CloudStore.put`, and apply to
`close` too: write conditions such as `If-None-Match: *` and `If-Match` are checked when
the upload completes. On S3 every part except the last must be at least 5 MiB; use
`CloudStore.put` for smaller objects. Other keyword arguments are passed to the HTTP
requests. This API is experimental.
"""
mutable struct MultipartUploadStream{T <: AbstractStore} <: IO
    store::T
    url::String
    credentials::Union{Nothing, AWS.Credentials, Azure.Credentials}
    # Object headers and content type, sent again on completion: S3 checks write
    # conditions only then, and Azure takes everything from the block-list commit.
    headers::HTTP.Headers
    contentType::Union{Nothing, String}
    uploadState::Union{String, Nothing}
    eTags::Vector{String}
    upload_queue::Channel{Tuple{Int, Vector{UInt8}}}
    cond_wait::Threads.Condition
    cur_part_id::Int
    ntasks::Int
    exc::Union{Exception, Nothing}
    sem::Base.Semaphore
    closed::Bool
    aborted::Bool

    function MultipartUploadStream(
        store::AWS.Bucket,
        key::String;
        credentials::Union{Nothing, AWS.Credentials}=nothing,
        concurrent_writes_to_channel::Int=(4 * Threads.nthreads()),
        headers=nothing,
        contentType::Union{Nothing,AbstractString}=nothing,
        kw...
    )
        concurrent_writes_to_channel > 0 || throw(ArgumentError(
            "`concurrent_writes_to_channel` must be positive"))
        url = makeURL(store, key)
        uploadState = API.startMultipartUpload(store, key; credentials,
            headers=transferheaders(headers), contentType, kw...)
        io = new{AWS.Bucket}(
            store,
            url,
            credentials,
            transferheaders(headers),
            contentType === nothing ? nothing : String(contentType),
            uploadState,
            Vector{String}(),
            Channel{Tuple{Int, Vector{UInt8}}}(Inf),
            Threads.Condition(),
            0,
            0,
            nothing,
            Base.Semaphore(concurrent_writes_to_channel),
            false,
            false,
        )
        return io
    end

    function MultipartUploadStream(
        store::Azure.Container,
        key::String;
        credentials::Union{Nothing, Azure.Credentials}=nothing,
        concurrent_writes_to_channel::Int=(4 * Threads.nthreads()),
        headers=nothing,
        contentType::Union{Nothing,AbstractString}=nothing,
        kw...
    )
        concurrent_writes_to_channel > 0 || throw(ArgumentError(
            "`concurrent_writes_to_channel` must be positive"))
        url = makeURL(store, key)
        uploadState = API.startMultipartUpload(store, key; credentials,
            headers=transferheaders(headers), contentType, kw...)
        io = new{Azure.Container}(
            store,
            url,
            credentials,
            transferheaders(headers),
            contentType === nothing ? nothing : String(contentType),
            uploadState,
            Vector{String}(),
            Channel{Tuple{Int, Vector{UInt8}}}(Inf),
            Threads.Condition(),
            0,
            0,
            nothing,
            Base.Semaphore(concurrent_writes_to_channel),
            false,
            false,
        )
        return io
    end

    # Alternative syntax that applies the function `f` to the result of
    # `MultipartUploadStream(args...; kwargs...)`, waits for all parts to be uploaded and
    # and closes the stream.
    # `close` and `abort` receive the other keywords; this one only configures the stream.
    function MultipartUploadStream(f::Function, args...;
        concurrent_writes_to_channel::Int=(4 * Threads.nthreads()), kw...)
        io = MultipartUploadStream(args...; concurrent_writes_to_channel, kw...)
        try
            f(io)
            close(io; kw...)
        catch
            if !io.closed && !io.aborted
                try
                    abort(io; kw...)
                catch abort_exception
                    @warn "Failed to abort multipart upload" exception=(abort_exception, catch_backtrace())
                end
            end
            rethrow()
        end
    end
end

# Writes a data chunk to the channel and spawn
function Base.write(io::MultipartUploadStream, bytes::Vector{UInt8}; kw...)
    isopen(io) || throw(InvalidStateException("multipart upload is closed", :closed))
    Base.acquire(io.sem)
    local part_n
    registered = false
    try
        Base.@lock io.cond_wait begin
            isopen(io) || throw(InvalidStateException(
                "multipart upload is closed", :closed))
            isnothing(io.exc) || throw(io.exc)
            io.ntasks += 1
            io.cur_part_id += 1
            part_n = io.cur_part_id
            registered = true
            notify(io.cond_wait)
        end
        # We expect the data chunks to be written in order in the channel.
        put!(io.upload_queue, (part_n, bytes))
        Threads.@spawn _upload_task($io; $(kw)...)
    catch
        Base.release(io.sem)
        if registered
            Base.@lock io.cond_wait begin
                io.ntasks -= 1
                notify(io.cond_wait, all=true)
            end
        end
        rethrow()
    end
    return nothing
end

# Waits for all parts to be uploaded
function Base.wait(io::MultipartUploadStream)
    Base.@lock io.cond_wait begin
        while io.ntasks != 0
            wait(io.cond_wait)
        end
        isnothing(io.exc) || throw(io.exc)
    end
    return nothing
end

function _wait_for_upload_tasks(io::MultipartUploadStream)
    Base.@lock io.cond_wait begin
        while io.ntasks != 0
            wait(io.cond_wait)
        end
    end
    return nothing
end

"""
    CloudStore.abort(io::MultipartUploadStream; kwargs...)

Wait for active part requests, then abort the multipart upload. S3 removes the
uploaded parts immediately. Azure has no matching abort request and removes
uncommitted blocks after its service retention period.
"""
# `contentType` describes the object; the do-block form passes it here too.
function abort(io::MultipartUploadStream; credentials=io.credentials, contentType=nothing, kw...)
    Base.@lock io.cond_wait begin
        io.closed && return nothing
        io.aborted && return nothing
        # Change state under the writer lock so no new part can register.
        io.aborted = true
    end
    isopen(io.upload_queue) && close(io.upload_queue)
    _wait_for_upload_tasks(io)
    return API.abortMultipartUpload(
        io.store, io.url, io.uploadState; credentials, kw...)
end

Base.isopen(io::MultipartUploadStream) =
    !io.closed && !io.aborted && isopen(io.upload_queue)

# When there are no more data chunks to upload, this function closes the channel and sends
# a POST request with a single id for the entire upload.
function Base.close(io::MultipartUploadStream; credentials=io.credentials,
    headers=io.headers, contentType=io.contentType, kw...)
    io.closed && return nothing
    io.aborted && throw(InvalidStateException("multipart upload was aborted", :closed))
    try
        isopen(io.upload_queue) && close(io.upload_queue)
        wait(io)
        result = API.completeMultipartUpload(io.store, io.url, io.eTags, io.uploadState;
            credentials, headers=transferheaders(headers), contentType, kw...)
        io.closed = true
        return result
    catch e
        io.exc = e
        try
            abort(io; credentials, kw...)
        catch abort_exception
            @warn "Failed to abort multipart upload" exception=(abort_exception, catch_backtrace())
        end
        rethrow()
    end
end
