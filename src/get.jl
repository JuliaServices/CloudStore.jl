# list
function listObjectsImpl(x::AbstractStore;
    prefix::String="",
    maxKeys=maxListKeys(x),
    query=Dict{String, String}(), kw...)
    # Each page adds prefix, page size, and continuation keys; the caller's query is not
    # changed, so reusing it cannot start another listing from this one's last token.
    query = copy(query)
    if maxKeys > maxListKeys(x)
        @warn "$(cloudName(x)) only supports $(maxListKeys(x)) keys per request: `$maxKeys` requested"
        maxKeys = maxListKeys(x)
    end
    if !isempty(prefix)
        query["prefix"] = prefix
    end
    if maxKeys != maxListKeys(x)
        query[listMaxKeysQuery(x)] = string(maxKeys)
    end
    contents, token = listObjects(x, query; kw...)
    while !isempty(token)
        # A service that answered a token with that same token would be asked forever.
        token == Base.get(query, continuationToken(x), nothing) &&
            error("$(cloudName(x)) listing returned the same continuation token twice")
        query[continuationToken(x)] = token
        contents2, token = listObjects(x, query; kw...)
        append!(contents, contents2)
    end
    return contents
end

# `multipartThreshold` and `allowMultipart` mirror `get` and are ignored: a ranged
# HEAD reports the range length as Content-Length (S3 also rejects it for empty objects).
function headObjectImpl(x::AbstractStore, key::Resource;
    multipartThreshold::Int=MULTIPART_THRESHOLD,
    allowMultipart::Bool=true,
    headers=nothing, kw...)
    resp = headObject(x, makeURL(x, key), transferheaders(headers); kw...)
    return Dict(resp.headers)
end

function existsObjectImpl(x::AbstractStore, key::Resource;
    headers=HTTP.Headers(), kw...)
    url = makeURL(x, key)
    request_kw = merge((; kw...), (; status_exception=false))
    resp = headObject(x, url, headers; request_kw...)
    resp.status == 404 && return false
    200 <= resp.status < 300 && return true
    throw(status_error(resp))
end

# Content-Range: bytes 0-9/443
contentRange(rng) = "Range" => "bytes=$(first(rng))-$(last(rng))"

function parseContentRange(str)
    m = match(r"^bytes (\d+)-(\d+)/(\d+)\z", str)
    m === nothing && error("invalid Content-Range: $str")
    return (parse(Int, m[1]), parse(Int, m[2]), parse(Int, m[3]))
end

function rangeETag(tag)
    (isempty(tag) || startswith(tag, "W/")) && throw(ArgumentError(
        "ranged downloads require a strong ETag; use allowMultipart=false for a single GET"))
    # `Object.eTag` omits the surrounding quotes.
    value = startswith(tag, '"') ? String(tag) : string('"', tag, '"')
    occursin(r"^\"[^\x00-\x20\"\x7f]*\"\z", value) || throw(ArgumentError("invalid ETag: $tag"))
    return value
end

@static if isdefined(HTTP, :BytesBody)
    # HTTP 2 retries a request only until its response head arrives. These errors can end
    # a body midway; the range is fetched again (If-Match pins the object version).
    const BODY_ERRORS = Union{HTTP.ParseError, HTTP.ProtocolError, HTTP.TimeoutError,
        HTTP.TLSTransportError, HTTP.H2GoAwayError, HTTP.H2StreamResetError, Base.IOError, EOFError}
    const RANGE_ATTEMPTS = 3
else
    # HTTP 1 retries the whole request, including reading the body.
    const BODY_ERRORS = Union{}
    const RANGE_ATTEMPTS = 1
end

# Call `f`, again when its response body fails midway (see BODY_ERRORS).
function retryrange(f)
    for attempt in 1:RANGE_ATTEMPTS
        try
            return f()
        catch e
            (attempt < RANGE_ATTEMPTS && e isa BODY_ERRORS) || rethrow()
            sleep(0.1 * attempt)
        end
    end
end

# Download bytes `rng` (0-based) of the object version `tag` into `dest` and
# return the byte count. Throws unless the response has exactly that range,
# the expected total size, and the same ETag. A body that fails midway is retried.
getRange!(dest, store, url, headers, rng, total, tag; kw...) =
    retryrange(() -> getRangeOnce!(dest, store, url, headers, rng, total, tag; kw...))

function getRangeOnce!(dest, store, url, headers, rng, total, tag; kw...)
    headers = transferheaders(headers)
    target = dest
    # HTTP writes into contiguous memory; other destinations use a temporary buffer.
    direct = dest isa StridedVector{UInt8} && stride(dest, 1) == 1
    direct || (dest = Vector{UInt8}(undef, length(rng)))
    HTTP.setheader(headers, contentRange(rng))
    # Preserve caller preconditions: adding If-Match can change how providers
    # evaluate If-Unmodified-Since. The response ETag is checked in either case.
    conditions = ("If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since", "If-Range")
    any(name -> HTTP.hasheader(headers, name), conditions) || HTTP.setheader(headers, "If-Match" => tag)
    resp, nbytes = rangeresponse!(dest, store, url, headers, length(rng); kw...)
    resp.status == 206 || throw(ArgumentError("expected HTTP 206 for a ranged download, got $(resp.status)"))
    range = HTTP.header(resp, "Content-Range", "")
    parseContentRange(range) == (first(rng), last(rng), total) || throw(ArgumentError(
        "unexpected Content-Range: $range; expected bytes $(first(rng))-$(last(rng))/$total"))
    nbytes == length(rng) || throw(ArgumentError(
        "incomplete ranged download: expected $(length(rng)) bytes, received $nbytes"))
    rangeETag(HTTP.header(resp, "ETag", "")) == tag || throw(ArgumentError(
        "object ETag changed during ranged download"))
    direct || copyto!(target, 1, dest, 1, nbytes)
    return nbytes
end

# Send a ranged GET and receive its body into the first `n` bytes of the contiguous
# `dest`, or into `resp.body` when `dest === nothing`. Error bodies never reach `dest`.
# Returns the response and the number of body bytes received.
function rangeresponse!(dest, store, url, headers, n; kw...)
    # Ranges address stored bytes. Decompression belongs after reassembly.
    request_kw = merge(OWNED_HEADERS_KW, (; kw...), (; decompress=false))
    # A view keeps HTTP from resizing the caller's buffer and bounds writes
    # when the server ignores Range or sends too many bytes.
    dest === nothing || (dest = view(dest, 1:n))
    @static if isdefined(HTTP, :BytesBody)
        resp = getObject(store, url, headers; response_stream=dest, request_kw...)
        # HTTP 2 sets `content_length` to the body bytes it received, not the header value.
        nbytes = resp.content_length
    else
        if dest === nothing
            resp = getObject(store, url, headers; request_kw...)
            nbytes = length(resp.body)
        else
            buffer = IOBuffer(dest; write=true, maxsize=length(dest))
            receive = function(http)
                # Retries call this again; start each attempt at the beginning of `dest`.
                seekstart(buffer)
                response = HTTP.startread(http)
                if response.status == 206
                    while !eof(http)
                        readbytes!(http, buffer)
                    end
                elseif response.status == 200
                    throw(ArgumentError("server ignored Range during ranged download"))
                else
                    # Keep error and redirect bodies out of `dest` for the normal HTTP layers.
                    response.body = read(http)
                end
            end
            resp = getObject(store, url, headers; response_stream=buffer, iofunction=receive, request_kw...)
            nbytes = position(buffer)
        end
    end
    return resp, nbytes
end

# The first part of a download is at most 256 KiB: most small objects fit, and later
# parts of a large object wait only for these bytes, not for a whole part.
const FIRST_PART_SIZE = 2^18

# Byte range (0-based) of download part `n` of an object of `total` bytes: part 1 holds
# the first `first` bytes, and later parts hold up to `partSize` bytes each.
partrange(n, first, partSize, total) = n == 1 ? (0:(first - 1)) :
    ((first + (n - 2) * partSize):(min(total, first + (n - 1) * partSize) - 1))

# The object size reported by the first range response, which must hold the object's
# first `min(n, size)` bytes.
function firstrangesize(resp, nbytes, n)
    range = HTTP.header(resp, "Content-Range", "")
    # S3 rejects every range of an empty object with 416; Azurite answers `bytes 0--1/0`.
    (resp.status == 416 || (resp.status == 206 && endswith(range, "/0"))) && return 0
    200 <= resp.status < 300 || throw(status_error(resp))
    resp.status == 206 || throw(ArgumentError("expected HTTP 206 for a ranged download, got $(resp.status)"))
    lo, hi, total = parseContentRange(range)
    expected = min(n, total)
    (lo, hi) == (0, expected - 1) || throw(ArgumentError(
        "unexpected Content-Range: $range; expected bytes 0-$(expected - 1)/$total"))
    nbytes == expected || throw(ArgumentError(
        "incomplete ranged download: expected $expected bytes, received $nbytes"))
    return total
end

# Request the object's first `n` bytes into `dest` (as in `rangeresponse!`). The
# response also carries the object's size and ETag, so an object of at most `n` bytes
# needs no other request. S3 answers any range of an empty object with 416; that
# response is returned rather than thrown.
function firstrange!(dest, store, url, headers, n; kw...)
    headers = transferheaders(headers)
    HTTP.setheader(headers, contentRange(0:(n - 1)))
    try
        return rangeresponse!(dest, store, url, headers, n; kw...)
    catch e
        (e isa HTTP.StatusError && e.status == 416) || rethrow()
        return e.response, 0
    end
end

# Body bytes received. A chunked response has no Content-Length, and HTTP 1 before
# 1.10 does not record the count.
@static if isdefined(HTTP, :BytesBody)
    receivedbytes(resp) = Int(resp.content_length)
else
    receivedbytes(resp) = get(() -> parse(Int, HTTP.header(resp, "Content-Length", "0")),
        resp.request.context, :nbytes)
end

function check_redirect(key, resp)
    if is_redirect_response(resp)
        try
            throw(status_error(resp))
        catch
            # The ArgumentError will be caused by the HTTP error to provide more context
            throw(ArgumentError("Invalid object key: $key"))
        end
    end
end

decompressorstream(zlibng) = zlibng ? CodecZlibNG.GzipDecompressorStream : CodecZlib.GzipDecompressorStream
decompressor(zlibng) = zlibng ? CodecZlibNG.GzipDecompressor : CodecZlib.GzipDecompressor

struct BufferBatch
    lock::ReentrantLock
    buffers::Vector{Vector{UInt8}}
    partSize::Int
end

BufferBatch(n, partSize) = BufferBatch(ReentrantLock(), Vector{Vector{UInt8}}(undef, n), partSize)
function Base.getindex(b::BufferBatch, i::Int)
    Base.@lock b.lock begin
        if isassigned(b.buffers, i)
            return b.buffers[i]
        else
            return b.buffers[i] = Vector{UInt8}(undef, b.partSize)
        end
    end
end

# For smaller object, we don't do a multipart download, but instead just do a single GET request.
# This changes the exception we get when the provided buffer is too small, as for the multipart
# case, the first range response reports the size of the object, which gives us the opportunity
# to throw an ArgumentError. But for the single GET case, HTTP fails while writing the body
# into the buffer. HTTP 1 wraps a response-stream error in HTTP.RequestError,
# while HTTP 2 can surface the underlying error directly. Unwrap the HTTP 1 shape when
# it is available so both versions return the same ArgumentError to the caller.
function _check_buffer_too_small_exception(@nospecialize(e::Exception))
    if isdefined(HTTP, :RequestError) && e isa getproperty(HTTP, :RequestError)
        request_error = e.error
        if request_error isa CompositeException
            length(request_error.exceptions) == 1 || return e
            request_error = request_error.exceptions[1]
        end
        request_error = unwrap_exception(request_error)
        if request_error isa ArgumentError
            return request_error
        end
    end
    return e
end

function getObjectImpl(x::AbstractStore, key::Resource, out::ResponseBodyType=nothing;
    multipartThreshold::Int=MULTIPART_THRESHOLD,
    partSize::Int=MULTIPART_SIZE,
    batchSize::Int=defaultBatchSize(),
    allowMultipart::Bool=true,
    objectMaxSize::Union{Int, Nothing}=out isa AbstractVector{UInt8} ? length(out) : nothing,
    decompress::Bool=false,
    zlibng::Bool=false,
    headers=nothing,
    progress=nothing,
    lograte::Bool=false, kw...)

    # if user provided a buffer or signalled the max object size is < multipartThreshold
    # then we'll make a single GET without a Range header
    # and take the user's word that the total object size is <= objectMaxSize | length(out)
    if objectMaxSize !== nothing && objectMaxSize < multipartThreshold
        allowMultipart = false
    end
    if allowMultipart
        partSize > 0 || throw(ArgumentError("partSize must be > 0"))
        batchSize > 0 || throw(ArgumentError("batchSize must be > 0"))
    end
    headers = transferheaders(headers)
    # Every request returns stored bytes, as ranged reads must; `decompress` above
    # is applied afterwards.
    kw = merge(OWNED_HEADERS_KW, (; kw...), (; decompress=false))
    start_time = time()
    url = makeURL(x, key)
    # setup return type
    # out types: nothing, AbstractVector{UInt8}, String, IO
    # rules:
    #   - if out is nothing, then we'll allocate a single Vector{UInt8}; if decompress, that's 1 extra allocation to decompress
    #   - if out is a Vector{UInt8}, then we'll use that as the response_stream, and resize! it down if needed
    #     - if decompress, we'll initially use out to write to, then make a copy of the written bytes and decompress back into out
    #     - provided buffer *MUST BE* large enough to hold entire object, whether compressed or uncompressed, gotchas include:
    #   - if out is a String, then we'll open a file and use that as the response_stream
    #   - if out is an IO, then we'll use that as the response_stream directly
    #   - BUT, if multipart, then we use a batchSize partSize-length Vector{UInt8}s as scratch space to download in parallel
    #     and then write to out as each part is downloaded
    if !(out === nothing || out isa AbstractVector{UInt8})
        res = out
    end
    # for tracking bitrate per second of overall download
    nbytes = Threads.Atomic{Int}(0)
    progressReported = false

    # if the user doesn't want multipart or we know from objectMaxSize or length(out) that we're
    # < multipartThreshold, then we'll just do a single GET request, handle that case first since
    # it's much simpler and then later we'll do all the multipart stitching logic
    local body
    if !allowMultipart
        if out === nothing
            resp = getObject(x, url, headers; kw...)
            res = resp.body
        elseif out isa AbstractVector{UInt8}
            resp = try
                getObject(x, url, headers; response_stream=out, kw...)
            catch e
                e = _check_buffer_too_small_exception(e)
                rethrow(e)
            end
        elseif out isa String
            file = open(out, "w")
            body = decompress ? decompressorstream(zlibng)(file) : file
            try
                resp = getObject(x, url, headers; response_stream=body, kw...)
                check_redirect(key, resp)
            catch
                # Close the file itself: closing a decompressor over partial data throws.
                close(file)
                rethrow()
            end
        else
            if decompress
                body = decompressorstream(zlibng)(out)
                resp = getObject(x, url, headers; response_stream=body, kw...)
            else
                body = out
                resp = getObject(x, url, headers; response_stream=out, kw...)
            end
        end
        check_redirect(key, resp)
        nbytes[] = receivedbytes(resp)
        @goto done
    end

    # multipart downloads

    # The first part's response also reports the object's size and ETag, so a small
    # object takes one request. A caller buffer that holds the first part receives it
    # directly; otherwise HTTP allocates the response body.
    firstSize = min(partSize, FIRST_PART_SIZE)
    direct = out isa StridedVector{UInt8} && stride(out, 1) == 1 && length(out) >= firstSize
    resp, firstbytes = retryrange(() -> firstrange!(direct ? out : nothing, x, url, headers, firstSize; kw...))
    check_redirect(key, resp)
    contentLength = firstrangesize(resp, firstbytes, firstSize)
    # Later parts must come from the same object version.
    tag = contentLength > firstbytes ? rangeETag(HTTP.header(resp, "ETag", "")) : ""
    if contentLength == 0
        # if the object is zero-length, return an "empty" version of the output type
        if out === nothing
            res = UInt8[]
        elseif out isa String
            body = open(out, "w")
        elseif !(out isa AbstractVector{UInt8})
            body = out
        end
        @goto done
    end
    nbytes[] = firstbytes
    if out === nothing
        if firstbytes == contentLength
            res = resp.body
        else
            # allocate the full, final buffer once the size is known
            res = Vector{UInt8}(undef, contentLength)
            copyto!(res, 1, resp.body, 1, firstbytes)
        end
    elseif out isa AbstractVector{UInt8}
        # user-provided buffer is allowed to be larger than actual object size, but not smaller
        # NOTE: wording of the error message matches what HTTP.jl throws when the buffer is too small
        length(out) < contentLength && throw(ArgumentError("Unable to grow response stream IOBuffer $(length(out)) large enough for response body size: $(contentLength)"))
        direct || copyto!(out, 1, resp.body, 1, firstbytes)
        res = out
    elseif out isa String
        file = open(out, "w")
        body = decompress ? decompressorstream(zlibng)(file) : file
        buffers = BufferBatch(batchSize, partSize)
    else
        body = decompress ? decompressorstream(zlibng)(out) : out
        buffers = BufferBatch(batchSize, partSize)
    end

    nTasks = 1 + cld(contentLength - firstbytes, partSize)
    nLoops = cld(nTasks, batchSize)
    downloads = Vector{Task}(undef, min(batchSize, nTasks))
    try
        # The first part reaches a stream destination before later parts are requested.
        out === nothing || out isa AbstractVector{UInt8} || write(body, resp.body)
        for j = 1:nLoops
            count = min(batchSize, nTasks - (j - 1) * batchSize)
            @sync begin
                for i = 1:count
                    n = (j - 1) * batchSize + i
                    # The first request fetched part 1.
                    n == 1 && continue
                    downloads[i] = Threads.@spawn begin
                        rng = partrange($n, firstbytes, partSize, contentLength)
                        if out === nothing || out isa AbstractVector{UInt8}
                            # The Content-Range header is 0-indexed; the view is 1-indexed.
                            buf = view(res, (first(rng) + 1):(last(rng) + 1))
                        else
                            buf = view(buffers[$i], 1:length(rng))
                        end
                        received = getRange!(buf, x, url, headers, rng, contentLength, tag; kw...)
                        Threads.atomic_add!(nbytes, received)
                    end
                end
                # Write parts in order as they are validated while later parts
                # download. On failure, @sync still waits for every task.
                if !(out === nothing || out isa AbstractVector{UInt8})
                    Threads.@spawn for i = (j == 1 ? 2 : 1):count
                        wait(downloads[i])
                        rng = partrange((j - 1) * batchSize + i, firstbytes, partSize, contentLength)
                        write(body, view(buffers[i], 1:length(rng)))
                    end
                end
            end
            if progress !== nothing
                progress(contentLength, nbytes[])
                progressReported = true
            end
        end
    catch
        # Close the file itself: closing a decompressor over partial data throws
        # and would replace the download error.
        out isa String && close(file)
        rethrow()
    end

@label done
    if out === nothing
        if decompress
            res = transcode(decompressor(zlibng), res)
        end
    elseif out isa AbstractVector{UInt8}
        if decompress
            # make a copy of a view of just the compressed bytes in out, then decompress into out
            res = transcode(decompressor(zlibng), copy(view(out, 1:nbytes[])), out)
        else
            res = out isa Vector{UInt8} ? resize!(out, nbytes[]) : view(out, 1:nbytes[])
        end
    elseif out isa String
        close(body)
    else
        # Ending the decompressor wrapped around `out` checks the gzip trailer, so a
        # truncated object raises; `out` stays open for the caller.
        body === out || write(body, TranscodingStreams.TOKEN_END)
        flush(body)
    end
    end_time = time()
    bytes = nbytes[]
    if progress !== nothing && !progressReported
        progress(bytes, bytes)
    end
    gbits_per_second = bytes == 0 ? 0 : (((8 * bytes) / 1e9) / (end_time - start_time))
    lograte && @info "CloudStore.get complete with bandwidth: $(gbits_per_second) Gbps"
    return res
end
