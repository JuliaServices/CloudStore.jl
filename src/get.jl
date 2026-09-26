# list
function listObjectsImpl(x::AbstractStore;
    prefix::String="",
    maxKeys=maxListKeys(x),
    query=Dict{String, String}(), kw...)
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
        query[continuationToken(x)] = token
        contents2, token = listObjects(x, query; kw...)
        append!(contents, contents2)
    end
    return contents
end

function headObjectImpl(x::AbstractStore, key::Resource;
    multipartThreshold::Int=MULTIPART_THRESHOLD,
    allowMultipart::Bool=true,
    headers=HTTP.Headers(), kw...)
    url = makeURL(x, key)
    if allowMultipart
        HTTP.setheader(headers, contentRange(0:(multipartThreshold - 1)))
    end
    return Dict(headObject(x, url, headers; kw...).headers)
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

# Download bytes `rng` (0-based) of the object version `tag` into `dest` and
# return the byte count. Throws unless the response has exactly that range,
# the expected total size, and the same ETag.
function getRange!(dest, store, url, headers, rng, total, tag; kw...)
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
    # Ranges address stored bytes. Decompression belongs after reassembly.
    request_kw = merge(OWNED_HEADERS_KW, (; kw...), (; decompress=false))
    # A view keeps HTTP from resizing the caller's buffer and bounds writes
    # when the server ignores Range or sends too many bytes.
    dest = view(dest, 1:length(rng))
    @static if isdefined(HTTP, :BytesBody)
        resp = getObject(store, url, headers; response_stream=dest, request_kw...)
        # HTTP 2 sets `content_length` to the body bytes it received, not the header value.
        nbytes = resp.content_length
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
# case, we do a HEAD request first to know the size of the object, which gives us the opportunity
# to throw an ArgumentError. But for the single GET case, we don't know the size of the object
# until we get the response. HTTP 1 wraps a response-stream error in HTTP.RequestError,
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
    # then we'll avoid doing an exploratory HEAD request to get total size
    # and take the user's word that the total object size is <= objectMaxSize | length(out)
    if objectMaxSize !== nothing && objectMaxSize < multipartThreshold
        allowMultipart = false
    end
    if allowMultipart
        partSize > 0 || throw(ArgumentError("partSize must be > 0"))
        batchSize > 0 || throw(ArgumentError("batchSize must be > 0"))
    end
    headers = transferheaders(headers)
    kw = merge(OWNED_HEADERS_KW, (; kw...))
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
            if decompress
                body = decompressorstream(zlibng)(open(out, "w"))
                resp = getObject(x, url, headers; response_stream=body, kw...)
            else
                body = open(out, "w")
                resp = getObject(x, url, headers; response_stream=body, kw...)
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
        nbytes[] = parse(Int, HTTP.header(resp, "Content-Length", "0"))
        @goto done
    end

    # multipart downloads

    # make a head request to see if the object happens to be empty
    # if so, it isn't valid to make a Range bytes request, so we'll short-circuit
    # the head request also lets us know how big the object it
    # `headers` seeds every range request below, so HEAD gets its own copy.
    resp = API.headObject(x, url, copy(headers); kw...)
    check_redirect(key, resp)
    resp.status == 200 || throw(status_error(resp))
    contentLength = parse(Int, HTTP.header(resp, "Content-Length", ""))
    contentLength >= 0 || throw(ArgumentError("negative object Content-Length"))
    tag = contentLength == 0 ? "" : rangeETag(HTTP.header(resp, "ETag", ""))
    if contentLength == 0
        # if the object is zero-length, return an "empty" version of the output type
        if out === nothing || out isa AbstractVector{UInt8}
            res = resp.body
        elseif out isa String
            body = open(out, "w")
        else
            body = out
        end
        @goto done
    elseif out === nothing
        # allocate the full, final buffer upfront since we know the length
        res = body = Vector{UInt8}(undef, contentLength)
    elseif out isa AbstractVector{UInt8}
        # user-provided buffer is allowed to be larger than actual object size, but not smaller
        # NOTE: wording of the error message matches what HTTP.jl throws when the buffer is too small
        length(out) < contentLength && throw(ArgumentError("Unable to grow response stream IOBuffer $(length(out)) large enough for response body size: $(contentLength)"))
        res = out
        body = view(out, 1:contentLength)
    elseif out isa String
        file = open(out, "w")
        body = decompress ? decompressorstream(zlibng)(file) : file
        buffers = BufferBatch(batchSize, partSize)
    else
        body = decompress ? decompressorstream(zlibng)(out) : out
        buffers = BufferBatch(batchSize, partSize)
    end

    nTasks = cld(contentLength, partSize)
    nLoops = cld(nTasks, batchSize)
    downloads = Vector{Task}(undef, min(batchSize, nTasks))
    try
        for j = 1:nLoops
            count = min(batchSize, nTasks - (j - 1) * batchSize)
            @sync begin
                for i = 1:count
                    n = (j - 1) * batchSize + i
                    downloads[i] = Threads.@spawn begin
                        _n = $n
                        rng = ((_n - 1) * partSize):min(contentLength - 1, _n * partSize - 1)
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
                    Threads.@spawn for i = 1:count
                        wait(downloads[i])
                        offset = ((j - 1) * batchSize + i - 1) * partSize
                        write(body, view(buffers[i], 1:min(partSize, contentLength - offset)))
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
