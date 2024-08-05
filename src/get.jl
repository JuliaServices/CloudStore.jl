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

function headObjectImpl(x::AbstractStore, key::String;
    multipartThreshold::Int=MULTIPART_THRESHOLD,
    allowMultipart::Bool=true,
    headers=HTTP2.Headers(), kw...)
    url = makeURL(x, key)
    if allowMultipart
        HTTP2.setheader(headers, contentRange(0:(multipartThreshold - 1))...)
    end
    return Dict(headObject(x, url, headers; kw...).headers)
end

# Content-Range: bytes 0-9/443
contentRange(rng) = "Range" => "bytes=$(first(rng))-$(last(rng))"

function parseContentRange(str)
    m = match(r"bytes (\d+)-(\d+)/(\d+)", str)
    m === nothing && error("invalid Content-Range: $str")
    return (parse(Int, m[1]), parse(Int, m[2]), parse(Int, m[3]))
end

# function check_redirect(key, resp)
#     if HTTP2.isredirect(resp)
#         try
#             throw(HTTP2.StatusError(resp.status, resp.request.method, resp.request.target, resp))
#         catch
#             # The ArgumentError will be caused by the HTTP2 error to provide more context
#             throw(ArgumentError("Invalid object key: $key"))
#         end
#     end
# end

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
# until we get the response, which would return as a HTTP2.RequestError from within HTTP2.jl.
# The idea here is to unwrap the HTTP2.RequestError and check if it's an ArgumentError, and if so,
# throw that instead, so we same exception type is thrown in this case.
# function _check_buffer_too_small_exception(@nospecialize(e::Exception))
#     if e isa HTTP2.RequestError
#         request_error = e.error
#         if request_error isa CompositeException
#             length(request_error.exceptions) == 1 || return e
#             request_error = request_error.exceptions[1]
#         end
#         request_error = unwrap_exception(request_error)
#         if request_error isa ArgumentError
#             return request_error
#         end
#     end
#     return e
# end

function getObjectImpl(x::AbstractStore, key::String, out::ResponseBodyType=nothing;
    multipartThreshold::Int=MULTIPART_THRESHOLD,
    partSize::Int=MULTIPART_SIZE,
    batchSize::Int=defaultBatchSize(),
    allowMultipart::Bool=true,
    objectMaxSize::Union{Int, Nothing}=out isa AbstractVector{UInt8} ? length(out) : nothing,
    decompress::Bool=false,
    zlibng::Bool=false,
    headers=HTTP2.Headers(),
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
                # e = _check_buffer_too_small_exception(e)
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
        # check_redirect(key, resp)
        nbytes[] = parse(Int, HTTP2.getheader(resp.headers, "Content-Length", "0"))
        @goto done
    end

    # multipart downloads

    # make a head request to see if the object happens to be empty
    # if so, it isn't valid to make a Range bytes request, so we'll short-circuit
    # the head request also lets us know how big the object it
    resp = API.headObject(x, url, headers; kw...)
    # check_redirect(key, resp)
    contentLength = parse(Int, HTTP2.getheader(resp.headers, "Content-Length", "0"))
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
        body = decompress ? decompressorstream(zlibng)(open(out, "w")) : open(out, "w")
        buffers = BufferBatch(batchSize, partSize)
    else
        body = decompress ? decompressorstream(zlibng)(out) : out
        buffers = BufferBatch(batchSize, partSize)
    end

    nTasks = max(1, cld(contentLength - 1, partSize))
    nLoops = cld(nTasks, batchSize)
    sync = OrderedSynchronizer(1)
    for j = 1:nLoops
        @sync for i = 1:batchSize
            n = (j - 1) * batchSize + i
            n > nTasks && break
            Threads.@spawn begin
                _n = $n
                _headers = copy(headers)
                rng = ((_n - 1) * partSize):min(contentLength - 1, _n * partSize - 1)
                HTTP2.setheader(_headers, contentRange(rng)...)
                if out === nothing || out isa AbstractVector{UInt8}
                    # the Content-Range header is 0-indexed, but the view is 1-indexed
                    _rng = (first(rng) + 1):(last(rng) + 1)
                    # we pass just this task's slice of the overall buffer to be filled in
                    # directly as HTTP receives the response body
                    _res = view(res, _rng)
                    r = getObject(x, url, _headers; response_stream=_res, kw...)
                    body_length = if hasproperty(resp, :metrics)
                        resp.metrics.request_body_length
                    else
                        contentLength
                    end
                    Threads.atomic_add!(nbytes, body_length)
                else
                    buf = view(buffers[$i], 1:min(partSize, length(rng)))
                    r = getObject(x, url, _headers; response_stream=buf, kw...)
                    body_length = if hasproperty(resp, :metrics)
                        resp.metrics.request_body_length
                    else
                        contentLength
                    end
                    Threads.atomic_add!(nbytes, body_length)
                    put!(() -> write(body, buf), sync, _n)
                end
            end
        end
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
            res = resize!(out, nbytes[])
        end
    elseif out isa String
        close(body)
    else
        flush(body)
    end
    end_time = time()
    bytes = nbytes[]
    gbits_per_second = bytes == 0 ? 0 : (((8 * bytes) / 1e9) / (end_time - start_time))
    lograte && @info "CloudStore.get complete with bandwidth: $(gbits_per_second) Gbps"
    return res
end
