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
    headers=HTTP.Headers(), kw...)
    url = makeURL(x, key)
    if allowMultipart
        HTTP.setheader(headers, contentRange(0:(multipartThreshold - 1)))
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

function check_redirect(key, resp)
    if HTTP.isredirect(resp)
        try
            throw(HTTP.StatusError(resp.status, resp.request.method, resp.request.target, resp))
        catch
            # The ArgumentError will be caused by the HTTP error to provide more context
            throw(ArgumentError("Invalid object key: $key"))
        end
    end
end

decompressorstream(zlibng) = zlibng ? CodecZlibNG.GzipDecompressorStream : CodecZlib.GzipDecompressorStream
decompressor(zlibng) = zlibng ? CodecZlibNG.GzipDecompressor : CodecZlib.GzipDecompressor

function getObjectImpl(x::AbstractStore, key::String, out::ResponseBodyType=nothing;
    multipartThreshold::Int=MULTIPART_THRESHOLD,
    partSize::Int=MULTIPART_SIZE,
    batchSize::Int=defaultBatchSize(),
    allowMultipart::Bool=true,
    decompress::Bool=false,
    zlibng::Bool=false,
    headers=HTTP.Headers(), kw...)

    # keyword arg handling
    if (out isa AbstractVector{UInt8} && length(out) <= multipartThreshold)
        allowMultipart = false
        res = out
    end
    start_time = time()
    url = makeURL(x, key)
    if allowMultipart
        # make a head request to see if the object happens to be empty
        # if so, it isn't valid to make a Range bytes request, so we'll short-circuit
        resp = API.headObject(x, url, headers; kw...)
        check_redirect(key, resp)
        contentLength = parse(Int, HTTP.header(resp, "Content-Length", "0"))
        if contentLength == 0
            if out === nothing
                return resp.body
            elseif out isa String
                open(io -> nothing, out, "w")
                return out
            else
                return out
            end
        elseif out === nothing
            # allocate the full, final buffer upfront since we know the length
            res = Vector{UInt8}(undef, contentLength)
        elseif out isa AbstractVector{UInt8}
            length(out) < contentLength && throw(ArgumentError("out ($(length(out))) must at least be of length $contentLength"))
            if length(out) > contentLength
                res = view(out, 1:contentLength)
            else
                res = out
            end
        end
        HTTP.setheader(headers, contentRange(0:min(multipartThreshold - 1, contentLength - 1)))
    end
    if out === nothing && allowMultipart || out isa AbstractVector{UInt8}
        _res = view(res, 1:min(multipartThreshold, length(res)))
        resp = getObject(x, url, headers; response_stream=_res, kw...)
    elseif out === nothing
        resp = getObject(x, url, headers; kw...)
    elseif out isa String
        res = open(out, "w")
        if decompress
            res = decompressorstream(zlibng)(res)
        end
        resp = getObject(x, url, headers; response_stream=res, kw...)
    else
        res = decompress ? decompressorstream(zlibng)(out) : out
        resp = getObject(x, url, headers; response_stream=res, kw...)
    end
    check_redirect(key, resp)
    nbytes = Threads.Atomic{Int}(get(resp.request.context, :nbytes, 0))
    if allowMultipart
        partSize > 0 || throw(ArgumentError("partSize must be > 0"))
        batchSize > 0 || throw(ArgumentError("batchSize must be > 0"))
        _, eoff, total = parseContentRange(HTTP.header(resp, "Content-Range"))
        if (eoff + 1) < total
            nTasks = cld((total - 1) - eoff, partSize)
            nLoops = cld(nTasks, batchSize)
            sync = OrderedSynchronizer(1)
            for j = 1:nLoops
                @sync for i = 1:batchSize
                    n = (j - 1) * batchSize + i
                    n > nTasks && break
                    Threads.@spawn begin
                        _n = $n
                        _headers = copy(headers)
                        rng = ((_n - 1) * partSize + eoff + 1):min(total - 1, (_n * partSize) + eoff)
                        HTTP.setheader(_headers, contentRange(rng))
                        if out === nothing || out isa AbstractVector{UInt8}
                            # the Content-Range header is 0-indexed, but the view is 1-indexed
                            _rng = (first(rng) + 1):(last(rng) + 1)
                            # we pass just this task's slice of the overall buffer to be filled in
                            # directly as HTTP receives the response body
                            _res = view(res, _rng)
                            r = getObject(x, url, _headers; response_stream=_res, kw...)
                            Threads.atomic_add!(nbytes, get(r.request.context, :nbytes, 0))
                        else
                            r = getObject(x, url, _headers; kw...)
                            Threads.atomic_add!(nbytes, get(r.request.context, :nbytes, 0))
                            put!(sync, n) do
                                write(res, r.body)
                            end
                        end
                    end
                end
            end
        end
    end
    if out isa String
        close(res)
        res = out
    elseif out isa AbstractVector{UInt8} && decompress && res isa SubArray
        # the user passed a pre-allocated buffer and wants to decompress
        transcode(decompressor(zlibng), copy(res), out)
        res = out
    elseif (out === nothing || out isa AbstractVector{UInt8}) && decompress
        res = transcode(decompressor(zlibng), res)
    elseif decompress && res isa decompressorstream(zlibng)
        flush(res)
        res = out
    end
    end_time = time()
    bytes = nbytes[]
    gbits_per_second = bytes == 0 ? 0 : (((8 * bytes) / 1e9) / (end_time - start_time))
    @debug "CloudStore.get complete with bandwidth: $(gbits_per_second) Gbps"
    return res
end
