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

function getObjectImpl(x::AbstractStore, key::String, out::ResponseBodyType=nothing;
    multipartThreshold::Int=MULTIPART_THRESHOLD,
    partSize::Int=MULTIPART_SIZE,
    batchSize::Int=defaultBatchSize(),
    allowMultipart::Bool=true,
    decompress::Bool=false,
    headers=HTTP.Headers(), kw...)

    url = makeURL(x, key)
    if allowMultipart
        # make a head request to see if the object happens to be empty
        # if so, it isn't valid to make a Range bytes request, so we'll short-circuit
        resp = API.headObject(x, url, headers; kw...)
        if HTTP.header(resp, "Content-Length") == "0"
            if out === nothing
                return resp.body
            elseif out isa String
                open(io -> nothing, out, "w")
                return out
            else
                return out
            end
        end
        HTTP.setheader(headers, contentRange(0:(multipartThreshold - 1)))
    end
    if out === nothing
        resp = getObject(x, url, headers; connection_limit=batchSize, kw...)
        res = resp.body
    elseif out isa String
        res = open(out, "w")
        if decompress
            res = GzipDecompressorStream(res)
        end
        resp = getObject(x, url, headers; response_stream=res, connection_limit=batchSize, kw...)
    else
        res = decompress ? GzipDecompressorStream(out) : out
        resp = getObject(x, url, headers; response_stream=res, connection_limit=batchSize, kw...)
    end
    if allowMultipart
        soff, eoff, total = parseContentRange(HTTP.header(resp, "Content-Range"))
        if (eoff + 1) < total
            nTasks = cld(total - eoff, partSize)
            nLoops = cld(nTasks, batchSize)
            sync = OrderedSynchronizer(1)
            if res isa AbstractVector{UInt8}
                resize!(res, total)
            end
            for j = 1:nLoops
                @sync for i = 1:batchSize
                    n = (j - 1) * batchSize + i
                    n > nTasks && break
                    let n=n, headers=copy(headers)
                        Threads.@spawn begin
                            rng = contentRange(((n - 1) * partSize + eoff + 1):min(total, (n * partSize) + eoff))
                            HTTP.setheader(headers, rng)
                            #TODO: in HTTP.jl, allow passing res as response_stream that we write to directly
                            r = getObject(x, url, headers; connection_limit=batchSize, kw...)
                            if res isa AbstractVector{UInt8}
                                off, off2, _ = parseContentRange(HTTP.header(r, "Content-Range"))
                                put!(sync, n) do
                                    copyto!(res, off + 1, r.body, 1, off2 - off + 1)
                                end
                            else
                                put!(sync, n) do
                                    write(res, r.body)
                                end
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
    elseif out === nothing && decompress
        res = transcode(GzipDecompressor, res)
    elseif decompress && res isa GzipDecompressorStream
        flush(res)
        res = out
    end
    return res
end
