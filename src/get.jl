# list
function listObjectsImpl(x::AbstractStore; prefix="", maxKeys=maxListKeys(x), kw...)
    if maxKeys > maxListKeys(x)
        @warn "$(cloudName(x)) only supports $(maxListKeys(x)) keys per request: `$maxKeys` requested"
        maxKeys = maxListKeys(x)
    end
    query = Dict{String, String}()
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

# Content-Range: bytes 0-9/443
contentRange(rng) = "Range" => "bytes=$(first(rng))-$(last(rng))"

function parseContentRange(str)
    m = match(r"bytes (\d+)-(\d+)/(\d+)", str)
    m === nothing && error("invalid Content-Range: $str")
    return (parse(Int, m[1]), parse(Int, m[2]), parse(Int, m[3]))
end

function getObjectImpl(x::AbstractStore, key::String, out::ResponseBodyType;
    multipartThreshold::Int=MULTIPART_THRESHOLD,
    partSize::Int=MULTIPART_SIZE,
    batchSize::Int=defaultBatchSize(),
    allowMultipart::Bool=true,
    decompress::Bool=false, kw...)

    url = makeURL(x, key)
    rng = allowMultipart ? contentRange(0:(multipartThreshold - 1)) : nothing
    if out === nothing
        resp = getObject(x, url, rng; kw...)
        res = resp.body
    elseif out isa String
        res = open(out, "w")
        if decompress
            res = GzipDecompressorStream(res)
        end
        resp = getObject(x, url, rng; response_stream=res, kw...)
    else
        res = decompress ? GzipDecompressorStream(out) : out
        resp = getObject(x, url, rng; response_stream=res, kw...)
    end
    if allowMultipart
        soff, eoff, total = parseContentRange(HTTP.header(resp, "Content-Range"))
        if (eoff + 1) < total
            nTasks = cld(total - eoff, partSize)
            nLoops = cld(nTasks, batchSize)
            sync = OrderedSynchronizer(1)
            if res isa Vector{UInt8}
                resize!(res, total)
            end
            for j = 1:nLoops
                @sync for i = 1:batchSize
                    n = (j - 1) * batchSize + i
                    n > nTasks && break
                    let n=n
                        @show n
                        Threads.@spawn begin
                            rng = contentRange(((n - 1) * partSize + eoff + 1):min(total, (n * partSize) + eoff))
                            #TODO: in HTTP.jl, allow passing res as response_stream that we write to directly
                            resp = getObject(x, url, rng; kw...)
                            #TODO: verify Last-Modified in resp matches from 1st response?
                            #TODO: do If-Match eTag w/ AWS?
                            #TODO: pass generation for each GCP request?
                            let resp=resp
                                if res isa Vector{UInt8}
                                    off, off2, _ = parseContentRange(HTTP.header(resp, "Content-Range"))
                                    put!(sync, n) do
                                        copyto!(res, off + 1, resp.body, 1, off2 - off + 1)
                                    end
                                else
                                    put!(sync, n) do
                                        write(res, resp.body)
                                    end
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
