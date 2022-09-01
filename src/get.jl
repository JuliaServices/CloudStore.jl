# Content-Range: bytes 0-9/443
function parseContentRange(str)
    m = match(r"bytes (\d+)-(\d+)/(\d+)", str)
    m === nothing && error("invalid Content-Range: $str")
    return (parse(Int, m[1]), parse(Int, m[2]), parse(Int, m[3]))
end

function getObject(mod, url::String, out::ResponseBodyType, service;
    multipartThreshold::Int=MULTIPART_THRESHOLD,
    partSize::Int=MULTIPART_SIZE,
    batchSize::Int=defaultBatchSize(),
    allowMultipart::Bool=true,
    decompress::Bool=false, kw...)

    rng = allowMultipart ? (0:(multipartThreshold - 1)) : nothing
    if out === nothing
        resp = request(mod, url, rng, service; kw...)
        res = resp.body
    elseif out isa String
        res = open(out, "w")
        if decompress
            res = GzipDecompressorStream(res)
        end
        resp = request(mod, url, rng, service; response_stream=res, kw...)
    else
        res = decompress ? GzipDecompressorStream(out) : out
        resp = request(mod, url, rng, service; response_stream=res, kw...)
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
                        Threads.@spawn begin
                            rng = ((n - 1) * partSize + eoff + 1):min(total, (n * partSize) + eoff)
                            #TODO: in HTTP.jl, allow passing res as response_stream that we write to directly
                            resp = request(mod, url, rng, service; kw...)
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

request(mod, url, ::Nothing, service; kw...) = mod.get(url; service, kw...)
request(mod, url, rng, service; kw...) =
    mod.get(url, ["Range" => "bytes=$(first(rng))-$(last(rng))"]; service, kw...)
