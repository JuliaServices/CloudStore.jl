nbytes(x::AbstractVector{UInt8}) = length(x)
nbytes(x::String) = filesize(x)
nbytes(x::IOBuffer) = x.size - x.ptr + 1
nbytes(x::IO) = eof(x) ? 0 : bytesavailable(x)

function prepBody(x::RequestBodyType, compress::Bool, zlibng::Bool)
    if x isa String || x isa IOStream
        body = Mmap.mmap(x)
    elseif x isa IOBuffer
        if x.ptr == 1
            body = x.data
        else
            body = x.data[x.ptr:end]
        end
    elseif x isa IO
        body = read(x)
    else
        body = x
    end
    return compress ? transcode(compressor(zlibng), body) : body
end

function prepBodyMultipart(x::RequestBodyType, compress::Bool, zlibng::Bool)
    if x isa String
        body = open(x, "r") # need to close later!
    elseif x isa AbstractVector{UInt8}
        body = IOBuffer(x)
    else
        @assert x isa IO
        body = x
    end
    return compress ? compressorstream(zlibng)(body) : body
end

_read(body, n) = read(body, n)

function _read(body::IOBuffer, n)
    if body.ptr + n > body.size
        n = body.size - body.ptr + 1
    end
    res = @view body.data[body.ptr:body.ptr + n - 1]
    body.ptr += n
    return res
end

compressorstream(zlibng) = zlibng ? CodecZlibNG.GzipCompressorStream : CodecZlib.GzipCompressorStream
compressor(zlibng) = zlibng ? CodecZlibNG.GzipCompressor : CodecZlib.GzipCompressor

function putObjectImpl(x::AbstractStore, key::String, in::RequestBodyType;
    multipartThreshold::Int=MULTIPART_THRESHOLD,
    partSize::Int=MULTIPART_SIZE,
    batchSize::Int=defaultBatchSize(),
    allowMultipart::Bool=true,
    zlibng::Bool=false,
    compress::Bool=false, credentials=nothing,
    lograte::Bool=false, kw...)

    start_time = time()
    N = nbytes(in)
    wbytes = Threads.Atomic{Int}(0)
    if N <= multipartThreshold || !allowMultipart
        body = prepBody(in, compress, zlibng)
        resp = putObject(x, key, body; credentials, kw...)
        wbytes[] = get(resp.request.context, :nbytes_written, 0)
        obj = Object(x, credentials, key, N, etag(HTTP.header(resp, "ETag")))
        @goto done
    end
    # multipart upload
    uploadState = startMultipartUpload(x, key; credentials, kw...)
    url = makeURL(x, key)
    eTags = String[]
    sync = OrderedSynchronizer(1)
    body = prepBodyMultipart(in, compress, zlibng)
    nTasks = cld(N, partSize)
    nLoops = cld(nTasks, batchSize)
    # while nLoops * batchSize is the *max* # of iterations we'll do
    # if we're compressing, it will likely be much fewer, so we add
    # eof(body) checks to both loops to account for earlier termination
    for j = 1:nLoops
        @sync for i = 1:batchSize
            eof(body) && break
            n = (j - 1) * batchSize + i
            part = _read(body, partSize)
            Threads.@spawn begin
                _n = $n
                parteTag, wb = uploadPart(x, url, $part, _n, uploadState; credentials, kw...)
                Threads.atomic_add!(wbytes, wb)
                # we synchronize the eTags here because the order matters
                # for the final call to completeMultipartUpload
                put!(() -> push!(eTags, parteTag), sync, _n)
            end
        end
        eof(body) && break
    end
    # cleanup body
    if body isa compressorstream(zlibng)
        close(body)
        body = body.stream
    end
    if in isa String
        close(body)
    end
    eTag = completeMultipartUpload(x, url, eTags, uploadState; credentials, kw...)
    obj = Object(x, credentials, key, N, eTag)
@label done
    end_time = time()
    bytes = wbytes[]
    gbits_per_second = bytes == 0 ? 0 : (((8 * bytes) / 1e9) / (end_time - start_time))
    lograte && @info "CloudStore.put complete with bandwidth: $(gbits_per_second) Gbps"
    return obj
end
