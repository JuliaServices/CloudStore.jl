nbytes(x::AbstractVector{UInt8}) = length(x)
nbytes(x::String) = filesize(x)
nbytes(x::IOBuffer) = max(x.size - x.ptr + 1, 0)
nbytes(x::IO) = eof(x) ? 0 : bytesavailable(x)

@inline function _iobuffer_data_view(x::IOBuffer)
    if x.ptr > x.size
        return view(x.data, 1:0)
    end
    return @view x.data[x.ptr:x.size]
end

function prepBody(x::RequestBodyType, compress::Bool, zlibng::Bool)
    if x isa String || x isa IOStream
        body = Mmap.mmap(x)
    elseif x isa IOBuffer
        body = _iobuffer_data_view(x)
    elseif x isa IO
        body = read(x)
    else
        body = x
    end
    if compress
        body isa Vector{UInt8} || (body = Vector{UInt8}(body))
        return transcode(compressor(zlibng), body)
    end
    return body
end

function prepBodyMultipart(x::RequestBodyType, compress::Bool, zlibng::Bool)
    if x isa String
        body = IOBuffer(Mmap.mmap(x))
    elseif x isa IOBuffer
        body = IOBuffer(_iobuffer_data_view(x))
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
    body = prepBodyMultipart(in, compress, zlibng)
    eTags = String[]
    sizehint!(eTags, cld(N, partSize))
    last_part = 0
    while !eof(body)
        @sync for i = 1:batchSize
            eof(body) && break
            last_part += 1
            length(eTags) < last_part && resize!(eTags, last_part)
            part = _read(body, partSize)
            Threads.@spawn begin
                _n = $last_part
                parteTag, wb = uploadPart(x, url, $part, _n, uploadState; credentials, kw...)
                Threads.atomic_add!(wbytes, wb)
                eTags[_n] = parteTag
            end
        end
    end
    # cleanup body
    if body isa compressorstream(zlibng)
        close(body)
    end
    resize!(eTags, last_part)
    eTag = completeMultipartUpload(x, url, eTags, uploadState; credentials, kw...)
    obj = Object(x, credentials, key, N, eTag)
@label done
    end_time = time()
    bytes = wbytes[]
    gbits_per_second = bytes == 0 ? 0 : (((8 * bytes) / 1e9) / (end_time - start_time))
    lograte && @info "CloudStore.put complete with bandwidth: $(gbits_per_second) Gbps"
    return obj
end
