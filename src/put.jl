nbytes(x::AbstractVector{UInt8}) = length(x)
nbytes(x::String) = filesize(x)
nbytes(x::IOBuffer) = x.size - x.ptr + 1
nbytes(x::IO) = eof(x) ? 0 : bytesavailable(x)

"""
    iobufferbytes(x::IOBuffer) -> AbstractVector{UInt8}

Return the readable contents of `x`, i.e. the bytes in `[x.ptr, x.size]`.

`x.data` is the buffer's *allocated capacity*, which for a buffer that has been written
to extends past the data itself, so using it directly sent whatever happened to be in
the rest of the allocation. On Julia 1.11+ `x.data` is also a `Memory`, which
`transcode` does not accept, and slicing a `Memory` yields another `Memory`.

The zero-copy path is preserved when the buffer's contents exactly fill a type the
callers already handle.
"""
function iobufferbytes(x::IOBuffer)
    lo, hi = x.ptr, x.size
    lo > hi && return UInt8[]
    data = x.data
    if lo == 1 && hi == length(data) && (data isa Vector{UInt8} || data isa Base.CodeUnits{UInt8})
        return data
    end
    return Vector{UInt8}(view(data, lo:hi))
end

function prepBody(x::RequestBodyType, compress::Bool, zlibng::Bool)
    if x isa String || x isa IOStream
        body = Mmap.mmap(x)
    elseif x isa IOBuffer
        body = iobufferbytes(x)
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
    return compress ? compressorstream(zlibng)(body; stop_on_end=true) : body
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

function putObjectImpl(x::AbstractStore, key::Resource, in::RequestBodyType;
    multipartThreshold::Int=MULTIPART_THRESHOLD,
    partSize::Int=MULTIPART_SIZE,
    batchSize::Int=defaultBatchSize(),
    allowMultipart::Bool=true,
    zlibng::Bool=false,
    compress::Bool=false, credentials=nothing,
    progress=nothing,
    lograte::Bool=false, kw...)

    start_time = time()
    N = nbytes(in)
    wbytes = Threads.Atomic{Int}(0)
    progressReported = false
    if N <= multipartThreshold || !allowMultipart
        body = prepBody(in, compress, zlibng)
        resp = putObject(x, key, body; credentials, kw...)
        wbytes[] = get(resp.request.context, :nbytes_written, 0)
        obj = Object(x, credentials, resourceKey(key), N, etag(HTTP.header(resp, "ETag")))
        @goto done
    end
    # multipart upload
    uploadState = startMultipartUpload(x, key; credentials, kw...)
    url = makeURL(x, key)
    eTags = String[]
    body = prepBodyMultipart(in, compress, zlibng)
    partNumber = 0
    try
        # Compression can make incompressible input larger than its source, so the
        # source byte count cannot safely bound the number of output parts.
        while !eof(body)
            parts = Tuple{Int,Any}[]
            for _ = 1:batchSize
                eof(body) && break
                part = _read(body, partSize)
                isempty(part) && break
                partNumber += 1
                push!(parts, (partNumber, part))
            end
            isempty(parts) && break
            results = Vector{Tuple{String,Int}}(undef, length(parts))
            @sync for index in eachindex(parts)
                n, part = parts[index]
                Threads.@spawn begin
                    results[$index] = uploadPart(x, url, $part, $n, uploadState; credentials, kw...)
                end
            end
            for (parteTag, wb) in results
                push!(eTags, parteTag)
                Threads.atomic_add!(wbytes, wb)
                if progress !== nothing
                    progress(compress ? 0 : N, wbytes[])
                    progressReported = true
                end
            end
        end
    finally
        if body isa compressorstream(zlibng)
            wrapped = body.stream
            close(body)
            body = wrapped
        end
        in isa String && close(body)
    end
    eTag = completeMultipartUpload(x, url, eTags, uploadState; credentials, kw...)
    obj = Object(x, credentials, resourceKey(key), N, eTag)
@label done
    end_time = time()
    bytes = wbytes[]
    if progress !== nothing && !progressReported
        progress(bytes, bytes)
    end
    gbits_per_second = bytes == 0 ? 0 : (((8 * bytes) / 1e9) / (end_time - start_time))
    lograte && @info "CloudStore.put complete with bandwidth: $(gbits_per_second) Gbps"
    return obj
end
