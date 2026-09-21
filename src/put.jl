nbytes(x::AbstractVector{UInt8}) = length(x)
nbytes(x::String) = filesize(x)
nbytes(x::Base.GenericIOBuffer) = x.size - x.ptr + 1
nbytes(x::IO) = eof(x) ? 0 : bytesavailable(x)

"""
    iobufferbytes(x::Base.GenericIOBuffer) -> AbstractVector{UInt8}

Return the readable contents of `x`, i.e. the bytes in `[x.ptr, x.size]`.

The allocated capacity of `x.data` can extend past the readable contents.
The view excludes that unused capacity and respects the current read position.
On Julia 1.11+, storage can be `Memory`; compression materializes a compatible
byte vector separately.

The returned view retains the buffer storage. Callers must not write to or resize
`x` until the upload, including retries, completes.
"""
function iobufferbytes(x::Base.GenericIOBuffer)
    lo, hi = x.ptr, x.size
    lo > hi && return UInt8[]
    data = x.data
    if lo == 1 && hi == length(data) && (data isa Vector{UInt8} || data isa Base.CodeUnits{UInt8})
        return data
    end
    return view(data, lo:hi)
end

# HTTP 1 cannot write non-strided byte views (for example multipart String
# storage). Preserve its materialized fallback; HTTP 2 accepts borrowed views.
uploadbytes(body) = isdefined(HTTP, :BytesBody) || body isa Union{StridedVector{UInt8},Base.CodeUnits{UInt8}} ? body : Vector{UInt8}(body)

function prepBody(x::RequestBodyType, compress::Bool, zlibng::Bool)
    if x isa String || x isa IOStream
        body = Mmap.mmap(x)
    elseif x isa Base.GenericIOBuffer
        body = iobufferbytes(x)
    elseif x isa IO
        body = read(x)
    else
        body = x
    end
    if compress
        input = body isa Union{Vector{UInt8},Base.CodeUnits{UInt8}} ? body : Vector{UInt8}(body)
        return transcode(compressor(zlibng), input)
    end
    return uploadbytes(body)
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

function _read(body::Base.GenericIOBuffer, n)
    if body.ptr + n > body.size
        n = body.size - body.ptr + 1
    end
    res = @view body.data[body.ptr:body.ptr + n - 1]
    body.ptr += n
    return uploadbytes(res)
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
    contentType::Union{Nothing,AbstractString}=nothing,
    headers=nothing,
    lograte::Bool=false, kw...)

    kw = merge((copyheaders=false,), (; kw...))
    start_time = time()
    N = nbytes(in)
    wbytes = Threads.Atomic{Int}(0)
    progressReported = false
    if N <= multipartThreshold || !allowMultipart
        body = prepBody(in, compress, zlibng)
        wire_bytes = nbytes(body)
        resp = putObject(x, key, body;
            contentType, headers=transferheaders(headers), credentials, kw...)
        wbytes[] = wire_bytes
        obj = Object(x, credentials, resourceKey(key), N, etag(HTTP.header(resp, "ETag")))
        @goto done
    end
    # multipart upload
    uploadState = startMultipartUpload(x, key;
        contentType, headers=transferheaders(headers), credentials, kw...)
    url = makeURL(x, key)
    eTags = String[]
    local eTag
    try
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
                        results[$index] = uploadPart(
                            x, url, $part, $n, uploadState; credentials, kw...)
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
        eTag = completeMultipartUpload(x, url, eTags, uploadState;
            contentType, headers=transferheaders(headers), credentials, kw...)
    catch
        try
            abortMultipartUpload(x, url, uploadState; credentials, kw...)
        catch abort_exception
            @warn "Failed to abort multipart upload" exception=(abort_exception, catch_backtrace())
        end
        rethrow()
    end
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
