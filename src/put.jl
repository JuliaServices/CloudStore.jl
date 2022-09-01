nbytes(x::AbstractVector{UInt8}) = length(x)
nbytes(x::String) = filesize(x)
nbytes(x::IOBuffer) = x.size - x.ptr + 1
nbytes(x::IO) = eof(x) ? 0 : bytesavailable(x)

function prepBody(x::RequestBodyType, compress::Bool)
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
    return compress ? transcode(GzipCompressor, body) : body
end

function prepBodyMultipart(x::RequestBodyType, compress::Bool)
    if x isa String
        body = open(x, "r") # need to close later!
    elseif x isa AbstractVector{UInt8}
        body = IOBuffer(x)
    else
        @assert x isa IO
        body = x
    end
    return compress ? GzipCompressorStream(body) : body
end

function putObject(mod, x::AbstractStore, key::String, in::RequestBodyType;
    multipartThreshold::Int=64_000_000,
    multipartSize::Int=16_000_000,
    allowMultipart::Bool=true,
    compress::Bool=false,
    kw...)
    N = nbytes(in)
    if N <= multipartThreshold || !allowMultipart
        body = prepBody(in, compress)
        resp = mod.putObjectSingle(x, key, body; kw...)
        return Object(x, key, "", etag(HTTP.header(resp, "ETag")), N, "")
    end
    # multipart upload
    uploadState = mod.startMultipartUpload(x, key; kw...)
    url = joinpath(x.baseurl, key)
    eTags = String[]
    s = OrderedSynchronizer(1)
    i = 1
    body = prepBodyMultipart(in, compress)
    @sync for i = 1:cld(N, multipartSize)
        # while cld(N, multipartSize) is the *most* # of parts; w/ compression, we won't need as many loops
        # so we make sure to check eof and break if we finish early
        part = read(body, multipartSize)
        # @show i, length(part)
        let i=i, part=part
            Threads.@spawn begin
                eTag = mod.uploadPart(url, part, i, uploadState; kw...)
                let eTag=eTag
                    put!(() -> push!(eTags, eTag), s, i)
                end
            end
        end
        eof(body) && break
    end
    # cleanup body
    if body isa GzipDecompressorStream
        body = body.stream
    end
    if in isa String
        close(body)
    end
    # println("closing multipart")
    # @show eTags
    eTag = mod.completeMultipartUpload(url, eTags, uploadState; kw...)
    return Object(x, key, "", eTag, N, "")
end
