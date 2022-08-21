"""
CloudBase provides very simple HTTP clients for cloud services
(AWS, Azure, Google Cloud), which consists of 2 main components:
  * Computing credentials necessary for authenticated cloud requests
  * Signing requests with the credentials

CloudStore provides a very simple, consistent, and performant API for
object management with cloud services.
  * Create/list/delete buckets/containers
  * Put object
    * in single operation
    * from existing object (copy)
    * for large objects, split into parts and upload in parallel
    * from ObjectWithParts, replace parts with new data, and upload in parallel (using UploadPartCopy/Put Block List w/ committed parts)
  * Get Object
    * get object metadata
    * get object in single operation
    * get large objects in parallel, either by concurrent parts/block, or by concurrent range requests
  * list objects in bucket/container

* allow compressing objects auto when putting; decompressing auto when getting
* allow public get from s3:// azure:// urls

Object model:
  * Bucket/Container: returned from create, list, arg for object operations, can create manually
  * Object: returned from put, get, list objects. can be used for put/get. 

"""
module CloudStore

using HTTP, CodecZlib
import CloudBase.AWS, CloudBase.Azure
import WorkerUtilities: OrderedSynchronizer

const ResponseBodyType = Union{Nothing, String, IO}
const RequestBodyType = Union{AbstractVector{UInt8}, String, IO}

function assembleresponse(mod, url::String, out::ResponseBodyType, service; decompress::Bool=false, kw...)
    if out === nothing
        resp = request(mod, url, 1, service; kw...)
        res = resp.body
    elseif out isa String
        res = open(out, "w")
        if decompress
            res = GzipDecompressorStream(res)
        end
        resp = request(mod, url, 1, service; response_stream=res, kw...)
    else
        res = decompress ? GzipDecompressorStream(out) : out
        resp = request(mod, url, 1, service; response_stream=res, kw...)
    end
    parts = HTTP.header(resp, "x-amz-mp-parts-count")
    if !isempty(parts)
        n = parse(Int, parts)
        x = OrderedSynchronizer(2)
        if res isa Vector{UInt8}
            # TODO: resize! res to full size and each part copyto!
            @sync for i = 2:n
                let i=i
                    errormonitor(Threads.@spawn begin 
                        resp = request(mod, url, i, service; kw...)
                        let resp=resp
                            put!(x, i) do
                                append!(res, resp.body)
                            end
                        end
                    end)
                end
            end
        else
            @sync for i = 2:n
                let i=i
                    errormonitor(Threads.@spawn begin
                        mod.open("GET", string(url, "?partNumber=$i"); service, kw...) do stream
                            startread(stream)
                            put!(x, i) do
                                write(res, stream)
                            end
                        end
                    end)
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

function request(mod, url, part, service; kw...)
    return mod.get(string(url, "?partNumber=$part"); service, kw...)
end

module S3

using CloudBase.AWS, XMLDict, HTTP, CodecZlib
import WorkerUtilities: OrderedSynchronizer
import ..ResponseBodyType, ..RequestBodyType, ..assembleresponse

const Bucket = AWS.Bucket

# function list(; baseurl::String="https://s3.amazonaws.com/", account::Union{Nothing, AWS.Credentials}=nothing, verbose=0)
#     result = xml_dict(String(AWS.get(baseurl; account, verbose).body))["ListAllMyBucketsResult"]
#     #TODO: we're not getting the region right the buckets here; should we call the get region endpoint?
#     return [Bucket(x["Name"]) for x in result["Buckets"]["Bucket"]]
# end

# function create(x::Bucket; account::Union{Nothing, AWS.Credentials}=nothing, verbose=0)
#     AWS.put(x.baseurl; account, verbose)
# end

struct Object
    bucket::Bucket
    key::String
    lastModified::String
    eTag::String
    size::Int
    storageClass::String
end

etag(x) = strip(x, '"')
object(b, x) = Object(b, x["Key"], x["LastModified"], etag(x["ETag"]), parse(Int, x["Size"]), x["StorageClass"])

function list(x::Bucket; prefix="", account::Union{Nothing, AWS.Credentials}=nothing, maxKeys=1000, verbose=0)
    query = Dict("list-type" => "2")
    if !isempty(prefix)
        query["prefix"] = prefix
    end
    if maxKeys != 1000
        query["max-keys"] = string(maxKeys)
    end
    result = xml_dict(String(AWS.get(x.baseurl; query, account, verbose).body))["ListBucketResult"]
    contents = map(y -> object(x, y), result["Contents"])
    while result["IsTruncated"] == "true"
        query["continuation-token"] = result["NextContinuationToken"]
        resp = AWS.get(x.baseurl; query, account, verbose)
        result = xml_dict(String(resp.body))["ListBucketResult"]
        append!(contents, map(y -> object(x, y), result["Contents"]))
    end
    return contents
end

get(x::Object, out::ResponseBodyType=nothing; kw...) = get(x.bucket, x.key, out; kw...)
function get(x::Bucket, key::String, out::ResponseBodyType=nothing; kw...)
    return assembleresponse(AWS, joinpath(x.baseurl, key), out, "s3"; kw...)
end

nbytes(x::AbstractVector{UInt8}) = length(x)
nbytes(x::String) = filesize(x)
nbytes(x::IOBuffer) = x.size - x.ptr + 1
nbytes(x::IO) = eof(x) ? 0 : bytesavailable(x)

using Mmap

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

function put(x::Bucket, key::String, in::RequestBodyType;
    multipartThreshold::Int=64_000_000,
    multipartSize::Int=16_000_000,
    allowMultipart::Bool=true,
    compress::Bool=false,
    kw...)
    N = nbytes(in)
    if N <= multipartThreshold || !allowMultipart
        body = prepBody(in, compress)
        resp = AWS.put(joinpath(x.baseurl, key), [], body; service="s3", kw...)
        return Object(x, key, "", etag(HTTP.header(resp, "ETag")), N, "")
    end
    # multipart upload
    resp = AWS.post(joinpath(x.baseurl, key); query=Dict("uploads" => ""), service="s3", retry_non_idempotent=true, kw...)
    uploadId = string(xml_dict(String(resp.body))["InitiateMultipartUploadResult"]["UploadId"])
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
            errormonitor(Threads.@spawn begin
                resp = AWS.put(url, [], part; query=Dict("partNumber" => string(i), "uploadId" => uploadId), service="s3", kw...)
                let resp=resp
                    put!(s, i) do
                        push!(eTags, HTTP.header(resp, "ETag"))
                    end
                end
            end)
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
    xmlbody = Dict("Part" => [Dict("PartNumber" => string(i), "ETag" => eTag) for (i, eTag) in enumerate(eTags)])
    resp = AWS.post(url; query=Dict("uploadId" => uploadId),
        body=XMLDict.dict_xml(Dict("CompleteMultipartUpload" => xmlbody)), service="s3", kw...)
    return Object(x, key, "", etag(HTTP.header(resp, "ETag")), N, "")
end

end # S3

# GCP

# credentials
 # https://cloud.google.com/storage/docs/authentication#user_accounts

end # module CloudStore
