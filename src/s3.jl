module S3

using CloudBase.AWS, XMLDict, HTTP, CodecZlib
using ..API

const Bucket = AWS.Bucket

API.cloudName(::Bucket) = "S3"

# function list(; baseurl::String="https://s3.amazonaws.com/", credentials::Union{Nothing, AWS.Credentials}=nothing, verbose=0)
#     result = xml_dict(String(AWS.get(baseurl; credentials, verbose).body))["ListAllMyBucketsResult"]
#     #TODO: we're not getting the region right the buckets here; should we call the get region endpoint?
#     return [Bucket(x["Name"]) for x in result["Buckets"]["Bucket"]]
# end

# function create(x::Bucket; credentials::Union{Nothing, AWS.Credentials}=nothing, verbose=0)
#     AWS.put(x.baseurl; credentials, verbose)
# end

object(b::Bucket, x) = Object(b, x["Key"], x["LastModified"], API.etag(x["ETag"]), parse(Int, x["Size"]), x["StorageClass"])

API.maxListKeys(::Bucket) = 1000
API.listMaxKeysQuery(::Bucket) = "max-keys"
API.continuationToken(::Bucket) = "continuation-token"

function API.listObjects(x::Bucket, query, result=nothing; kw...)
    query["list-type"] = "2"
    result = xml_dict(String(AWS.get(x.baseurl; query, service="s3", kw...).body))["ListBucketResult"]
    if parse(Int, result["KeyCount"]) == 0
        return (Object[], "")
    end
    contents = map(y -> object(x, y), API.asArray(result["Contents"]))
    return (contents, result["IsTruncated"] == "true" ? result["NextContinuationToken"] : "")
end

list(x::Bucket; kw...) = API.listObjectsImpl(x; kw...)

function API.getObject(x::Bucket, url, rng; kw...)
    if rng === nothing
        return AWS.get(url; service="s3", kw...)
    else
        return AWS.get(url, [rng]; service="s3", kw...)
    end
end

get(x::Object, out::ResponseBodyType=nothing; kw...) = get(x.store, x.key, out; kw...)
get(args...; kw...) = API.getObjectImpl(args...; kw...)

head(x::Object; kw...) = head(x.store, x.key; kw...)
head(x::Bucket, key::String; kw...) = Dict(AWS.head(joinpath(x.baseurl, key); service="s3", kw...).headers)

put(args...; kw...) = API.putObjectImpl(args...; kw...)
put(x::Object; kw...) = put(x.store, x.key; kw...)

API.putObject(x::Bucket, key, body; kw...) = AWS.put(joinpath(x.baseurl, key), [], body; service="s3", kw...)

function API.startMultipartUpload(x::Bucket, key; kw...)
    resp = AWS.post(joinpath(x.baseurl, key); query=Dict("uploads" => ""), service="s3", retry_non_idempotent=true, kw...)
    return xml_dict(String(resp.body))["InitiateMultipartUploadResult"]["UploadId"]
end

function API.uploadPart(x::Bucket, url, part, partNumber, uploadId; kw...)
    resp = AWS.put(url, [], part; query=Dict("partNumber" => string(partNumber), "uploadId" => uploadId), service="s3", kw...)
    return HTTP.header(resp, "ETag")
end

function API.completeMultipartUpload(x::Bucket, url, eTags, uploadId; kw...)
    body = XMLDict.node_xml("CompleteMultipartUpload", Dict("Part" => [Dict("PartNumber" => string(i), "ETag" => eTag) for (i, eTag) in enumerate(eTags)]))
    resp = AWS.post(url; query=Dict("uploadId" => uploadId), body, service="s3", kw...)
    return API.etag(HTTP.header(resp, "ETag"))
end

delete(x::Bucket, key; kw...) = AWS.delete(joinpath(x.baseurl, key); service="s3", kw...)
delete(x::Object; kw...) = delete(x.store, x.key; kw...)

end # S3
