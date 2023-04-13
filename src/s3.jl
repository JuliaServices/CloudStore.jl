module S3

using CloudBase.AWS, XMLDict, HTTP, CodecZlib
using ..API
import ..parseAWSBucketRegionKey

const Bucket = AWS.Bucket
const Credentials = AWS.Credentials

API.cloudName(::Bucket) = "S3"

object(b::Bucket, creds, x) = Object(b, creds, x["Key"], parse(Int, x["Size"]), API.etag(x["ETag"]))

API.maxListKeys(::Bucket) = 1000
API.listMaxKeysQuery(::Bucket) = "max-keys"
API.continuationToken(::Bucket) = "continuation-token"

function API.listObjects(x::Bucket, query, result=nothing; credentials=nothing, kw...)
    query["list-type"] = "2"
    result = xml_dict(String(AWS.get(x.baseurl; credentials, query, service="s3", kw...).body))["ListBucketResult"]
    if parse(Int, result["KeyCount"]) == 0
        return (Object[], "")
    end
    contents = map(y -> object(x, credentials, y), API.asArray(result["Contents"]))
    return (contents, result["IsTruncated"] == "true" ? result["NextContinuationToken"] : "")
end

list(x::Bucket; kw...) = API.listObjectsImpl(x; kw...)

API.getObject(x::Bucket, url, headers; kw...) = AWS.get(url, headers; service="s3", kw...)

get(x::Object, out::ResponseBodyType=nothing; kw...) = get(x.store, x.key, out; credentials=x.credentials, kw...)
get(args...; kw...) = API.getObjectImpl(args...; kw...)

API.headObject(x::Bucket, url, headers; kw...) = AWS.head(url; headers, service="s3", kw...)
head(x::Object; kw...) = head(x.store, x.key; credentials=x.credentials, kw...)
head(x::Bucket, key::String; kw...) = API.headObjectImpl(x, key; kw...)

put(args...; kw...) = API.putObjectImpl(args...; kw...)
put(x::Object; kw...) = put(x.store, x.key; credentials=x.credentials, kw...)

API.putObject(x::Bucket, key, body; kw...) = AWS.put(API.makeURL(x, key), [], body; service="s3", kw...)

function API.startMultipartUpload(x::Bucket, key; kw...)
    resp = AWS.post(API.makeURL(x, key); query=Dict("uploads" => ""), service="s3", retry_non_idempotent=true, kw...)
    return xml_dict(String(resp.body))["InitiateMultipartUploadResult"]["UploadId"]
end

function API.uploadPart(x::Bucket, url, part, partNumber, uploadId; kw...)
    resp = AWS.put(url, [], part; query=Dict("partNumber" => string(partNumber), "uploadId" => uploadId), service="s3", kw...)
    return (HTTP.header(resp, "ETag"), Base.get(resp.request.context, :nbytes_written, 0))
end

function API.completeMultipartUpload(x::Bucket, url, eTags, uploadId; kw...)
    body = XMLDict.node_xml("CompleteMultipartUpload", Dict("Part" => [Dict("PartNumber" => string(i), "ETag" => eTag) for (i, eTag) in enumerate(eTags)]))
    resp = AWS.post(url; query=Dict("uploadId" => uploadId), body, service="s3", kw...)
    return API.etag(HTTP.header(resp, "ETag"))
end

delete(x::Bucket, key; kw...) = AWS.delete(API.makeURL(x, key); service="s3", kw...)
delete(x::Object; kw...) = delete(x.store, x.key; credentials=x.credentials, kw...)

for func in (:list, :get, :head, :put, :delete)
    @eval function $func(url::AbstractString, args...; region=nothing, nowarn::Bool=false, parseLocal::Bool=false, kw...)
        ok, accelerate, host, bucket, reg, key = parseAWSBucketRegionKey(url; parseLocal=parseLocal)
        ok || throw(ArgumentError("invalid url for S3.$($func): `$url`"))
        if region === nothing
            nowarn || @warn "`region` keyword argument not provided to `S3.$($func)` and undetected from url.  Defaulting to `us-east-1`"
            region = AWS.AWS_DEFAULT_REGION
        end
        if key !== nothing
            return $func(S3.Bucket(bucket, region; accelerate, host), key, args...; kw...)
        else
            return $func(S3.Bucket(bucket, region; accelerate, host), args...; kw...)
        end
    end
end

end # S3
