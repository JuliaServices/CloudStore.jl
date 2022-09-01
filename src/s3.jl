module S3

using CloudBase.AWS, XMLDict, HTTP, CodecZlib
import ..ResponseBodyType, ..RequestBodyType, ..getObject, ..putObject, ..Object, ..etag, ..asArray

const Bucket = AWS.Bucket

# function list(; baseurl::String="https://s3.amazonaws.com/", credentials::Union{Nothing, AWS.Credentials}=nothing, verbose=0)
#     result = xml_dict(String(AWS.get(baseurl; credentials, verbose).body))["ListAllMyBucketsResult"]
#     #TODO: we're not getting the region right the buckets here; should we call the get region endpoint?
#     return [Bucket(x["Name"]) for x in result["Buckets"]["Bucket"]]
# end

# function create(x::Bucket; credentials::Union{Nothing, AWS.Credentials}=nothing, verbose=0)
#     AWS.put(x.baseurl; credentials, verbose)
# end

object(b, x) = Object(b, x["Key"], x["LastModified"], etag(x["ETag"]), parse(Int, x["Size"]), x["StorageClass"])

function list(x::Bucket; prefix="", maxKeys=1000, kw...)
    if maxKeys > 1000
        @warn "S3 only supports 1000 keys per request: `$maxKeys` requested"
        maxKeys = 1000
    end
    query = Dict("list-type" => "2")
    if !isempty(prefix)
        query["prefix"] = prefix
    end
    if maxKeys != 1000
        query["max-keys"] = string(maxKeys)
    end
    result = xml_dict(String(AWS.get(x.baseurl; query, service="s3", kw...).body))["ListBucketResult"]
    if parse(Int, result["KeyCount"]) == 0
        return Object[]
    end
    contents = map(y -> object(x, y), asArray(result["Contents"]))
    while result["IsTruncated"] == "true"
        query["continuation-token"] = result["NextContinuationToken"]
        result = xml_dict(String(AWS.get(x.baseurl; query, service="s3", kw...).body))["ListBucketResult"]
        if parse(Int, result["KeyCount"]) > 0
            append!(contents, map(y -> object(x, y), asArray(result["Contents"])))
        else
            break
        end
    end
    return contents
end

get(x::Object, out::ResponseBodyType=nothing; kw...) = get(x.store, x.key, out; kw...)
function get(x::Bucket, key::String, out::ResponseBodyType=nothing; kw...)
    return getObject(AWS, joinpath(x.baseurl, key), out, "s3"; kw...)
end

head(x::Object; kw...) = head(x.store, x.key; kw...)
head(x::Bucket, key::String; kw...) = Dict(AWS.head(joinpath(x.baseurl, key); service="s3", kw...).headers)

put(x::Bucket, key::String, in::RequestBodyType; kw...) =
    putObject(S3, x, key, in; kw...)

putObjectSingle(x::Bucket, key, body; kw...) = AWS.put(joinpath(x.baseurl, key), [], body; service="s3", kw...)

function startMultipartUpload(x::Bucket, key; kw...)
    resp = AWS.post(joinpath(x.baseurl, key); query=Dict("uploads" => ""), service="s3", retry_non_idempotent=true, kw...)
    return xml_dict(String(resp.body))["InitiateMultipartUploadResult"]["UploadId"]
end

function uploadPart(url, part, partNumber, uploadId; kw...)
    resp = AWS.put(url, [], part; query=Dict("partNumber" => string(partNumber), "uploadId" => uploadId), service="s3", kw...)
    return HTTP.header(resp, "ETag")
end

function completeMultipartUpload(url, eTags, uploadId; kw...)
    body = XMLDict.node_xml("CompleteMultipartUpload", Dict("Part" => [Dict("PartNumber" => string(i), "ETag" => eTag) for (i, eTag) in enumerate(eTags)]))
    resp = AWS.post(url; query=Dict("uploadId" => uploadId), body, service="s3", kw...)
    return etag(HTTP.header(resp, "ETag"))
end

delete(x::Bucket, key; kw...) = AWS.delete(joinpath(x.baseurl, key); service="s3", kw...)
delete(x::Object; kw...) = delete(x.store, x.key; kw...)

end # S3
