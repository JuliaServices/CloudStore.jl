module S3

using CloudBase.AWS, XMLDict, HTTP, CodecZlib
import ..ResponseBodyType, ..RequestBodyType, ..getObject, ..putObject, ..Object, ..etag, ..object

const Bucket = AWS.Bucket

# function list(; baseurl::String="https://s3.amazonaws.com/", credentials::Union{Nothing, AWS.Credentials}=nothing, verbose=0)
#     result = xml_dict(String(AWS.get(baseurl; credentials, verbose).body))["ListAllMyBucketsResult"]
#     #TODO: we're not getting the region right the buckets here; should we call the get region endpoint?
#     return [Bucket(x["Name"]) for x in result["Buckets"]["Bucket"]]
# end

# function create(x::Bucket; credentials::Union{Nothing, AWS.Credentials}=nothing, verbose=0)
#     AWS.put(x.baseurl; credentials, verbose)
# end

function list(x::Bucket; prefix="", credentials::Union{Nothing, AWS.Credentials}=nothing, maxKeys=1000, verbose=0)
    query = Dict("list-type" => "2")
    if !isempty(prefix)
        query["prefix"] = prefix
    end
    if maxKeys != 1000
        query["max-keys"] = string(maxKeys)
    end
    result = xml_dict(String(AWS.get(x.baseurl; query, credentials, verbose).body))["ListBucketResult"]
    contents = map(y -> object(x, y), result["Contents"])
    while result["IsTruncated"] == "true"
        query["continuation-token"] = result["NextContinuationToken"]
        resp = AWS.get(x.baseurl; query, credentials, verbose)
        result = xml_dict(String(resp.body))["ListBucketResult"]
        append!(contents, map(y -> object(x, y), result["Contents"]))
    end
    return contents
end

get(x::Object, out::ResponseBodyType=nothing; kw...) = get(x.store, x.key, out; kw...)
function get(x::Bucket, key::String, out::ResponseBodyType=nothing; kw...)
    return getObject(AWS, joinpath(x.baseurl, key), out, "s3"; kw...)
end

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

end # S3
