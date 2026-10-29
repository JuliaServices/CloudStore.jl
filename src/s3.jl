module S3

using CloudBase.AWS, XMLDict, HTTP, CodecZlib, MD5
using ..API
import ..parseAWSBucketRegionKey

const Bucket = AWS.Bucket
const Credentials = AWS.Credentials

API.cloudName(::Bucket) = "S3"

# An `encoding-type=url` key is form-encoded: `+` is a space.
s3key(x, encoded) = encoded ? HTTP.unescapeuri(replace(x["Key"], '+' => ' ')) : x["Key"]
object(b::Bucket, creds, x, encoded) =
    Object(b, creds, s3key(x, encoded), parse(Int, x["Size"]), API.etag(x["ETag"]))

API.maxListKeys(::Bucket) = 1000
API.listMaxKeysQuery(::Bucket) = "max-keys"
API.continuationToken(::Bucket) = "continuation-token"

function API.listObjects(x::Bucket, query, result=nothing; credentials=nothing, kw...)
    query["list-type"] = "2"
    # Raw keys do not survive XML: control characters fail to parse, and XMLDict drops
    # a whitespace-only key.
    query["encoding-type"] = "url"
    result = xml_dict(String(AWS.get(x.baseurl; credentials, query, service="s3", kw...).body))["ListBucketResult"]
    # A page can be empty and still continue; only the token ends the listing.
    token = result["IsTruncated"] == "true" ? result["NextContinuationToken"] : ""
    # Common prefixes of a delimiter listing count in KeyCount but are not objects.
    encoded = Base.get(result, "EncodingType", "") == "url"
    contents = Object[object(x, credentials, y, encoded) for y in API.xmlelements(result, "Contents")]
    return (contents, token)
end

list(x::Bucket; kw...) = API.listObjectsImpl(x; kw...)

API.getObject(x::Bucket, url, headers; kw...) = AWS.get(url, headers; service="s3", kw...)

get(x::Object, out::ResponseBodyType=nothing; kw...) = get(x.store, x.key, out; credentials=x.credentials, kw...)
get(args...; kw...) = API.getObjectImpl(args...; kw...)

API.headObject(x::Bucket, url, headers; kw...) = AWS.head(url; headers, service="s3", kw...)
head(x::Object; kw...) = head(x.store, x.key; credentials=x.credentials, kw...)
head(x::Bucket, key::API.Resource; kw...) = API.headObjectImpl(x, key; kw...)
exists(x::Object; kw...) = exists(x.store, x.key; credentials=x.credentials, kw...)
exists(x::Bucket, key::API.Resource; kw...) = API.existsObjectImpl(x, key; kw...)

put(args...; kw...) = API.putObjectImpl(args...; kw...)
put(x::Object; kw...) = put(x.store, x.key; credentials=x.credentials, kw...)

function API.putObject(x::Bucket, key, body;
    contentType=nothing, headers=HTTP.Headers(), kw...)
    contentType === nothing || HTTP.setheader(headers, "Content-Type" => String(contentType))
    return AWS.put(API.makeURL(x, key), headers, body; service="s3", kw...)
end

function API.startMultipartUpload(x::Bucket, key;
    contentType=nothing, headers=HTTP.Headers(), kw...)
    return startupload(x, key; contentType, headers, retry_non_idempotent=true, kw...)
end

function startupload(x::Bucket, key; contentType=nothing, headers=HTTP.Headers(), kw...)
    contentType === nothing || HTTP.setheader(headers, "Content-Type" => String(contentType))
    resp = AWS.post(API.makeURL(x, key), headers;
        query=Dict("uploads" => ""), service="s3", kw...)
    API.multipartresponse(resp)
    return xml_dict(String(resp.body))["InitiateMultipartUploadResult"]["UploadId"]
end

function partrequest(url, part, partNumber, uploadId; headers=HTTP.Headers(), kw...)
    return AWS.put(url, headers, part;
        query=Dict("partNumber" => string(partNumber), "uploadId" => uploadId), service="s3", kw...)
end

function API.uploadPart(x::Bucket, url, part, partNumber, uploadId; kw...)
    resp = partrequest(url, part, partNumber, uploadId; kw...)
    return (HTTP.header(resp, "ETag"), length(part))
end

function API.completeMultipartUpload(x::Bucket, url, eTags, uploadId;
    contentType=nothing, headers=HTTP.Headers(), kw...)
    body = XMLDict.node_xml("CompleteMultipartUpload", Dict("Part" => [Dict("PartNumber" => string(i), "ETag" => eTag) for (i, eTag) in enumerate(eTags)]))
    # Caller headers describe the object and were sent when the upload started.
    # The completion request carries only its XML body. S3 asks clients to retry a
    # failed completion; an upload ID completes at most once, so a retry cannot
    # publish different data.
    try
        return completeupload(url, uploadId, body; attempts=3, retry_non_idempotent=true, kw...)
    catch e
        # A retry whose earlier attempt completed the upload (its response was lost)
        # finds no upload: HTTP 404, NoSuchUpload.
        (e isa HTTP.StatusError && e.status == 404) || rethrow()
        tag = completedetag(url, eTags; kw...)
        tag === nothing && rethrow()
        return tag
    end
end

# The ETag S3 gives an object completed from parts with these ETags: the MD5 of the
# parts' binary MD5s, "-", and the part count. `nothing` if a part ETag is not an MD5.
function multipartetag(eTags)
    digests = UInt8[]
    for tag in eTags
        hex = strip(tag, '"')
        (length(hex) == 32 && all(isxdigit, hex)) || return nothing
        append!(digests, hex2bytes(hex))
    end
    return string(bytes2hex(md5(digests)), '-', length(eTags))
end

# The object's ETag if it is the one these parts complete to, else `nothing`.
function completedetag(url, eTags; credentials=nothing, kw...)
    expected = multipartetag(eTags)
    expected === nothing && return nothing
    resp = try
        AWS.head(url; credentials, service="s3", kw...)
    catch
        return nothing
    end
    tag = API.etag(HTTP.header(resp, "ETag", ""))
    return resp.status == 200 && tag == expected ? tag : nothing
end

# `attempts` bounds retries of an InternalError sent in an HTTP 200 body.
function completeupload(url, uploadId, body; headers=HTTP.Headers(), attempts=1, kw...)
    for attempt in 1:attempts
        resp = AWS.post(url, headers; query=Dict("uploadId" => uploadId), body, service="s3", kw...)
        API.multipartresponse(resp)
        # S3 can return an Error body after HTTP 200 and keepalive whitespace.
        result = xml_dict(lstrip(String(resp.body)))
        haskey(result, "Error") || return API.etag(result["CompleteMultipartUploadResult"]["ETag"])
        failure = result["Error"]
        if failure["Code"] != "InternalError" || attempt == attempts
            error("S3 CompleteMultipartUpload failed ($(failure["Code"])): $(failure["Message"])")
        end
        sleep(0.1 * attempt)
    end
end

const CHECKSUM_FIELDS = ("ChecksumCRC32", "ChecksumCRC32C", "ChecksumCRC64NVME",
    "ChecksumMD5", "ChecksumSHA1", "ChecksumSHA256", "ChecksumSHA512",
    "ChecksumXXHASH128", "ChecksumXXHASH3", "ChecksumXXHASH64")
const ListedPart = NamedTuple{(:number, :etag, :size, :checksums, :last_modified),
    Tuple{Int,String,Int64,Dict{String,String},String}}

function partnumber(number::Integer)
    1 <= number <= 10000 || throw(ArgumentError("S3 part number must be between 1 and 10000"))
    return Int(number)
end

function uploadid(id::AbstractString)
    isempty(id) && throw(ArgumentError("S3 upload ID must not be empty"))
    return String(id)
end

function checksumfields(fields)
    result = Dict{String,String}()
    for (name, value) in fields
        name in CHECKSUM_FIELDS || throw(ArgumentError("unsupported S3 part checksum: $name"))
        value isa AbstractString && !isempty(value) ||
            throw(ArgumentError("S3 part checksum must be a nonempty string"))
        result[name] = String(value)
    end
    return result
end

function checksumheaders(resp)
    fields = Dict{String,String}()
    for (name, value) in resp.headers
        name = lowercase(name)
        startswith(name, "x-amz-checksum-") || continue
        name == "x-amz-checksum-type" && continue
        index = findfirst(field -> "x-amz-checksum-" * lowercase(field[9:end]) == name, CHECKSUM_FIELDS)
        index === nothing && throw(ArgumentError("unsupported S3 part checksum header: $name"))
        fields[CHECKSUM_FIELDS[index]] = value
    end
    return checksumfields(fields)
end

"""
    S3.createmultipartupload(bucket, key; credentials=nothing, headers=nothing, contentType=nothing, kwargs...) -> String

Start a caller-owned multipart upload and return its opaque upload ID. No automatic
cleanup is registered. Save the ID and successful [`S3.uploadpart`](@ref) receipts
to resume later, or call [`S3.abortmultipartupload`](@ref) to discard the upload.
Creation disables HTTP retries and redirects: a lost response can leave an upload
whose ID is unknown. `headers` is copied and sets object metadata at creation.
"""
function createmultipartupload(x::Bucket, key::AbstractString;
    credentials=nothing, headers=nothing, contentType=nothing, kw...)
    API.multipartkwargs(kw; final=true)
    return uploadid(startupload(x, key; credentials, headers=API.transferheaders(headers),
        contentType, retry=false, redirect=false, kw...))
end

"""
    S3.uploadpart(bucket, key, upload_id, number, bytes; credentials=nothing, headers=nothing, kwargs...)

Upload bytes under a part number in `1:10000`, replacing that number if it exists.
Return an owned `(number, etag, size, checksums)` receipt. The quoted ETag and
optional checksum values are opaque; persist them for completion. `bytes` is an
`AbstractVector{UInt8}` borrowed until this synchronous call returns. Headers are
copied. A failure does not abort the upload.
"""
function uploadpart(x::Bucket, key::AbstractString, id::AbstractString, number::Integer,
    bytes::AbstractVector{UInt8}; credentials=nothing, headers=nothing, kw...)
    API.multipartkwargs(kw)
    number = partnumber(number)
    resp = API.multipartresponse(partrequest(API.makeURL(x, key), API.uploadbytes(bytes),
        number, uploadid(id); credentials, headers=API.transferheaders(headers), kw...))
    etag = String(HTTP.header(resp, "ETag"))
    isempty(etag) && error("S3 UploadPart returned no ETag")
    return (number=number, etag=etag, size=Int64(length(bytes)), checksums=checksumheaders(resp))
end

"""
    S3.listparts(bucket, key, upload_id; credentials=nothing, headers=nothing, maxparts=1000, kwargs...)

Inspect all uploaded parts, following pages of at most `maxparts` (1–1000).
Return records with `number`, quoted `etag`, `size::Int64`, `checksums`, and
`last_modified`. This is an inventory, not a completion receipt ledger: preserve
successful upload receipts and verify caller-owned source identity before reuse.
Malformed or non-advancing pages throw instead of returning a partial inventory.
Concurrent uploads may change between pages; this is not an atomic snapshot.
"""
function listparts(x::Bucket, key::AbstractString, id::AbstractString;
    credentials=nothing, headers=nothing, maxparts::Integer=1000, kw...)
    API.multipartkwargs(kw)
    1 <= maxparts <= 1000 || throw(ArgumentError("maxparts must be between 1 and 1000"))
    id = uploadid(id)
    parts = ListedPart[]
    marker = 0
    while true
        resp = API.multipartresponse(AWS.get(API.makeURL(x, key), API.transferheaders(headers);
            credentials, service="s3", query=Dict("uploadId" => id,
                "max-parts" => string(maxparts), "part-number-marker" => string(marker)), kw...))
        page = xml_dict(String(resp.body))["ListPartsResult"]
        haskey(page, "UploadId") && page["UploadId"] != id && error("S3 ListParts returned a different upload ID")
        previous = marker
        for part in (haskey(page, "Part") ? API.asArray(page["Part"]) : ())
            number = partnumber(parse(Int, part["PartNumber"]))
            number > previous || error("S3 ListParts returned unordered or repeated part numbers")
            size = parse(Int64, part["Size"])
            size >= 0 || error("S3 ListParts returned a negative part size")
            etag = String(part["ETag"])
            isempty(etag) && error("S3 ListParts returned an empty ETag")
            checksums = checksumfields((name => value for (name, value) in part if startswith(string(name), "Checksum")))
            push!(parts, (number=number, etag=etag, size=size, checksums=checksums,
                last_modified=String(part["LastModified"])))
            previous = number
        end
        truncated = page["IsTruncated"]
        truncated == "false" && return parts
        truncated == "true" || error("S3 ListParts returned an invalid IsTruncated value")
        next = parse(Int, page["NextPartNumberMarker"])
        marker < next <= 10000 && next >= previous || error("S3 ListParts returned a non-advancing marker")
        marker = next
    end
end

"""
    S3.completemultipartupload(bucket, key, upload_id, receipts; credentials=nothing, headers=nothing, kwargs...) -> AbstractString

Publish acknowledged part receipts in strictly increasing `number` order, without
renumbering them. Each receipt needs `number` and `etag`, and may have a `checksums`
dictionary returned by [`S3.uploadpart`](@ref). Return the final ETag without quotes.
Use completion `headers` for conditions such as `If-None-Match`; object metadata
belongs on creation. HTTP retries and redirects are disabled. After a lost response,
reconcile the destination before another commit. Errors never trigger automatic abort.
"""
function completemultipartupload(x::Bucket, key::AbstractString, id::AbstractString, receipts;
    credentials=nothing, headers=nothing, kw...)
    API.multipartkwargs(kw; final=true)
    id = uploadid(id)
    parts = Dict{String,String}[]
    previous = 0
    for receipt in receipts
        number = partnumber(receipt.number)
        number > previous || throw(ArgumentError("completion receipts must have strictly increasing part numbers"))
        etag = receipt.etag
        etag isa AbstractString && !isempty(etag) || throw(ArgumentError("completion ETags must be nonempty strings"))
        part = hasproperty(receipt, :checksums) ? checksumfields(receipt.checksums) : Dict{String,String}()
        part["PartNumber"] = string(number)
        part["ETag"] = String(etag)
        push!(parts, part)
        previous = number
    end
    isempty(parts) && throw(ArgumentError("completion requires at least one part receipt"))
    body = XMLDict.node_xml("CompleteMultipartUpload", Dict("Part" => parts))
    return completeupload(API.makeURL(x, key), id, body; credentials,
        headers=API.transferheaders(headers), retry=false, redirect=false, kw...)
end

"""
    S3.abortmultipartupload(bucket, key, upload_id; credentials=nothing, headers=nothing, kwargs...)

Discard a caller-owned S3 upload. This does not delete a published object. Stop and
join in-flight part uploads before aborting; the provider may require another abort
if a part finishes concurrently. Provider errors (including a missing upload) propagate.
"""
function abortmultipartupload(x::Bucket, key::AbstractString, id::AbstractString;
    credentials=nothing, headers=nothing, kw...)
    API.multipartkwargs(kw)
    API.multipartresponse(AWS.delete(API.makeURL(x, key), API.transferheaders(headers);
        credentials, query=Dict("uploadId" => uploadid(id)), service="s3", kw...))
    return nothing
end

function API.abortMultipartUpload(x::Bucket, url, uploadId; kw...)
    return AWS.delete(url, HTTP.Headers();
        query=Dict("uploadId" => uploadId), service="s3", kw...)
end

delete(x::Bucket, key::API.Resource; kw...) = AWS.delete(API.makeURL(x, key); service="s3", kw...)
delete(x::Object; kw...) = delete(x.store, x.key; credentials=x.credentials, kw...)

for func in (:list, :get, :head, :exists, :put, :delete)
    @eval function $func(url::AbstractString, args...; region=nothing, nowarn::Bool=false, parseLocal::Bool=false, kw...)
        ok, accelerate, host, bucket, reg, key = parseAWSBucketRegionKey(url; parseLocal=parseLocal)
        ok || throw(ArgumentError("invalid url for S3.$($func): `$url`"))
        if region === nothing
            nowarn || @warn "`region` keyword argument not provided to `S3.$($func)` and undetected from url.  Defaulting to `us-east-1`"
            region = AWS.AWS_DEFAULT_REGION
        end
        if key !== nothing
            resource = API.parsedURLResource(key)
            return $func(S3.Bucket(bucket, region; accelerate, host), resource, args...; kw...)
        else
            return $func(S3.Bucket(bucket, region; accelerate, host), args...; kw...)
        end
    end
end

end # S3
