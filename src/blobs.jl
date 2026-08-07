module Blobs

using CloudBase.Azure, XMLDict, HTTP, CodecZlib, Base64
using ..API
import ..parseAzureAccountContainerBlob

const Container = Azure.Container
const Credentials = Azure.Credentials

API.cloudName(::Container) = "Blob Storage"

function make_object(store, creds, body, add_properties=false)
    properties = Dict{String, Any}()
    if add_properties
        # body is a Dict{Any, Any}, convert keys to strings
        for (key,value) in body
            properties[key] = value
        end
    end

    return Object(store, creds, body["Name"],
                  parse(Int, body["Properties"]["Content-Length"]),
                  API.etag(body["Properties"]["Etag"]), properties)
end

API.maxListKeys(::Container) = 5000
API.listMaxKeysQuery(::Container) = "maxresults"
API.continuationToken(::Container) = "marker"

function API.listObjects(x::Container, query, result=nothing; credentials=nothing,
                                              get_properties=false, kw...)
    query["restype"] = "container"
    query["comp"] = "list"
    result = xml_dict(String(Azure.get(x.baseurl; query, credentials, kw...).body))["EnumerationResults"]
    if isempty(result["Blobs"])
        return (Object[], "")
    end
    contents = map(y -> make_object(x, credentials, y, get_properties), API.asArray(result["Blobs"]["Blob"]))
    return (contents, result["NextMarker"])
end

list(x::Container; kw...) = API.listObjectsImpl(x; kw...)

API.getObject(x::Container, url, headers; kw...) = Azure.get(url, headers; kw...)

get(x::Object, args...; kw...) = get(x.store, x.key, args...; credentials=x.credentials, kw...)
get(args...; kw...) = API.getObjectImpl(args...; kw...)

API.headObject(x::Container, url, headers; kw...) = Azure.head(url; headers, kw...)
head(x::Object; kw...) = head(x.store, x.key; credentials=x.credentials, kw...)
head(x::Container, key::API.Resource; kw...) = API.headObjectImpl(x, key; kw...)
exists(x::Object; kw...) = exists(x.store, x.key; credentials=x.credentials, kw...)
exists(x::Container, key::API.Resource; kw...) = API.existsObjectImpl(x, key; kw...)

put(args...; kw...) = API.putObjectImpl(args...; kw...)
put(x::Object; kw...) = put(x.store, x.key; credentials=x.credentials, kw...)

function API.putObject(x::Container, key, body;
    contentType=nothing, headers=HTTP.Headers(), kw...)
    HTTP.setheader(headers, "x-ms-blob-type" => "BlockBlob")
    contentType === nothing || HTTP.setheader(headers, "Content-Type" => String(contentType))
    return Azure.put(API.makeURL(x, key), headers, body; kw...)
end

API.startMultipartUpload(x::Container, key;
    contentType=nothing, headers=HTTP.Headers(), kw...) = nothing

function API.uploadPart(x::Container, url, part, partNumber, uploadId; kw...)
    blockid = base64encode(lpad(partNumber - 1, 64, '0'))
    Azure.put(url, [], part;
        query=Dict("comp" => "block", "blockid" => blockid), kw...)
    return (blockid, length(part))
end

function API.completeMultipartUpload(x::Container, url, eTags, uploadId;
    contentType=nothing, headers=HTTP.Headers(), kw...)
    contentType === nothing || HTTP.setheader(
        headers, "x-ms-blob-content-type" => String(contentType))
    body = XMLDict.node_xml("BlockList", Dict("Latest" => eTags))
    resp = Azure.put(url, headers, body; query=Dict("comp" => "blocklist"), kw...)
    return API.etag(HTTP.header(resp, "ETag"))
end

# Azure Blob Storage has no abort operation for uncommitted blocks. The service
# removes them automatically after its retention period.
API.abortMultipartUpload(x::Container, url, uploadId; kw...) = nothing

delete(x::Container, key::API.Resource; kw...) = Azure.delete(API.makeURL(x, key); kw...)
delete(x::Object; kw...) = delete(x.store, x.key; credentials=x.credentials, kw...)

for func in (:list, :get, :head, :exists, :put, :delete)
    @eval function $func(url::AbstractString, args...; parseLocal::Bool=false, kw...)
        ok, host, account, container, blob = parseAzureAccountContainerBlob(url; parseLocal=parseLocal)
        ok || throw(ArgumentError("invalid url for Blobs.$($func): `$url`"))
        if blob !== nothing
            resource = API.parsedURLResource(blob)
            return $func(Azure.Container(container, account; host), resource, args...; kw...)
        else
            return $func(Azure.Container(container, account; host), args...; kw...)
        end
    end
end

end # module Blobs
