module Blobs

using CloudBase.Azure, XMLDict, HTTP, CodecZlib, Base64
using ..API
import ..parseAzureAccountContainerBlob

const Container = Azure.Container
const Credentials = Azure.Credentials

API.cloudName(::Container) = "Blob Storage"

object(b, creds, x) = Object(b, creds, x["Name"], parse(Int, x["Properties"]["Content-Length"]), API.etag(x["Properties"]["Etag"]))

API.maxListKeys(::Container) = 5000
API.listMaxKeysQuery(::Container) = "maxresults"
API.continuationToken(::Container) = "marker"

function API.listObjects(x::Container, query, result=nothing; credentials=nothing, kw...)
    query["restype"] = "container"
    query["comp"] = "list"
    result = xml_dict(String(Azure.get(x.baseurl; query, credentials, kw...).body))["EnumerationResults"]
    if isempty(result["Blobs"])
        return (Object[], "")
    end
    contents = map(y -> object(x, credentials, y), API.asArray(result["Blobs"]["Blob"]))
    return (contents, result["NextMarker"])
end

list(x::Container; kw...) = API.listObjectsImpl(x; kw...)

API.getObject(x::Container, url, headers; kw...) = Azure.get(url, headers; kw...)

get(x::Object, args...; kw...) = get(x.store, x.key, args...; credentials=x.credentials, kw...)
get(args...; kw...) = API.getObjectImpl(args...; kw...)

API.headObject(x::Container, url, headers; kw...) = Azure.head(url; headers, kw...)
head(x::Object; kw...) = head(x.store, x.key; credentials=x.credentials, kw...)
head(x::Container, key::String; kw...) = API.headObjectImpl(x, key; kw...)

put(args...; kw...) = API.putObjectImpl(args...; kw...)
put(x::Object; kw...) = put(x.store, x.key; credentials=x.credentials, kw...)

API.putObject(x::Container, key, body; kw...) = Azure.put(API.makeURL(x, key), ["x-ms-blob-type" => "BlockBlob"], body; kw...)

API.startMultipartUpload(x::Container, key; kw...) = nothing

function API.uploadPart(x::Container, url, part, partNumber, uploadId; kw...)
    blockid = base64encode(lpad(partNumber - 1, 64, '0'))
    Azure.put(url, [], part; query=Dict("comp" => "block", "blockid" => blockid), kw...)
    return blockid
end

function API.completeMultipartUpload(x::Container, url, eTags, uploadId; kw...)
    body = XMLDict.node_xml("BlockList", Dict("Latest" => eTags))
    resp = Azure.put(url; query=Dict("comp" => "blocklist"), body, kw...)
    return API.etag(HTTP.header(resp, "ETag"))
end

delete(x::Container, key::String; kw...) = Azure.delete(API.makeURL(x, key); kw...)
delete(x::Object; kw...) = delete(x.store, x.key; credentials=x.credentials, kw...)

for func in (:list, :get, :head, :put, :delete)
    @eval function $func(url::AbstractString, args...; parseLocal::Bool=false, kw...)
        ok, host, account, container, blob = parseAzureAccountContainerBlob(url; parseLocal=parseLocal)
        ok || throw(ArgumentError("invalid url for Blobs.$($func): `$url`"))
        if blob !== nothing
            return $func(Azure.Container(container, account; host), blob, args...; kw...)
        else
            return $func(Azure.Container(container, account; host), args...; kw...)
        end
    end
end

end # module Blobs
