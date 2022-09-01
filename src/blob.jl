module Blobs

using CloudBase.Azure, XMLDict, HTTP, CodecZlib, Base64
import ..ResponseBodyType, ..RequestBodyType, ..getObject, ..putObject, ..Object, ..etag, ..asArray

const Container = Azure.Container

object(b, x) = Object(b, x["Name"], x["Properties"]["Last-Modified"], etag(x["Properties"]["Etag"]), parse(Int, x["Properties"]["Content-Length"]), "")

function list(x::Container; prefix="", maxKeys=5000, kw...)
    if maxKeys > 5000
        @warn "Azure only supports 5000 keys per request: `$maxKeys` requested"
        maxKeys = 5000
    end
    query = Dict("restype" => "container", "comp" => "list")
    if !isempty(prefix)
        query["prefix"] = prefix
    end
    if maxKeys != 5000
        query["maxresults"] = string(maxKeys)
    end
    result = xml_dict(String(Azure.get(x.baseurl; query, kw...).body))["EnumerationResults"]
    if isempty(result["Blobs"])
        return Object[]
    end
    contents = map(y -> object(x, y), asArray(result["Blobs"]["Blob"]))
    while !isempty(result["NextMarker"])
        query["marker"] = result["NextMarker"]
        result = xml_dict(String(Azure.get(x.baseurl; query, kw...).body))["EnumerationResults"]
        if !isempty(result["Blobs"])
            append!(contents, map(y -> object(x, y), asArray(result["Blobs"]["Blob"])))
        else
            break
        end
    end
    return contents
end

get(x::Object, out::ResponseBodyType=nothing; kw...) = get(x.store, x.key, out; kw...)
function get(x::Container, key::String, out::ResponseBodyType=nothing; kw...)
    return getObject(Azure, joinpath(x.baseurl, key), out, "blob"; kw...)
end

head(x::Object; kw...) = head(x.store, x.key; kw...)
head(x::Container, key::String; kw...) = Dict(Azure.get(joinpath(x.baseurl, key); query=Dict("comp" => "metadata"), kw...).headers)

put(x::Container, key::String, in::RequestBodyType; kw...) =
    putObject(Blobs, x, key, in; kw...)

putObjectSingle(x::Container, key, body; kw...) = Azure.put(joinpath(x.baseurl, key), ["x-ms-blob-type" => "BlockBlob"], body; kw...)

startMultipartUpload(x::Container, key; kw...) = nothing

function uploadPart(url, part, partNumber, uploadId; kw...)
    blockid = base64encode(lpad(partNumber - 1, 64, '0'))
    Azure.put(url, [], part; query=Dict("comp" => "block", "blockid" => blockid), kw...)
    return blockid
end

function completeMultipartUpload(url, eTags, uploadId; kw...)
    body = XMLDict.node_xml("BlockList", Dict("Latest" => eTags))
    resp = Azure.put(url; query=Dict("comp" => "blocklist"), body, kw...)
    return etag(HTTP.header(resp, "ETag"))
end

delete(x::Container, key::String; kw...) = Azure.delete(joinpath(x.baseurl, key); kw...)
delete(x::Object; kw...) = delete(x.store, x.key; kw...)

end # module Blobs
