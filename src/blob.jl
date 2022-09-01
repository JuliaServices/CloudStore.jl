module Blobs

using CloudBase.Azure, XMLDict, HTTP, CodecZlib, Base64
import ..ResponseBodyType, ..RequestBodyType, ..getObject, ..putObject, ..Object, ..etag, ..object

const Container = Azure.Container

get(x::Object, out::ResponseBodyType=nothing; kw...) = get(x.store, x.key, out; kw...)
function get(x::Container, key::String, out::ResponseBodyType=nothing; kw...)
    return getObject(Azure, joinpath(x.baseurl, key), out, "blob"; kw...)
end

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

end # module Blobs
