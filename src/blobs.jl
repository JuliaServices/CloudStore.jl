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
    blockrequest(url, blockid, part; kw...)
    return (blockid, length(part))
end

function blockrequest(url, blockid, part; headers=HTTP.Headers(), kw...)
    return Azure.put(url, headers, part; query=Dict("comp" => "block", "blockid" => blockid), kw...)
end

function API.completeMultipartUpload(x::Container, url, eTags, uploadId;
    contentType=nothing, headers=HTTP.Headers(), kw...)
    contentType === nothing || HTTP.setheader(
        headers, "x-ms-blob-content-type" => String(contentType))
    body = XMLDict.node_xml("BlockList", Dict("Latest" => eTags))
    return commitblocklist(url, body; headers, kw...)
end

function commitblocklist(url, body; headers=HTTP.Headers(), kw...)
    resp = Azure.put(url, headers, body; query=Dict("comp" => "blocklist"), kw...)
    API.multipartresponse(resp)
    return API.etag(HTTP.header(resp, "ETag"))
end

function blockid(id::AbstractString)
    decoded = base64decode(id)
    1 <= length(decoded) <= 64 && base64encode(decoded) == id ||
        throw(ArgumentError("Azure block ID must be canonical Base64 of 1 to 64 bytes"))
    return String(id), length(decoded)
end

const BlockInfo = NamedTuple{(:id, :size),Tuple{String,Int64}}

function blockentries(result, name)
    blocks = BlockInfo[]
    isempty(result) && return blocks
    group = Base.get(result, name, nothing)
    (group === nothing || isempty(group)) && return blocks
    for block in API.asArray(group["Block"])
        id, _ = blockid(block["Name"])
        size = parse(Int64, block["Size"])
        size >= 0 || error("Azure Get Block List returned a negative block size")
        push!(blocks, (id=id, size=size))
    end
    return blocks
end

"""
    Blobs.stageblock(container, key, block_id, bytes; credentials=nothing, headers=nothing, kwargs...)

Stage bytes under a caller-owned, opaque Base64 block ID and return `(id, size)`.
IDs must encode 1–64 bytes, with the same decoded length for all blocks of one blob.
Reusing an ID replaces its uncommitted bytes. Use a unique fixed-length namespace
per upload and save IDs, receipts, and source identity before resuming. `bytes` is
an `AbstractVector{UInt8}` borrowed until this synchronous call returns; headers
are copied. Azure has no abort operation for uncommitted blocks.
"""
function stageblock(x::Container, key::AbstractString, id::AbstractString,
    bytes::AbstractVector{UInt8}; credentials=nothing, headers=nothing, kw...)
    API.multipartkwargs(kw)
    id, _ = blockid(id)
    API.multipartresponse(blockrequest(API.makeURL(x, key), id, API.uploadbytes(bytes);
        credentials, headers=API.transferheaders(headers), kw...))
    return (id=id, size=Int64(length(bytes)))
end

"""
    Blobs.listblocks(container, key; state=:all, credentials=nothing, headers=nothing, kwargs...)

Inspect `:committed`, `:uncommitted`, or `:all` blocks. Return separate `committed`
and `uncommitted` vectors of `(id, size::Int64)`, and `etag` (quoted, or `nothing`
when absent). Committed order is preserved. A block ID can appear in both lists
with different data. Listings contain no per-block content checksum and do not
prove source identity or provide an atomic snapshot of concurrent staging.
"""
function listblocks(x::Container, key::AbstractString; state::Symbol=:all,
    credentials=nothing, headers=nothing, kw...)
    API.multipartkwargs(kw)
    state in (:all, :committed, :uncommitted) || throw(ArgumentError("invalid Azure block-list state: $state"))
    resp = API.multipartresponse(Azure.get(API.makeURL(x, key), API.transferheaders(headers);
        credentials, query=Dict("comp" => "blocklist", "blocklisttype" => string(state)), kw...))
    result = xml_dict(String(resp.body))["BlockList"]
    etag = String(HTTP.header(resp, "ETag"))
    return (committed=blockentries(result, "CommittedBlocks"),
        uncommitted=blockentries(result, "UncommittedBlocks"), etag=isempty(etag) ? nothing : etag)
end

"""
    Blobs.commitblocks(container, key, blocks; credentials=nothing, headers=nothing, contentType=nothing, kwargs...) -> AbstractString

Publish an ordered collection of `(id, state)` entries, where `state` is
`:committed`, `:uncommitted`, or `:latest`. Order and repeated IDs are preserved;
repeated IDs must use the same state. `:latest` prefers an uncommitted block and
falls back to a committed one. Use `:uncommitted` when that fallback is unwanted.
An empty collection publishes an empty blob. Return the final ETag without quotes.

Headers are copied and may set destination conditions or blob metadata. Commit
replaces metadata unless supplied again. HTTP retries and redirects are disabled:
after a lost response, reconcile the destination before committing again. Failures
do not delete the destination or staged blocks.
"""
function commitblocks(x::Container, key::AbstractString, blocks;
    credentials=nothing, headers=nothing, contentType=nothing, kw...)
    API.multipartkwargs(kw; final=true)
    nodes = String[]
    states = Dict{String,Symbol}()
    width = 0
    for block in blocks
        id, size = blockid(block.id)
        width == 0 || size == width || throw(ArgumentError("Azure block IDs must have the same decoded length"))
        width = size
        state = block.state
        state in (:committed, :uncommitted, :latest) || throw(ArgumentError("invalid Azure commit state: $state"))
        haskey(states, id) && states[id] != state && throw(ArgumentError("repeated Azure block IDs must use the same state"))
        states[id] = state
        tag = state == :committed ? "Committed" : state == :uncommitted ? "Uncommitted" : "Latest"
        push!(nodes, XMLDict.node_xml(tag, id))
        length(nodes) <= 50000 || throw(ArgumentError("Azure block lists cannot exceed 50000 entries"))
    end
    # A dictionary grouped by selector would reorder mixed committed/uncommitted entries.
    body = "<BlockList>" * join(nodes) * "</BlockList>"
    headers = API.transferheaders(headers)
    contentType === nothing || HTTP.setheader(headers, "x-ms-blob-content-type" => String(contentType))
    return commitblocklist(API.makeURL(x, key), body; credentials, headers, retry=false, redirect=false, kw...)
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
