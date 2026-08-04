module CloudStore

import CloudBase: AWS, Azure, CloudTest

# convenience module that holds consts, utils, and functions to overload
# for specific clouds
module API

export Object, PrefetchedDownloadStream, ResponseBodyType, RequestBodyType,
    MultipartUploadStream

using HTTP, CodecZlib, CodecZlibNG, Mmap, TranscodingStreams
import WorkerUtilities: OrderedSynchronizer
import CloudBase: AbstractStore
using ExceptionUnwrapping

"""
Controls the automatic use of concurrency when downloading/uploading.
  * Downloading: the size of the initial content range requested; if
"""
const MULTIPART_THRESHOLD = 2^23 # 8MB
const MULTIPART_SIZE = 2^23

defaultBatchSize() = 4 * Threads.nthreads()

const ResponseBodyType = Union{Nothing, AbstractVector{UInt8}, String, IO}
const RequestBodyType = Union{AbstractVector{UInt8}, String, IO}

struct ParsedURLResource
    path::String
    query::String
end

const Resource = Union{AbstractString,ParsedURLResource}

function parsedURLResource(resource::AbstractString)
    parts = split(String(resource), '?'; limit=2, keepempty=true)
    return length(parts) == 1 ? only(parts) : ParsedURLResource(parts...)
end

resourceKey(resource::AbstractString) = String(resource)
resourceKey(resource::ParsedURLResource) = resource.path

asArray(x::Array) = x
asArray(x) = [x]

etag(x) = strip(x, '"')

function makeURL(x::AbstractStore, key)
    parts = split(lstrip(key, '/'), '/'; keepempty=true)
    escaped = join(HTTP.escapeuri.(parts), '/')
    return joinpath(x.baseurl, escaped)
end

makeURL(x::AbstractStore, resource::ParsedURLResource) =
    string(makeURL(x, resource.path), '?', resource.query)

include("object.jl")

function cloudName end
function maxListKeys end
function listMaxKeysQuery end
function continuationToken end
function listObjects end
function getObject end
function headObject end
function existsObject end
include("get.jl")
function putObject end
function startMultipartUpload end
function uploadPart end
function completeMultipartUpload end
include("put.jl")

end # module API

using .API

include("parse.jl")

# generic dispatches
get(x::Object, out::ResponseBodyType=nothing; kw...) = get(x.store, x.key, out; kw...)
head(x::Object; kw...) = head(x.store, x.key; kw...)
exists(x::Object; kw...) = exists(x.store, x.key; kw...)
put(x::Object, in::RequestBodyType; kw...) = put(x.store, x.key, in; kw...)
delete(x::Object; kw...) = delete(x.store, x.key; kw...)

# generic methods that dispatch on store type
list(x::AWS.Bucket; kw...) = S3.list(x; kw...)
get(x::AWS.Bucket, key::API.Resource, out::ResponseBodyType=nothing; kw...) = S3.get(x, key, out; kw...)
head(x::AWS.Bucket, key::API.Resource; kw...) = S3.head(x, key; kw...)
exists(x::AWS.Bucket, key::API.Resource; kw...) = S3.exists(x, key; kw...)
put(x::AWS.Bucket, key::API.Resource, in::RequestBodyType; kw...) = S3.put(x, key, in; kw...)
delete(x::AWS.Bucket, key::API.Resource; kw...) = S3.delete(x, key; kw...)

list(x::Azure.Container; kw...) = Blobs.list(x; kw...)
get(x::Azure.Container, key::API.Resource, out::ResponseBodyType=nothing; kw...) = Blobs.get(x, key, out; kw...)
head(x::Azure.Container, key::API.Resource; kw...) = Blobs.head(x, key; kw...)
exists(x::Azure.Container, key::API.Resource; kw...) = Blobs.exists(x, key; kw...)
put(x::Azure.Container, key::API.Resource, in::RequestBodyType; kw...) = Blobs.put(x, key, in; kw...)
delete(x::Azure.Container, key::API.Resource; kw...) = Blobs.delete(x, key; kw...)

function get(url::AbstractString, out::ResponseBodyType=nothing; region=nothing, nowarn::Bool=false, kw...)
    store, key = parseURLForDispatch(url, region, nowarn)
    return get(store, key, out; kw...)
end

function head(url::AbstractString; region=nothing, nowarn::Bool=false, kw...)
    store, key = parseURLForDispatch(url, region, nowarn)
    return head(store, key; kw...)
end

"""
    CloudStore.exists(store, key; kwargs...) -> Bool
    CloudStore.exists(object; kwargs...) -> Bool
    CloudStore.exists(url; kwargs...) -> Bool

Return `true` when the object exists. Return `false` only when the provider returns
HTTP 404. Other HTTP errors are rethrown so that authentication and service failures
are not reported as missing objects.
"""
function exists(url::AbstractString; region=nothing, nowarn::Bool=false, kw...)
    store, key = parseURLForDispatch(url, region, nowarn)
    return exists(store, key; kw...)
end

function put(url::AbstractString, in::RequestBodyType; region=nothing, nowarn::Bool=false, kw...)
    store, key = parseURLForDispatch(url, region, nowarn)
    return put(store, key, in; kw...)
end

function delete(url::AbstractString; region=nothing, nowarn::Bool=false, kw...)
    store, key = parseURLForDispatch(url, region, nowarn)
    return delete(store, key; kw...)
end

function list(url::AbstractString; region=nothing, nowarn::Bool=false, kw...)
    store, _ = parseURLForDispatch(url, region, nowarn)
    return list(store; kw...)
end

# cloud-specific API implementations
include("s3.jl")
include("blobs.jl")
const BlobStorage = Blobs

end # module CloudStore
