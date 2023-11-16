module CloudStore

import CloudBase: AWS, Azure, CloudTest

# convenience module that holds consts, utils, and functions to overload
# for specific clouds
module API

export Object, PrefetchedDownloadStream, ResponseBodyType, RequestBodyType,
    MultipartUploadStream

using HTTP, CodecZlib, CodecZlibNG, Mmap
import WorkerUtilities: OrderedSynchronizer
import CloudBase: AbstractStore

"""
Controls the automatic use of concurrency when downloading/uploading.
  * Downloading: the size of the initial content range requested; if
"""
const MULTIPART_THRESHOLD = 2^23 # 8MB
const MULTIPART_SIZE = 2^23

defaultBatchSize() = 4 * Threads.nthreads()

const ResponseBodyType = Union{Nothing, AbstractVector{UInt8}, String, IO}
const RequestBodyType = Union{AbstractVector{UInt8}, String, IO}

asArray(x::Array) = x
asArray(x) = [x]

etag(x) = strip(x, '"')
makeURL(x::AbstractStore, key) = joinpath(x.baseurl, lstrip(key, '/'))

include("object.jl")

function cloudName end
function maxListKeys end
function listMaxKeysQuery end
function continuationToken end
function listObjects end
function getObject end
function headObject end
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
put(x::Object, in::RequestBodyType; kw...) = put(x.store, x.key, in; kw...)
delete(x::Object; kw...) = delete(x.store, x.key; kw...)

# generic methods that dispatch on store type
list(x::AWS.Bucket; kw...) = S3.list(x; kw...)
get(x::AWS.Bucket, key::String, out::ResponseBodyType=nothing; kw...) = S3.get(x, key, out; kw...)
head(x::AWS.Bucket, key::String; kw...) = S3.head(x, key; kw...)
put(x::AWS.Bucket, key::String, in::RequestBodyType; kw...) = S3.put(x, key, in; kw...)
delete(x::AWS.Bucket, key::String; kw...) = S3.delete(x, key; kw...)

list(x::Azure.Container; kw...) = Blobs.list(x; kw...)
get(x::Azure.Container, key::String, out::ResponseBodyType=nothing; kw...) = Blobs.get(x, key, out; kw...)
head(x::Azure.Container, key::String; kw...) = Blobs.head(x, key; kw...)
put(x::Azure.Container, key::String, in::RequestBodyType; kw...) = Blobs.put(x, key, in; kw...)
delete(x::Azure.Container, key::String; kw...) = Blobs.delete(x, key; kw...)

function get(url::AbstractString, out::ResponseBodyType=nothing; region=nothing, nowarn::Bool=false, kw...)
    store, key = parseURLForDispatch(url, region, nowarn)
    return get(store, key, out; kw...)
end

function head(url::AbstractString; region=nothing, nowarn::Bool=false, kw...)
    store, key = parseURLForDispatch(url, region, nowarn)
    return head(store, key; kw...)
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
