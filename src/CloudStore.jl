module CloudStore

import CloudBase: AWS, Azure, CloudTest

# convenience module that holds consts, utils, and functions to overload
# for specific clouds
module API

export Object, PrefetchedDownloadStream, ResponseBodyType, RequestBodyType

using HTTP, CodecZlib, Mmap
import WorkerUtilities: OrderedSynchronizer
import CloudBase: AbstractStore

"""
Controls the automatic use of concurrency when downloading/uploading.
  * Downloading: the size of the initial content range requested; if
"""
const MULTIPART_THRESHOLD = 2^23 # 8MB
const MULTIPART_SIZE = 2^23

defaultBatchSize() = 4 * Threads.nthreads()

const ResponseBodyType = Union{Nothing, String, IO}
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

# try to parse cloud-specific url schemes and dispatch
function parseAzureAccountContainerBlob(url; parseLocal::Bool=false)
    url = String(url)
    # https://myaccount.blob.core.windows.net/mycontainer/myblob
    # https://myaccount.blob.core.windows.net/mycontainer
    m = match(r"^(https|azure)://(?<account>[^\.]+?)(\.blob\.core\.windows\.net)?/(?<container>[^/]+?)(?:/(?<blob>.+))?$", url)
    m !== nothing && return (true, nothing, String(m[:account]), String(m[:container]), String(something(m[:blob], "")))
    if parseLocal
        # "https://127.0.0.1:45942/devstoreaccount1/jl-azurite-21807/"
        m = match(r"^(?<host>(https|azure)://[\d|\.|:]+?)/(?<account>[^/]+?)/(?<container>[^/]+?)(?:/(?<blob>.+))?$", url)
        m !== nothing && return (true, replace(String(m[:host]), "azure" => "https"; count=1), String(m[:account]), String(m[:container]), String(something(m[:blob], "")))
    end
    # azure://myaccount/mycontainer/myblob
    # azure://myaccount/mycontainer
    # azure://myaccount
    m = match(r"^azure://(?<account>[^/]+)(?:/(?<container>.+))?(?:/(?<blob>.+))?$"i, url)
    m !== nothing && return (true, nothing, String(m[:account]), String(something(m[:container], "")), String(something(m[:blob], "")))
    return (false, nothing, "", "", "")
end

function parseAWSBucketRegionKey(url; parseLocal::Bool=false)
    url = String(url)
    # https://bucket-name.s3-accelerate.region-code.amazonaws.com/key-name
    # https://bucket-name.s3-accelerate.region-code.amazonaws.com
    # https://bucket-name.s3-accelerate.amazonaws.com/key-name
    # https://bucket-name.s3-accelerate.amazonaws.com
    # https://bucket-name.s3.region-code.amazonaws.com/key-name
    # https://bucket-name.s3.region-code.amazonaws.com
    # https://bucket-name.s3.amazonaws.com/key-name
    # https://bucket-name.s3.amazonaws.com
    m = match(r"^https://(?<bucket>[^\.]+)\.s3(?<accelerate>-accelerate)?(?:\.(?<region>[^\.]+))?\.amazonaws\.com(?:/(?<key>.+))?$", url)
    m !== nothing && return (true, !isnothing(m[:accelerate]), nothing, String(m[:bucket]), String(something(m[:region], "")), String(something(m[:key], "")))
    # https://s3.region-code.amazonaws.com/bucket-name/key-name
    # https://s3.region-code.amazonaws.com/bucket-name
    m = match(r"^https://s3(?:\.(?<region>[^\.]+))?\.amazonaws\.com/(?<bucket>[^/]+)(?:/(?<key>.+))?$", url)
    m !== nothing && return (true, false, nothing, String(m[:bucket]), String(something(m[:region], "")), String(something(m[:key], "")))
    if parseLocal
        # "http://127.0.0.1:27181/jl-minio-4483/"
        m = match(r"^(?<host>(http|s3)://[\d|\.|:]+?)/(?<bucket>[^/]+?)(?:/(?<key>.+))?$", url)
        m !== nothing && return (true, false, replace(String(m[:host]), "s3" => "http"; count=1), String(m[:bucket]), "", String(something(m[:key], "")))
    end
    # S3://bucket-name/key-name
    # S3://bucket-name
    m = match(r"^s3://(?<bucket>[^/]+)(?:/(?<key>.+))?$"i, url)
    m !== nothing && return (true, false, nothing, String(m[:bucket]), "", String(something(m[:key], "")))
    return (false, false, nothing, "", "", "")
end

function parseGCPBucketObject(url)
    url = String(url)
    # https://storage.googleapis.com/BUCKET_NAME/OBJECT_NAME
    # https://storage.googleapis.com/BUCKET_NAME
    m = match(r"^https://storage\.googleapis\.com/(?<bucket>[^/]+)(?:/(?<key>.+))?$", url)
    m !== nothing && return (true, String(m[:bucket]), String(something(m[:object], "")))
    # https://BUCKET_NAME.storage.googleapis.com/OBJECT_NAME
    # https://BUCKET_NAME.storage.googleapis.com
    m = match(r"^https://(?<bucket>[^\.]+)\.storage\.googleapis\.com(?:/(?<key>.+))?$", url)
    m !== nothing && return (true, String(m[:bucket]), String(something(m[:object], "")))
    # https://storage.googleapis.com/download/storage/v1/b/BUCKET_NAME/o/OBJECT_NAME?alt=media
    # https://storage.googleapis.com/download/storage/v1/b/BUCKET_NAME
    m = match(r"^https://storage\.googleapis\.com/download/storage/v1/b/(?<bucket>[^/]+)(?:/o/(?<key>.+?))?(\?alt=media)?$", url)
    m !== nothing && return (true, String(m[:bucket]), String(something(m[:object], "")))
    return (false, "", "")
end

function parseURLForDispatch(url, region, nowarn)
    # try to parse cloud-specific url schemes and dispatch
    ok, accelerate, host, bucket, reg, key = parseAWSBucketRegionKey(url)
    region = isempty(reg) ? region : reg
    if ok && region === nothing
        nowarn || @warn "`region` keyword argument not provided to `CloudStore.get` and undetected from url.  Defaulting to `us-east-1`"
        region = AWS.AWS_DEFAULT_REGION
    end
    ok && return (AWS.Bucket(bucket, region; accelerate, host), key)
    ok, host, account, container, blob = parseAzureAccountContainerBlob(url)
    ok && return (Azure.Container(container, account; host), blob)
    # ok, bucket, object = parseGCPBucketObject(url)
    # ok && return (GCP.Bucket(bucket), object)
    error("couldn't determine cloud from string url: `$url`")
end

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
