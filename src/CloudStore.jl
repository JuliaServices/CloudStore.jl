module CloudStore

using HTTP, CodecZlib, Mmap
import CloudBase: AWS, Azure, AbstractStore
import WorkerUtilities: OrderedSynchronizer

const ResponseBodyType = Union{Nothing, String, IO}
const RequestBodyType = Union{AbstractVector{UInt8}, String, IO}

const MULTIPART_THRESHOLD = 64_000_000
const MULTIPART_SIZE = 16_000_000

defaultBatchSize() = 4 * Threads.nthreads()

asArray(x::Array) = x
asArray(x) = [x]

struct Object
    store::AbstractStore
    key::String
    lastModified::String
    eTag::String
    size::Int
    storageClass::String
end

etag(x) = strip(x, '"')

include("get.jl")
include("put.jl")
include("s3.jl")
include("blob.jl")

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

list(x::Azure.Container; kw...) = Azure.list(x; kw...)
get(x::Azure.Container, key::String, out::ResponseBodyType=nothing; kw...) = Azure.get(x, key, out; kw...)
head(x::Azure.Container, key::String; kw...) = Azure.head(x, key; kw...)
put(x::Azure.Container, key::String, in::RequestBodyType; kw...) = Azure.put(x, key, in; kw...)
delete(x::Azure.Container, key::String; kw...) = Azure.delete(x, key; kw...)

# try to parse cloud-specific url schemes and dispatch
function parseAzureAccountContainerBlob(url)
    # https://myaccount.blob.core.windows.net/mycontainer/myblob
    m = match(r"^https://(?<account>[^\.]+)\.blob\.core\.windows\.net/(?<container>[^/]+)/(?<blob>.+)$", url)
    m !== nothing && return (true, String(m[:account]), String(m[:container]), String(m[:blob]))
    # https://myaccount.blob.core.windows.net/mycontainer
    m = match(r"^https://(?<account>[^\.]+)\.blob\.core\.windows\.net/(?<container>[^/]+)$", url)
    m !== nothing && return (true, String(m[:account]), String(m[:container]), "")
    return (false, "", "", "")
end

function parseAWSBucketRegionKey(url)
    # https://bucket-name.s3.region-code.amazonaws.com/key-name
    m = match(r"^https://(?<bucket>[^\.]+)\.s3\.(?<region>[^\.]+)\.amazonaws\.com/(?<key>.+)$", url)
    m !== nothing && return (true, String(m[:bucket]), String(m[:region]), String(m[:key]))
    # https://bucket-name.s3.region-code.amazonaws.com
    m = match(r"^https://(?<bucket>[^\.]+)\.s3\.(?<region>[^\.]+)\.amazonaws\.com$", url)
    m !== nothing && return (true, String(m[:bucket]), String(m[:region]), "")
    # https://bucket-name.s3.amazonaws.com/key-name
    m = match(r"^https://(?<bucket>[^\.]+)\.s3\.amazonaws\.com/(?<key>.+)$", url)
    m !== nothing && return (true, String(m[:bucket]), "", String(m[:key]))
    # https://bucket-name.s3.amazonaws.com
    m = match(r"^https://(?<bucket>[^\.]+)\.s3\.amazonaws\.com$", url)
    m !== nothing && return (true, String(m[:bucket]), "", "")
    # https://s3.region-code.amazonaws.com/bucket-name/key-name
    m = match(r"^https://s3\.(?<region>[^\.]+)\.amazonaws\.com/(?<bucket>[^/]+)/(?<key>.+)$", url)
    m !== nothing && return (true, String(m[:bucket]), String(m[:region]), String(m[:key]))
    # https://s3.region-code.amazonaws.com/bucket-name
    m = match(r"^https://s3\.(?<region>[^\.]+)\.amazonaws\.com/(?<bucket>[^/]+)$", url)
    m !== nothing && return (true, String(m[:bucket]), String(m[:region]), "")
    # S3://bucket-name/key-name
    m = match(r"^s3://(?<bucket>[^/]+)/(?<key>.+)$", url)
    m !== nothing && return (true, String(m[:bucket]), "", String(m[:key]))
    # S3://bucket-name
    m = match(r"^s3://(?<bucket>[^/]+)$", url)
    m !== nothing && return (true, String(m[:bucket]), "", "")
    return (false, "", "", "")
end

function parseGCPBucketObject(url)
    # https://storage.googleapis.com/BUCKET_NAME/OBJECT_NAME
    m = match(r"^https://storage\.googleapis\.com/(?<bucket>[^/]+)/(?<object>.+)$", url)
    m !== nothing && return (true, String(m[:bucket]), String(m[:object]))
    # https://storage.googleapis.com/BUCKET_NAME
    m = match(r"^https://storage\.googleapis\.com/(?<bucket>[^/]+)$", url)
    m !== nothing && return (true, String(m[:bucket]), "")
    # https://BUCKET_NAME.storage.googleapis.com/OBJECT_NAME
    m = match(r"^https://(?<bucket>[^\.]+)\.storage\.googleapis\.com/(?<object>.+)$", url)
    m !== nothing && return (true, String(m[:bucket]), String(m[:object]))
    # https://BUCKET_NAME.storage.googleapis.com
    m = match(r"^https://(?<bucket>[^\.]+)\.storage\.googleapis\.com$", url)
    m !== nothing && return (true, String(m[:bucket]), "")
    # https://storage.googleapis.com/download/storage/v1/b/BUCKET_NAME/o/OBJECT_NAME?alt=media
    m = match(r"^https://storage\.googleapis\.com/download/storage/v1/b/(?<bucket>[^/]+)/o/(?<object>.+)\?alt=media$", url)
    m !== nothing && return (true, String(m[:bucket]), String(m[:object]))
    # https://storage.googleapis.com/download/storage/v1/b/BUCKET_NAME
    m = match(r"^https://storage\.googleapis\.com/download/storage/v1/b/(?<bucket>[^/]+)$", url)
    m !== nothing && return (true, String(m[:bucket]), "")
    return (false, "", "")
end

function parseURLForDispatch(url, region, nowarn)
    # try to parse cloud-specific url schemes and dispatch
    ok, bucket, reg, key = parseAWSBucketRegionKey(url)
    region = isempty(reg) ? region : reg
    if region === nothing
        nowarn || @warn "`region` keyword argument not provided to `CloudStore.get` and undetected from url.  Defaulting to `us-east-1`"
        region = AWS.AWS_DEFAULT_REGION
    end
    ok && return (AWS.Bucket(bucket, region), key)
    ok, account, container, blob = parseAzureAccountContainerBlob(url)
    ok && return (Azure.Container(container, account), blob)
    # ok, bucket, object = parseGCPBucketObject(url)
    # ok && return (GCP.Bucket(bucket), object)
    error("couldn't determine cloud from string url: `$url`")
end

function get(url::String, out::ResponseBodyType=nothing; region=nothing, nowarn::Bool=false, kw...)
    store, key = parseURLForDispatch(url, region, nowarn)
    return get(store, key, out; kw...)
end

function head(url::String; region=nothing, nowarn::Bool=false, kw...)
    store, key = parseURLForDispatch(url, region, nowarn)
    return head(store, key; kw...)
end

function put(url::String, in::RequestBodyType; region=nothing, nowarn::Bool=false, kw...)
    store, key = parseURLForDispatch(url, region, nowarn)
    return put(store, key, in; kw...)
end

function delete(url::String; region=nothing, nowarn::Bool=false, kw...)
    store, key = parseURLForDispatch(url, region, nowarn)
    return delete(store, key; kw...)
end

function list(url::String; region=nothing, nowarn::Bool=false, kw...)
    store, _ = parseURLForDispatch(url, region, nowarn)
    return list(store; kw...)
end

end # module CloudStore
