module GCS

using CloudBase.GCP, HTTP
using ..API

const Bucket = GCP.Bucket
const Credentials = GCP.Credentials

API.cloudName(::Bucket) = "GCS"

API.getObject(x::Bucket, url, headers; kw...) = GCP.get(url, headers; kw...)
get(args...; kw...) = API.getObjectImpl(args...; kw...)

API.headObject(x::Bucket, url, headers; kw...) = GCP.head(url; headers, kw...)
head(x::Bucket, key::String; kw...) = API.headObjectImpl(x, key; kw...)

put(args...; kw...) = API.putObjectImpl(args...; kw...)
API.putObject(x::Bucket, key, body; kw...) = GCP.put(API.makeURL(x, key), [], body; kw...)

delete(x::Bucket, key; kw...) = GCP.delete(API.makeURL(x, key); kw...)

end # module GCS
