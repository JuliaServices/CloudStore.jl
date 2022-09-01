"""
CloudBase provides very simple HTTP clients for cloud services
(AWS, Azure, Google Cloud), which consists of 2 main components:
  * Computing credentials necessary for authenticated cloud requests
  * Signing requests with the credentials

CloudStore provides a very simple, consistent, and performant API for
object management with cloud services.
  * Create/list/delete buckets/containers
  * Put object
    * in single operation
    * from existing object (copy)
    * for large objects, split into parts and upload in parallel
    * from ObjectWithParts, replace parts with new data, and upload in parallel (using UploadPartCopy/Put Block List w/ committed parts)
  * Get Object
    * get object metadata
    * get object in single operation
    * get large objects in parallel, either by concurrent parts/block, or by concurrent range requests
  * list objects in bucket/container

* allow compressing objects auto when putting; decompressing auto when getting
* allow public get from s3:// azure:// urls

Object model:
  * Bucket/Container: returned from create, list, arg for object operations, can create manually
  * Object: returned from put, get, list objects. can be used for put/get. 

"""
module CloudStore

using HTTP, CodecZlib, Mmap
import CloudBase: AWS, Azure, AbstractStore
import WorkerUtilities: OrderedSynchronizer

const ResponseBodyType = Union{Nothing, String, IO}
const RequestBodyType = Union{AbstractVector{UInt8}, String, IO}

const MULTIPART_THRESHOLD = 64_000_000
const MULTIPART_SIZE = 16_000_000

defaultBatchSize() = 4 * Threads.nthreads()

struct Object
    store::AbstractStore
    key::String
    lastModified::String
    eTag::String
    size::Int
    storageClass::String
end

etag(x) = strip(x, '"')
object(b, x) = Object(b, x["Key"], x["LastModified"], etag(x["ETag"]), parse(Int, x["Size"]), x["StorageClass"])

include("get.jl")
include("put.jl")
include("s3.jl")
include("blob.jl")

end # module CloudStore
