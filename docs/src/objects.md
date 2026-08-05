# Object operations

CloudStore accepts three forms for most object operations:

- a store handle and object key;
- a [`CloudStore.Object`](@ref);
- an S3 or Azure object URL.

The examples below use the generic `CloudStore` interface. The `CloudStore.S3` and
`CloudStore.Blobs` namespaces provide matching methods.

## Upload

Upload bytes, text, an open `IO`, or a file path:

```julia
obj = CloudStore.put(bucket, "reports/today.csv", codeunits("a,b\n1,2\n"))
obj = CloudStore.put(bucket, "reports/today.csv", IOBuffer(data))
obj = CloudStore.put(bucket, "reports/today.csv", "/tmp/today.csv")
```

The return value is an [`Object`](@ref). It contains the store, key, byte size, and ETag.

CloudStore starts a multipart upload above `multipartThreshold`. Control the transfer with
`partSize` and `batchSize`:

```julia
obj = CloudStore.put(
    bucket,
    "large.bin",
    data;
    multipartThreshold=8 * 1024^2,
    partSize=8 * 1024^2,
    batchSize=8,
)
```

Set `allowMultipart=false` to force one request. Set `compress=true` to gzip the uploaded
bytes. CloudStore does not add `.gz` to the key.

## Download

Download into a new byte vector:

```julia
data = CloudStore.get(bucket, "reports/today.csv")
```

Download into an existing destination:

```julia
buffer = Vector{UInt8}(undef, expected_size)
CloudStore.get(bucket, "large.bin", buffer)

CloudStore.get(bucket, "large.bin", "/tmp/large.bin")

open("/tmp/large.bin", "w") do io
    CloudStore.get(bucket, "large.bin", io)
end
```

Multipart downloads first read object metadata. Use `objectMaxSize` when you know an upper
bound and want to avoid that request for a small object. Set `decompress=true` to gunzip the
downloaded bytes.

## Metadata and listing

Read response headers without downloading the object:

```julia
headers = CloudStore.head(bucket, "reports/today.csv")
content_length = parse(Int, headers["Content-Length"])
```

List every object, or limit the result to a prefix:

```julia
objects = CloudStore.list(bucket)
reports = CloudStore.list(bucket; prefix="reports/")
```

CloudStore follows provider pagination until it has the complete result. `maxKeys` controls
the page size. It does not limit the total returned object count.

## Use an Object

An upload or listing returns [`CloudStore.Object`](@ref) values. You can use an object as the
source for later operations:

```julia
data = CloudStore.get(obj)
headers = CloudStore.head(obj)
CloudStore.delete(obj)
```

Constructing an object by key sends a metadata request:

```julia
obj = CloudStore.Object(bucket, "reports/today.csv")
```

## Delete

Delete one object:

```julia
CloudStore.delete(bucket, "reports/today.csv")
```

CloudStore does not create or delete S3 buckets or Azure containers. Use the provider control
plane for store lifecycle operations.

## Pass request options

Extra keyword arguments pass to the signed CloudBase HTTP request. This supports options such
as timeouts, retries, and explicit credentials. Treat provider-specific headers and query
parameters with care because they can change request signing.
