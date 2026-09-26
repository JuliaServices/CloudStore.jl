# Streaming transfers

Use a stream when the complete object should not be in memory at one time.

## Prefetched downloads

[`CloudStore.PrefetchedDownloadStream`](@ref) reads ranges in parallel into two buffers.
Construct it from an [`Object`](@ref):

```julia
obj = CloudStore.Object(bucket, "large.csv")
io = CloudStore.PrefetchedDownloadStream(obj)
try
    while !eof(io)
        row = readline(io)
        # Process the row.
    end
finally
    close(io)
end
```

Tune the memory and request sizes when needed:

```julia
io = CloudStore.PrefetchedDownloadStream(
    obj,
    16 * 1024^2;
    prefetch_multipart_size=2 * 1024^2,
)
```

The first positional size is the size of each in-memory prefetch buffer. The keyword value is
the maximum size of each range request. The stream is read-only and is not thread-safe.

Prefetched streams and `copyto!(dest, doff, object, soff, n)` check every range response
against the object's size and strong ETag, and throw if the object changed. An `Object`
without an ETag is refreshed with a metadata request first; `copyto!` does this on every
call. The stream returns the stored bytes; wrap it in a decompressor such as
`CodecZlib.GzipDecompressorStream` for gzip objects.

## Multipart uploads

[`CloudStore.MultipartUploadStream`](@ref) sends each written byte vector as one part. The
do-block form waits for all parts and completes the upload:

```julia
CloudStore.MultipartUploadStream(bucket, "generated.bin") do io
    for chunk in chunks
        write(io, chunk)
    end
end
```

Each `chunk` must be a `Vector{UInt8}`. Except for the final part, Amazon S3 requires parts to
meet its minimum part size. Use `CloudStore.put` for small objects.

Use the manual form only when you need direct lifecycle control:

```julia
io = CloudStore.MultipartUploadStream(bucket, "generated.bin")
write(io, first_chunk)
write(io, second_chunk)
wait(io)
close(io)
```

Keep chunks in object order. `concurrent_writes_to_channel` limits the number of uploads in
flight and applies backpressure to `write`.


## Default buffered transfers

With `compress=false`, `CloudStore.put` uploads byte vectors and the readable part
of an `IOBuffer` without copying them. Multipart parts are views into the input.
Keep that storage unchanged until the call returns, including retries. Files,
other `IO` inputs, and compressed uploads use extra buffers. On HTTP 1, views
over non-`Array` storage (such as string bytes) are copied first.

`CloudStore.get(store, key, destination)` accepts a byte vector or a writable
byte view. Multipart downloads give each concurrent range request its own part
of the destination. File and `IO` outputs use bounded part buffers and write each
part in order while later parts download. `copyto!` receives directly into a
contiguous destination.

CloudStore gives HTTP a private `HTTP.Headers` collection for each request and
passes `copyheaders=false` when the installed HTTP version honors it. Caller
headers stay unchanged, and concurrent parts never share headers. A
`copyheaders=true` keyword from the caller still takes precedence.

The lowest-allocation path needs an HTTP version that supports `copyheaders` and
a CloudBase version that signs buffered payloads without copying them.
`bench/transfer_allocations.jl --check` measures client allocations against
local MinIO and Azurite services, including one forced retry per data request.
