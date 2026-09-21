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

`CloudStore.put` borrows byte-vector inputs and the readable portion of an
`IOBuffer` when `compress=false`. Keep that storage unchanged until the call
returns, including all retries and multipart work. Each multipart byte-buffer
part is a view into the input. File, arbitrary `IO`, noncontiguous array, and
compression paths can require additional buffers. HTTP 1 retains a copy fallback
for views over non-Array storage, such as string bytes.

`CloudStore.get(store, key, destination)` accepts a byte vector or writable view.
For multipart downloads it gives each concurrent range request a disjoint view
of the destination. Without a destination, a multipart download allocates the
final vector after learning the object size. Single-request downloads without a
known size can grow their result buffer. File and IO multipart outputs use
bounded part buffers to preserve order.

CloudStore creates private `HTTP.Headers` collections before handing them to
HTTP with `copyheaders=false`. Caller headers remain unchanged and concurrent
parts do not share mutable headers. No separate fast API is required. Explicit
`copyheaders=true` remains available through forwarded keywords.

The complete allocation path also depends on dependency versions: older HTTP 2
versions, including 2.7.1, ignore `copyheaders=false` and stage downloads; older
CloudBase versions copy buffered AWS payloads before signing. Run
`bench/transfer_allocations.jl --check` against the selected stack to verify its allocation
profile. The script uses local authenticated MinIO and Azurite services and
prints package versions, bytes allocated, and elapsed time. It does not measure
cloud network saturation.

Even with the optimized stack, signing hashes data, TLS encrypts it, and HTTP/2
uses a reusable frame buffer. The supported target is no extra full-payload
materialization for eligible buffers, bounded working storage per active part,
and isolated retry state. This is not a promise of zero total allocations.
