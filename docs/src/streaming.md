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
