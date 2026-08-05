# API reference

CloudStore uses a small namespace-based API. The main operations are:

```julia
CloudStore.list(store; kwargs...)
CloudStore.get(store, key[, destination]; kwargs...)
CloudStore.head(store, key; kwargs...)
CloudStore.put(store, key, source; kwargs...)
CloudStore.delete(store, key; kwargs...)
```

The same operation names are available as `CloudStore.S3.list`, `CloudStore.S3.get`, and so
on, and as `CloudStore.Blobs.list`, `CloudStore.Blobs.get`, and so on.

## Object

```@docs
CloudStore.Object
```

## Transfer streams

```@docs
CloudStore.PrefetchedDownloadStream
CloudStore.MultipartUploadStream
CloudStore.abort
```
