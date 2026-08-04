# CloudStore.jl

CloudStore.jl provides one object-storage interface for Amazon S3 and Azure Blob Storage.
It supports object listing, upload, download, metadata, deletion, parallel transfers, and
streaming transfers.

CloudStore uses [CloudBase.jl](https://github.com/JuliaServices/CloudBase.jl) for credentials
and signed HTTP requests. It does not create buckets, containers, or other cloud resources.

## Installation

Install CloudStore from the General registry:

```julia
using Pkg
Pkg.add("CloudStore")
```

Load the package and the provider namespace that you need:

```julia
using CloudStore

const S3 = CloudStore.S3
const Blobs = CloudStore.Blobs
```

CloudStore keeps its public names inside the package namespace. It does not add common names
such as `get`, `put`, or `delete` to your session.

## Create a store

Create an S3 bucket handle with its name and region:

```julia
bucket = S3.Bucket("my-bucket", "us-west-2")
```

Create an Azure container handle with its container name and storage account:

```julia
container = Blobs.Container("my-container", "mystorageaccount")
```

These constructors do not send a network request. They only create a handle for later
operations.

## Credentials

CloudStore accepts `credentials` as a keyword argument. CloudBase can also load credentials
from the standard environment, configuration files, and cloud-host metadata services.

```julia
data = CloudStore.get(bucket, "reports/today.csv"; credentials)
```

Do not put long-lived secrets in source code. Use your cloud provider's standard credential
chain when possible.

## Choose an interface

The generic interface dispatches from the store type:

```julia
CloudStore.put(bucket, "hello.txt", codeunits("hello"))
CloudStore.get(bucket, "hello.txt")
```

The provider namespaces offer the same operations:

```julia
S3.put(bucket, "hello.txt", codeunits("hello"))
Blobs.put(container, "hello.txt", codeunits("hello"))
```

You can also use an S3 or Azure object URL. Provide `region` for an S3 URL when the URL does
not contain it.

```julia
CloudStore.get("s3://my-bucket/reports/today.csv"; region="us-west-2")
CloudStore.get("https://mystorageaccount.blob.core.windows.net/my-container/data.csv")
```

See [Object operations](@ref) for the complete transfer workflow.
