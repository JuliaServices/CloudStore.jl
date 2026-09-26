# Manual multipart uploads

Use these provider operations when an application needs to save progress and
continue an upload in another process. The application owns the remote upload,
source identity, successful receipts, checkpoint, and decision to publish or
abandon it. No operation starts background work or automatically aborts on error.

For ordinary uploads, keep using `CloudStore.put` or
[`CloudStore.MultipartUploadStream`](@ref). Their existing cleanup behavior still
applies: an aborted managed upload is not a resumable checkpoint.

## Provider operations

All names are qualified; none is exported.

| S3 | Azure Blob Storage |
|:---|:---|
| `S3.createmultipartupload(bucket, key)` returns an upload ID | No creation request; choose a unique block-ID namespace |
| `S3.uploadpart(bucket, key, id, number, bytes)` returns a receipt | `Blobs.stageblock(container, key, block_id, bytes)` returns a receipt |
| `S3.listparts(bucket, key, id)` follows all pages | `Blobs.listblocks(container, key; state=:all)` separates committed and uncommitted blocks |
| `S3.completemultipartupload(bucket, key, id, receipts)` publishes | `Blobs.commitblocks(container, key, blocks)` publishes an explicit ordered selection |
| `S3.abortmultipartupload(bucket, key, id)` discards the upload | There is no abort operation; do not delete an existing blob to discard staged blocks |

These methods take a store and a **literal object key**, plus explicit
`credentials` when needed. They do not discover credentials. Request headers are
copied. Byte vectors and views are borrowed until the synchronous call returns,
including permitted part retries; do not mutate them concurrently.

S3 key support also depends on the installed signer. CloudBase 1.4.10, used with
Julia 1.6, incorrectly signs keys that require URL escaping. CloudBase 1.6 supports
these keys and requires Julia 1.10 or later.

HTTP timeout and signing options pass through to CloudBase. Operation-owned
`query`, `body`, `service`, `response_stream`, and `status_exception` options cannot
be replaced. Creation and final publication also reserve `retry` and `redirect`
and disable both. In particular, Azure's final PUT is not automatically retried
merely because HTTP normally considers PUT idempotent.

## Receipts and inspection

Save the exact S3 receipt from every successful upload: `number`, quoted opaque
`etag`, byte `size`, and optional `checksums`. Completion requires strictly
increasing, unique part numbers and does not renumber them. Checksum metadata is
preserved for completion; CloudStore does not calculate or verify those checksums.
The object ETag returned by completion has its surrounding quotes removed, matching
the existing upload API. A listing adds `last_modified`, but should not replace the
application's acknowledgement ledger. If a part reached the server before its
receipt was saved, upload the verified same bytes under the same number again.
[S3 multipart lifecycle](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)

S3 inspection requests at most 1000 parts per page and rejects malformed or
non-advancing pages instead of returning a partial result. Concurrent uploads can
change between pages; inspection is not an atomic snapshot. S3's part-size, part-count,
encryption, checksum-mode, and permission requirements still apply. Listing may
need permissions beyond those used to upload.
[S3 ListParts](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html)

Azure block IDs are canonical Base64 encodings of 1–64 bytes. All IDs for one blob
must have equal decoded lengths. Use a unique fixed-length namespace for each
upload. A block ID may simultaneously identify different committed and uncommitted
bytes. `commitblocks` preserves the caller's order and selects each entry with
`:committed`, `:uncommitted`, or `:latest`; repeated IDs must use the same selector.
`:latest` falls back to committed data when an uncommitted block is missing.
Use `:uncommitted` when resuming a fresh upload to avoid that substitution.
[Block identity](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block),
[commit selectors](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list)

Azure listings contain no per-block content checksum. ID and size alone cannot
prove that a block came from the intended source. Safe reuse requires the
application's unique IDs, saved acknowledgements, immutable source, and exclusive
ownership of those IDs. Listings are not atomic snapshots of concurrent staging.
Uncommitted blocks expire under the provider's retention policy.
[Azure block inspection](https://learn.microsoft.com/en-us/rest/api/storageservices/get-block-list)

## Restarting in another process

The runnable [checkpoint example](https://github.com/JuliaServices/CloudStore.jl/blob/main/examples/manual_resume.jl)
implements one deliberately narrow policy: one nonempty immutable local file,
one writer, a new destination, fixed parts of at least 5 MiB, and a caller-owned
TOML file. It uses only CloudStore and Julia standard libraries. Credentials never
enter the checkpoint. Keep the checkpoint private and use a durable checkpoint
store if recovery after an OS or storage failure is required; its file replacement
does not claim crash-durable persistence.

In the first process, construct `store` and `credentials`, then:

```julia
include("examples/manual_resume.jl")
ManualResumeExample.start(store, "new-object", "source.bin", "upload.toml";
    credentials, initial=(3, 1))
```

After that process exits, reconstruct the same store and credentials in another
process. The checkpoint supplies the key, upload ID or block IDs, source hash and
layout, and saved receipts:

```julia
include("examples/manual_resume.jl")
ManualResumeExample.resume(store, "upload.toml"; credentials)
```

The example checks the full source hash before network requests and each part's
hash before uploading. It compares saved acknowledgements against provider
inspection, fills missing or unacknowledged parts, and commits in source order.
The caller must prevent concurrent source changes and checkpoint writers; hashes
do not lock a file or make an untrusted checkpoint safe.

## Lost final responses

A timeout or error after publication does **not** prove the object was rejected.
The library does not replay a final request or delete data to roll it back. S3
completion also checks for an error embedded in an HTTP 200 XML response.
[S3 completion outcomes](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)

The example records `committing` before sending the final request and refuses a
second commit in that state. To recognize successful publication, it checks a
unique marker in object metadata, then downloads the inspected version using an
ETag condition and compares its bytes with the source hash:

```julia
ManualResumeExample.reconcile(store, "upload.toml"; credentials)
```

This example downloads the whole object into memory during reconciliation, so
verification can require object-sized memory even though uploads read one part
at a time.

If reconciliation fails, stop and investigate the provider state. It does not
automatically retry publication. Conditional headers such as `If-None-Match` and
`If-Match` are available on manual completion; the example requires a new object.
Azure commit replaces metadata unless it is supplied again.

## API

```@docs
CloudStore.S3.createmultipartupload
CloudStore.S3.uploadpart
CloudStore.S3.listparts
CloudStore.S3.completemultipartupload
CloudStore.S3.abortmultipartupload
CloudStore.Blobs.stageblock
CloudStore.Blobs.listblocks
CloudStore.Blobs.commitblocks
```
