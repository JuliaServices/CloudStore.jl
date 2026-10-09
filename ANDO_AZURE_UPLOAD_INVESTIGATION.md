# Ando Azure Blob Upload Investigation

Date: 2026-03-27

This report captures what we observed while debugging blob-backed persistence for the `Ando` Julia service. The immediate user-facing problem was:

- Claude restore/follow-up worked.
- Codex same-container follow-up worked.
- Codex restore/follow-up on a fresh container failed.

The original question was whether the problem was:

- an `Ando` usage bug,
- an Azure Blob usage bug,
- or a `CloudStore` / `CloudBase` implementation bug.

The short answer is:

- `Ando` originally had some real race/overwrite issues that made the problem easier to hit.
- even after tightening `Ando`, there is still strong evidence of a `CloudStore` Azure upload-path bug or limitation.
- the most suspicious paths are:
  - `CloudStore.put(..., path::String)` for small blob uploads
  - `CloudStore.put(..., bytes; allowMultipart=true)` for large blob uploads that cross the multipart threshold

## Scope

The investigation was performed in the context of `Ando`, which uses Azure Blob Storage to persist:

- provider state bundles
  - `dev/codex/home.tar.gz`
  - `dev/claude/home.tar.gz`
- SQLite eval state
  - `state/dev/ando-evals.sqlite`

The relevant local repos / packages were:

- app under test:
  - `/Users/jacob.quinn/andavo/apps/julia/andavo/Ando`
- dev CloudStore checkout:
  - `/Users/jacob.quinn/.julia/dev/CloudStore`
- installed package copies actually loaded by Julia during most tests:
  - `~/.julia/packages/CloudStore/6XBiA`
  - `~/.julia/packages/CloudBase/Tgheg`

Versions observed during the investigation:

- `CloudStore v1.6.4`
- `CloudBase v1.4.10`
- `codex-cli 0.117.0`
- `claude-code 2.1.85`

## Original Symptom Chain

What worked:

- Codex login in container worked.
- Codex headless eval in the same running service worked.
- Codex follow-up/resume in the same running service worked.
- Claude login / eval / restore / follow-up all worked.

What failed:

- fresh-container Codex restore + resume failed with:

```text
thread/resume failed: no rollout found for thread id 019d3068-d0a6-7f21-bddc-af933fe8cc5f
```

Important control experiment:

- if the live `.codex` directory was copied directly from the working container into the fresh container, `codex resume` succeeded.

That was the first strong sign that:

- Codex resume itself was not fundamentally broken across containers
- blob sync / restore was preserving the wrong bytes

## Initial Azure Error Seen In Production Path

Before the lower-level blob checks, the first concrete Azure failure we saw from the live `Ando` sync path was:

- `HTTP 400 Bad Request`
- `x-ms-error-code: InvalidBlockList`
- on the Azure `PUT ...?comp=blocklist` step

This happened when syncing a large Codex provider bundle.

That error strongly suggested multipart block upload issues, and it fit `Ando`'s original behavior because `sync_provider_bundle!` could be triggered from multiple places:

- periodic provider sync loop
- eval finalization
- login completion / refresh
- shutdown cleanup

At the `Ando` level, there really was a bug/risk here:

- same blob key could be overwritten concurrently
- bundle creation used a fixed temp filename

Those `Ando` issues were real and were patched later, but they were not the whole story.

## Relevant CloudStore / CloudBase Code Paths Inspected

### `CloudStore/src/put.jl`

The important behavior in `putObjectImpl` is:

- multipart threshold is `8 MB`
- multipart part size is `8 MB`
- if input is small enough:
  - `prepBody(x, ...)`
- if input is larger:
  - `prepBodyMultipart(x, ...)`

For `String` input:

- small upload path:
  - `prepBody(x::String, ...)` uses `Mmap.mmap(x)`
- multipart upload path:
  - `prepBodyMultipart(x::String, ...)` uses `open(x, "r")`

### `CloudStore/src/blobs.jl`

Azure multipart block upload uses:

- `uploadPart` with block ids generated from:

```julia
base64encode(lpad(partNumber - 1, 64, '0'))
```

- `completeMultipartUpload` by sending a `<BlockList>` to:
  - `PUT ...?comp=blocklist`

Nothing obviously wrong jumped out from block-id formatting alone.

### `CloudBase/src/reseau_http.jl`

The transport layer does:

- `AbstractVector{UInt8}` -> copied bytes body
- `IO` -> read into bytes
- `AbstractString` -> treated as string body

For the CloudStore path upload case, the path string does not reach transport directly; CloudStore converts it first via `Mmap.mmap` or `open(...)`.

That matters because the path-based bug seems to be above the raw transport layer.

## Repro Matrix

The following matrix is the most important part of the investigation.

| Scenario | Input form | Blob size range | Multipart used | Result |
| --- | --- | ---: | --- | --- |
| SQLite backup upload | `path::String` | ~100 KB | no | bad / stale / inconsistent |
| SQLite backup upload | `Vector{UInt8}` | ~100 KB | no | good |
| Codex tar upload | `Vector{UInt8}` | ~107 MB | yes | bad / truncated |
| Codex tar upload | `Vector{UInt8}` | ~107 MB | no (`allowMultipart=false`) | good |

## SQLite Findings

### Local source file was good

The local `Ando` source container had:

- `/root/ando-evals.sqlite`
- `/root/ando-evals-backup.sqlite`

Using a materialized query (`Tables.namedtupleiterator`), both files contained the target thread:

- thread id:
  - `CZU3oEuj6wCSqY1nuQ78Q`
- count in source DB:
  - `1`
- count in backup DB:
  - `1`

### Blob created via `CloudStore.put(..., path::String)` was bad

At one point the local backup file and blob-downloaded copy differed like this:

- local backup:
  - path: `/root/ando-evals-backup.sqlite`
  - size: `102400`
  - sha256:
    - `f4cb66eca4d3089d58b945df4f54c4a98221981a8afd84149d85bbe018896413`
- downloaded blob copy:
  - path: `/tmp/ando-blob.sqlite`
  - size: `86016`
  - sha256:
    - `ba4732e8d618949d76ee6460b97dd6daf666d5f96e39957a1bf45ce143541ca7`

This was not just a checksum mismatch. Querying the blob-downloaded file showed the target thread count was:

- `0`

We also saw Azure `HEAD` return unexpected content lengths for the same SQLite blob key during this investigation. One direct check returned:

- `Content-Length: 69632`

The exact size varied across attempts, but the important point is:

- path-based uploads produced blob objects whose contents and size did not reliably match the local source file

### SQLite bytes upload worked

When the exact same SQLite backup file was uploaded as raw bytes instead of a path:

```julia
bytes = read("/root/ando-evals-backup.sqlite")
CloudStore.put(container, key, bytes; credentials=creds)
```

the round-trip worked:

- uploaded size:
  - `102400`
- downloaded blob query count:
  - `1`

This strongly suggests:

- the small-file problem is not Azure Blob Storage in general
- the bad path is specifically `CloudStore.put(..., path::String)` or its immediate implementation path

## Codex Bundle Findings

### Same-container resume was fine

For Codex, we proved:

- same-container create/follow-up succeeded
- `providerSessionId` / thread id was preserved

Example session id:

- `019d3087-8faa-70d0-b411-b5d7a00bd6f1`

### Direct container-to-container live `.codex` copy was fine

When the live `.codex` directory was copied directly from the source container into the restore container, Codex resume succeeded.

This ruled out:

- "Codex cannot resume across containers"

### Multipart blob-backed Codex bundle was bad

The Codex bundle tarball used for restore was large enough to trigger multipart upload.

One key repro used:

- local source tar:
  - `/tmp/codex-home.tar.gz`
  - size: `107474681`
  - sha256:
    - `8fd9e903357af8a3ab6f81451db2589cafed88d72edc0f300070f49d41b14ab5`

After uploading the tarball bytes with default multipart behavior and downloading the blob back:

- blob tar:
  - `/tmp/codex-blob.tar.gz`
  - size: `100665444`
  - sha256:
    - `455e3cf95c2733c5b6ff27215c0a426e44642713eff82470f960323407417663`

The source and blob copies were not identical.

That is the strongest evidence we found for the real Codex restore bug:

- the Codex state bundle was being truncated or otherwise altered on the multipart upload/download path

### Forced single-part Codex bytes upload worked

We then repeated the same Codex tar upload but forced:

- `allowMultipart=false`

That round-tripped cleanly:

- source tar:
  - sha256:
    - `8fd9e903357af8a3ab6f81451db2589cafed88d72edc0f300070f49d41b14ab5`
- blob-downloaded tar:
  - sha256:
    - `8fd9e903357af8a3ab6f81451db2589cafed88d72edc0f300070f49d41b14ab5`
- source size:
  - `107474681`
- downloaded size:
  - `107474681`

This is the strongest evidence that:

- the large-file Codex corruption is specifically in the multipart upload path
- Azure Blob Storage itself is not rejecting the data
- single-part bytes upload is a working workaround for this payload size

## Codex Restore Error After Bad Blob Upload

When the restore container was populated from the corrupted blob-backed Codex tar, the resumed turn failed with:

```text
thread/resume failed: no rollout found for thread id 019d3087-8faa-70d0-b411-b5d7a00bd6f1
```

Important nuance:

- this error appears to come from Codex because critical session/rollout state was missing from the restored `.codex` bundle
- it is not itself evidence that Codex resume is impossible
- it is evidence that the uploaded bundle was incomplete or damaged

## Important Red Herring

During investigation, one test accidentally nested the source codex directory as:

- `/root/.codex/.codex/auth.json`

instead of:

- `/root/.codex/auth.json`

That caused unrelated failures like:

```text
unexpected status 401 Unauthorized: Missing bearer or basic authentication in header
```

This was a test harness mistake, not part of the CloudStore/Azure bug.

It is worth keeping in the report because it briefly looked like a Codex auth refresh problem when it was really just the wrong file path.

## What Was Fixed In `Ando`

The following `Ando` changes were made during the investigation:

- serialized provider syncs with a per-provider `ReentrantLock`
- stopped using a fixed temp bundle filename
- skipped unchanged provider bundle uploads by fingerprint
- changed blob replacement to delete first, then upload
- changed blob replacement to upload raw bytes instead of a file path
- finally changed blob replacement to force:
  - `allowMultipart=false`

The rationale for those changes:

- the lock and temp-file fixes address real app-level race conditions
- delete-then-put avoids stale same-key overwrite behavior
- bytes upload avoids the bad path upload behavior seen with SQLite
- `allowMultipart=false` avoids the bad large-file multipart behavior seen with Codex

## What We Know vs. What We Infer

### What we know

- `CloudStore.put(..., path::String)` can produce wrong blob contents for a small SQLite file in this setup.
- `CloudStore.put(..., bytes)` works for the same small SQLite file.
- `CloudStore.put(..., bytes)` with multipart default produces a corrupted/truncated large Codex tarball in this setup.
- `CloudStore.put(..., bytes; allowMultipart=false)` preserves the large Codex tarball exactly in this setup.
- Codex restore failure was caused by bad restored state, not by Codex being inherently unable to resume across containers.

### What we infer

- the root package bug is likely split into two related issues:
  - a path upload issue for small Azure blobs
  - a multipart upload issue for large Azure blobs
- both issues appear to be in `CloudStore` / `CloudBase` Azure behavior rather than in Azure itself
- the original `Ando` concurrency made the multipart issue easier to trigger, but was not the sole cause

## Likely Areas To Investigate In CloudStore

### 1. `path::String` upload path

Suggested minimal repro:

- create a small SQLite file with a known row
- upload via:
  - `CloudStore.put(container, key, "/path/to/file"; credentials=...)`
- immediately download and compare:
  - `sha256`
  - `Content-Length`
  - actual SQLite row count
- repeat with:
  - `CloudStore.put(container, key, read("/path/to/file"); credentials=...)`

Why this matters:

- we already know the bytes path succeeds where the path path fails

### 2. Azure multipart upload path

Suggested minimal repro:

- create a stable large blob around `100-120 MB`
- upload via:
  - `CloudStore.put(container, key, bytes; credentials=...)`
- download and compare hash
- repeat with:
  - `allowMultipart=false`

Why this matters:

- the source tar and blob tar had different hashes under multipart
- they matched exactly under forced single-part

### 3. Overwrite semantics

Even before the bytes vs path split was fully clear, same-key overwrite behavior looked suspicious.

Suggested check:

- compare:
  - `CloudStore.put(existing_key, ...)`
  - `CloudStore.delete(existing_key); CloudStore.put(new_data, ...)`

for both small and large blobs.

### 4. Azure block upload implementation

Even though block IDs looked sane on inspection, the `InvalidBlockList` error and corrupted large blob strongly suggest inspecting:

- staged block upload ordering
- block list finalization
- overwrite behavior when the same key already exists
- possible interaction with prior staged blocks on the same blob name

## Most Important Raw Artifacts

### Azure multipart error

Observed from the live provider sync path:

```text
HTTP 400 Bad Request
x-ms-error-code: InvalidBlockList
PUT ...?comp=blocklist
```

### Broken Codex restore error

Observed after restoring from the corrupted blob-backed Codex tar:

```text
thread/resume failed: no rollout found for thread id 019d3087-8faa-70d0-b411-b5d7a00bd6f1
```

### Broken SQLite path-upload round-trip

- local backup hash:
  - `f4cb66eca4d3089d58b945df4f54c4a98221981a8afd84149d85bbe018896413`
- blob copy hash:
  - `ba4732e8d618949d76ee6460b97dd6daf666d5f96e39957a1bf45ce143541ca7`

### Broken Codex multipart round-trip

- source tar hash:
  - `8fd9e903357af8a3ab6f81451db2589cafed88d72edc0f300070f49d41b14ab5`
- blob tar hash:
  - `455e3cf95c2733c5b6ff27215c0a426e44642713eff82470f960323407417663`

### Good Codex single-part round-trip

- source tar hash:
  - `8fd9e903357af8a3ab6f81451db2589cafed88d72edc0f300070f49d41b14ab5`
- blob tar hash:
  - `8fd9e903357af8a3ab6f81451db2589cafed88d72edc0f300070f49d41b14ab5`

## Bottom Line

The best current explanation is:

- `CloudStore` Azure uploads are not safe for `Ando` state persistence when using:
  - `path::String` uploads for small files
  - multipart uploads for large files

The current safest app-level workaround is:

- delete existing blob first
- upload raw bytes
- force `allowMultipart=false`

Given the current `Ando` bundle cap of `200 MB`, forcing single-part uploads is still operationally reasonable and produced the first exact round-trip we saw for the large Codex bundle.

If this is followed up inside `CloudStore`, the most useful goal is a standalone repro script that isolates:

- `path` vs `bytes`
- multipart vs single-part
- overwrite vs delete-then-put

against the same Azure container and the same blob keys.
