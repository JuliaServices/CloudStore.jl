# Caller-owned checkpoint policy for one immutable, nonempty local file. Run one
# writer per checkpoint and destination. Credentials are supplied separately.
module ManualResumeExample

using CloudStore, SHA, TOML, Base64
import CloudStore: S3, Blobs

digest(path) = open(io -> bytes2hex(sha256(io)), path)

function save(path, checkpoint)
    open(path * ".tmp", "w") do io
        TOML.print(io, checkpoint)
    end
    mv(path * ".tmp", path; force=true)
    return checkpoint
end

function partbytes(checkpoint, part)
    bytes = open(checkpoint["source"]) do io
        seek(io, part["offset"])
        read(io, part["size"])
    end
    length(bytes) == part["size"] && bytes2hex(sha256(bytes)) == part["sha256"] ||
        error("source part changed; refusing upload")
    return bytes
end

function verify(store, checkpoint)
    store.baseurl == checkpoint["store"] || error("checkpoint belongs to a different destination")
    filesize(checkpoint["source"]) == checkpoint["size"] &&
        digest(checkpoint["source"]) == checkpoint["sha256"] || error("source changed; refusing resume")
    return nothing
end

function stage(store, checkpoint, part, credentials)
    bytes = partbytes(checkpoint, part)
    receipt = if store isa S3.Bucket
        S3.uploadpart(store, checkpoint["key"], checkpoint["upload_id"], part["number"], bytes; credentials)
    else
        Blobs.stageblock(store, checkpoint["key"], part["id"], bytes; credentials)
    end
    return Dict(string(name) => value for (name, value) in pairs(receipt))
end

"""Create a checkpoint and acknowledge selected parts, without publishing the object."""
function start(store, key, source, checkpoint_path; credentials, initial=(1,), partsize=5 * 1024^2)
    isfile(checkpoint_path) && error("checkpoint already exists")
    size = filesize(source)
    size > 0 || error("this example requires a nonempty file")
    partsize >= 5 * 1024^2 || error("this example uses parts of at least 5 MiB")
    cld(size, partsize) <= 10000 || error("too many parts for this example")
    marker = bytes2hex(rand(UInt8, 16))
    checkpoint = Dict{String,Any}("source" => abspath(source), "size" => size,
        "sha256" => digest(source), "store" => store.baseurl, "key" => key,
        "marker" => marker, "state" => "uploading", "receipts" => Dict{String,Any}())
    checkpoint["parts"] = open(source) do io
        parts = Dict{String,Any}[]
        for number in 1:cld(size, partsize)
            offset = position(io)
            bytes = read(io, partsize)
            push!(parts, Dict("number" => number, "offset" => offset, "size" => length(bytes),
                "sha256" => bytes2hex(sha256(bytes)), "id" => base64encode(marker * lpad(number, 5, '0'))))
        end
        parts
    end
    verify(store, checkpoint)
    if store isa S3.Bucket
        checkpoint["upload_id"] = S3.createmultipartupload(store, key; credentials,
            headers=["x-amz-meta-checkpoint" => marker])
    end
    save(checkpoint_path, checkpoint)
    for number in initial
        part = checkpoint["parts"][number]
        checkpoint["receipts"][string(number)] = stage(store, checkpoint, part, credentials)
        save(checkpoint_path, checkpoint)
    end
    return checkpoint
end

"""Verify source and saved acknowledgements, fill missing parts, and publish once."""
function resume(store, checkpoint_path; credentials)
    checkpoint = TOML.parsefile(checkpoint_path)
    verify(store, checkpoint)
    checkpoint["state"] == "uploading" || error("commit outcome requires reconciliation; refusing replay")
    receipts = checkpoint["receipts"]
    for part in checkpoint["parts"]
        receipt = get(receipts, string(part["number"]), nothing)
        receipt === nothing && continue
        identity = store isa S3.Bucket ? "number" : "id"
        receipt[identity] == part[identity] && receipt["size"] == part["size"] ||
            error("saved receipt does not match the source layout")
    end
    if store isa S3.Bucket
        listed = Dict(part.number => part for part in S3.listparts(store, checkpoint["key"], checkpoint["upload_id"]; credentials))
        for (number, receipt) in collect(receipts)
            remote = get(listed, parse(Int, number), nothing)
            if remote === nothing
                delete!(receipts, number)
            else
                remote.size == receipt["size"] && remote.etag == receipt["etag"] &&
                    all(get(remote.checksums, key, "") == value for (key, value) in receipt["checksums"]) ||
                    error("saved S3 receipt does not match remote part")
            end
        end
    else
        inventory = Blobs.listblocks(store, checkpoint["key"]; state=:uncommitted, credentials)
        listed = Dict(block.id => block.size for block in inventory.uncommitted)
        for (number, receipt) in collect(receipts)
            if !haskey(listed, receipt["id"])
                delete!(receipts, number)
            else
                listed[receipt["id"]] == receipt["size"] || error("saved Azure receipt does not match remote block")
            end
        end
    end
    for part in checkpoint["parts"]
        number = string(part["number"])
        if !haskey(receipts, number)
            # A server-side part without a saved acknowledgement is uploaded again.
            receipts[number] = stage(store, checkpoint, part, credentials)
            save(checkpoint_path, checkpoint)
        end
    end
    verify(store, checkpoint)
    checkpoint["state"] = "committing"
    save(checkpoint_path, checkpoint)
    etag = if store isa S3.Bucket
        parts = [(number=part["number"], etag=receipts[string(part["number"])]["etag"],
            checksums=receipts[string(part["number"])]["checksums"]) for part in checkpoint["parts"]]
        S3.completemultipartupload(store, checkpoint["key"], checkpoint["upload_id"], parts;
            credentials, headers=["If-None-Match" => "*"])
    else
        blocks = [(id=part["id"], state=:uncommitted) for part in checkpoint["parts"]]
        Blobs.commitblocks(store, checkpoint["key"], blocks; credentials,
            headers=["If-None-Match" => "*", "x-ms-meta-checkpoint" => checkpoint["marker"]])
    end
    checkpoint["state"] = "complete"
    checkpoint["etag"] = etag
    save(checkpoint_path, checkpoint)
    return etag
end

"""
Recognize a completed ambiguous commit without issuing another write.
Verification downloads the whole object into memory.
"""
function reconcile(store, checkpoint_path; credentials)
    checkpoint = TOML.parsefile(checkpoint_path)
    verify(store, checkpoint)
    checkpoint["state"] == "committing" || error("checkpoint has no ambiguous commit")
    headers = CloudStore.head(store, checkpoint["key"]; credentials, allowMultipart=false)
    name = store isa S3.Bucket ? "x-amz-meta-checkpoint" : "x-ms-meta-checkpoint"
    any(lowercase(key) == name && value == checkpoint["marker"] for (key, value) in headers) ||
        error("destination belongs to a different upload")
    # Pin the read to the version inspected above; a concurrent overwrite must fail.
    etag = only(value for (key, value) in headers if lowercase(key) == "etag")
    bytes = CloudStore.get(store, checkpoint["key"]; credentials, allowMultipart=false,
        headers=["If-Match" => etag])
    length(bytes) == checkpoint["size"] && bytes2hex(sha256(bytes)) == checkpoint["sha256"] ||
        error("published bytes do not match the source")
    checkpoint["state"] = "complete"
    checkpoint["etag"] = etag
    save(checkpoint_path, checkpoint)
    return etag
end

end
