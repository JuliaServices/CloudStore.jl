using SHA, TOML
include(joinpath(@__DIR__, "..", "examples", "manual_resume.jl"))

function resume_child(provider, phase, host, name, source, checkpoint, key)
    script = joinpath(@__DIR__, "manual_resume_child.jl")
    cmd = `$(Base.julia_cmd()) --startup-file=no --project=$(Base.active_project()) $script $provider $phase $host $name $source $checkpoint $key`
    output = IOBuffer()
    process = run(pipeline(ignorestatus(cmd); stdout=output, stderr=output); wait=false)
    if timedwait(() -> process_exited(process), 120.0) == :timed_out
        kill(process, Base.SIGKILL)
        wait(process)
        error("manual resume child timed out: $(String(take!(output)))")
    end
    wait(process)
    success(process) || error("manual resume child failed: $(String(take!(output)))")
    return String(take!(output))
end

function multipart_proxy(f, config)
    uri = HTTP.URI(config.store.baseurl)
    origin = "$(uri.scheme)://$(uri.host):$(uri.port)"
    requests = Channel{Any}(128)
    lose_commit_response = Ref(false)
    server = HTTP.serve!(0; listenany=true, verbose=false) do request
        query = manual_query(request)
        final = (request.method == "POST" && haskey(query, "uploadId")) ||
            (request.method == "PUT" && get(query, "comp", "") == "blocklist")
        # Keep the signed Host and original encoded target. Both disposable
        # providers accept a request addressed through this transparent proxy.
        bytes = manual_bytes(request.body)
        response = HTTP.request(request.method, origin * request.target, collect(request.headers), bytes;
            retry=false, redirect=false, status_exception=false, decompress=false)
        put!(requests, (method=request.method, query=query, size=length(bytes), final=final,
            status=response.status, signed=HTTP.hasheader(request, "Authorization")))
        if final && lose_commit_response[] && 200 <= response.status < 300
            lose_commit_response[] = false
            return HTTP.Response(503, [], "upstream commit response was lost")
        end
        # HTTP 2 client metadata counts received bytes (zero for HEAD); the
        # proxy must send the provider's declared object size back to the caller.
        if request.method == "HEAD" && hasproperty(response, :content_length)
            response.content_length = parse(Int, HTTP.header(response, "Content-Length", "0"))
        end
        return response
    end
    try
        host = "http://127.0.0.1:$(HTTP.port(server))"
        store = config.store isa S3.Bucket ? S3.Bucket(config.store.name; host) :
            Blobs.Container(config.store.name, "devstoreaccount1"; host)
        f(store, host, requests, lose_commit_response)
    finally
        close(server)
        close(requests)
    end
end

function drain_requests(requests)
    result = []
    while isready(requests)
        push!(result, take!(requests))
    end
    return result
end

@testset "Caller-owned multipart restart" begin
    for provider in (:s3, :azure)
        service = provider == :s3 ? CloudBase.CloudTest.Minio : CloudBase.CloudTest.Azurite
        options = provider == :s3 ? NamedTuple() : (use_ssl=false,)
        service.with(; options...) do config
            multipart_proxy(config) do store, host, requests, lose_commit_response
                credentials = config.credentials
                mktempdir() do dir
                    source, checkpoint = joinpath(dir, "source.bin"), joinpath(dir, "upload.toml")
                    expected = repeat(UInt8[0x00, 0x01, 0xff, 0x7f, 0x42], 2 * 1024^2 + 7)
                    write(source, expected) # Two full 5 MiB parts and a small last part.
                    key = "manual-resume/source.bin"
                    @test occursin("completed start", resume_child(string(provider), "start", host,
                        store.name, source, checkpoint, key))
                    started = drain_requests(requests)
                    @test all(request -> request.signed && 200 <= request.status < 300, started)
                    @test count(request -> request.method == "PUT", started) == 3
                    @test !any(request -> request.final, started)
                    state = TOML.parsefile(checkpoint)
                    @test sort(collect(keys(state["receipts"]))) == ["1", "3"]
                    @test !occursin("minioadmin", read(checkpoint, String))
                    @test !occursin("Eby8vdM", read(checkpoint, String))
                    if provider == :s3
                        @test [part.number for part in S3.listparts(store, key, state["upload_id"]; credentials, maxparts=1)] == [1, 2, 3]
                        @test length(drain_requests(requests)) == 3
                    else
                        inventory = Blobs.listblocks(store, key; credentials)
                        @test isempty(inventory.committed)
                        @test length(inventory.uncommitted) == 3
                        drain_requests(requests)
                    end

                    open(source, "r+") do io
                        write(io, UInt8(9))
                    end
                    @test_throws ErrorException("source changed; refusing resume") ManualResumeExample.resume(store, checkpoint; credentials)
                    @test !isready(requests)
                    open(source, "r+") do io
                        write(io, first(expected))
                    end
                    corrupt = deepcopy(state)
                    corrupt["receipts"]["1"]["size"] += 1
                    ManualResumeExample.save(checkpoint, corrupt)
                    @test_throws ErrorException("saved receipt does not match the source layout") ManualResumeExample.resume(store, checkpoint; credentials)
                    @test !isready(requests)
                    ManualResumeExample.save(checkpoint, state)

                    # A changed remote part must not be silently accepted as the
                    # saved acknowledgement. Restore it afterward using the same source.
                    part = state["parts"][1]
                    if provider == :s3
                        S3.uploadpart(store, key, state["upload_id"], 1, UInt8[9]; credentials)
                    else
                        Blobs.stageblock(store, key, part["id"], UInt8[9]; credentials)
                    end
                    drain_requests(requests)
                    message = provider == :s3 ? "saved S3 receipt does not match remote part" : "saved Azure receipt does not match remote block"
                    @test_throws ErrorException(message) ManualResumeExample.resume(store, checkpoint; credentials)
                    @test all(request -> request.method == "GET", drain_requests(requests))
                    ManualResumeExample.stage(store, state, part, credentials)
                    drain_requests(requests)

                    @test occursin("completed resume", resume_child(string(provider), "resume", host,
                        store.name, source, checkpoint, key))
                    resumed = drain_requests(requests)
                    parts = filter(request -> request.method == "PUT" && !request.final, resumed)
                    @test length(parts) == 1
                    @test provider == :s3 ? only(parts).query["partNumber"] == "2" :
                        only(parts).query["blockid"] == state["parts"][2]["id"]
                    @test count(request -> request.final, resumed) == 1
                    @test all(request -> request.signed && 200 <= request.status < 300, resumed)
                    @test TOML.parsefile(checkpoint)["state"] == "complete"
                    @test CloudStore.get(store, key; credentials, allowMultipart=false) == expected
                    drain_requests(requests)

                    # A successful provider commit followed by a lost response is
                    # neither replayed nor rolled back. Reconciliation only reads.
                    small = joinpath(dir, "small.bin")
                    write(small, UInt8[1, 2, 3])
                    ambiguous = joinpath(dir, "ambiguous.toml")
                    ManualResumeExample.start(store, "ambiguous", small, ambiguous; credentials)
                    drain_requests(requests)
                    lose_commit_response[] = true
                    @test_throws Exception ManualResumeExample.resume(store, ambiguous; credentials)
                    failed = drain_requests(requests)
                    @test count(request -> request.final, failed) == 1
                    @test all(request -> request.method != "DELETE", failed)
                    @test only(filter(request -> request.final, failed)).status in (200, 201)
                    @test TOML.parsefile(ambiguous)["state"] == "committing"
                    @test_throws ErrorException("commit outcome requires reconciliation; refusing replay") ManualResumeExample.resume(store, ambiguous; credentials)
                    @test !isready(requests)
                    @test !isempty(ManualResumeExample.reconcile(store, ambiguous; credentials))
                    @test all(request -> request.method in ("HEAD", "GET"), drain_requests(requests))
                    @test TOML.parsefile(ambiguous)["state"] == "complete"

                    # A destination condition failure must preserve the existing object.
                    blocked = joinpath(dir, "blocked.toml")
                    ManualResumeExample.start(store, key, small, blocked; credentials)
                    @test_throws Exception ManualResumeExample.resume(store, blocked; credentials)
                    @test CloudStore.get(store, key; credentials, allowMultipart=false) == expected
                    if provider == :s3
                        blocked_state = TOML.parsefile(blocked)
                        @test S3.abortmultipartupload(store, key, blocked_state["upload_id"]; credentials) === nothing
                        @test CloudStore.get(store, key; credentials, allowMultipart=false) == expected
                    else
                        a, b = base64encode("mixed-01"), base64encode("mixed-02")
                        Blobs.stageblock(store, "mixed", a, UInt8[1]; credentials)
                        Blobs.stageblock(store, "mixed", b, UInt8[2]; credentials)
                        Blobs.commitblocks(store, "mixed", [(id=a, state=:latest), (id=b, state=:latest)]; credentials)
                        Blobs.stageblock(store, "mixed", a, UInt8[3, 4]; credentials)
                        inventory = Blobs.listblocks(store, "mixed"; credentials)
                        @test inventory.committed == [(id=a, size=1), (id=b, size=1)]
                        @test inventory.uncommitted == [(id=a, size=2)]
                        @test inventory.etag !== nothing
                        Blobs.commitblocks(store, "mixed", [(id=b, state=:committed), (id=a, state=:uncommitted), (id=b, state=:committed)]; credentials)
                        @test CloudStore.get(store, "mixed"; credentials) == UInt8[2, 3, 4, 2]
                        @test_throws Exception Blobs.commitblocks(store, "mixed", [(id=a, state=:uncommitted)]; credentials)
                        @test CloudStore.get(store, "mixed"; credentials) == UInt8[2, 3, 4, 2]
                        Blobs.commitblocks(store, "mixed", [(id=a, state=:latest)]; credentials)
                        @test CloudStore.get(store, "mixed"; credentials) == UInt8[3, 4]
                        Blobs.commitblocks(store, "empty", []; credentials)
                        @test isempty(CloudStore.get(store, "empty"; credentials))
                    end
                end
            end
        end
    end
end
