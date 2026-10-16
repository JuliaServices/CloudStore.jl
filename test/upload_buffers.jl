module UploadBufferTests

using Test, CloudStore, CodecZlib
using CloudBase.CloudTest: Minio, Azurite
import CloudBase
import HTTP, Sockets
const API = CloudStore.API

mutable struct UploadStore{F} <: CloudBase.AbstractStore
    baseurl::String
    lock::ReentrantLock
    hook::F
    parts::Dict{Int,Vector{UInt8}}
    buffers::Dict{Int,AbstractVector{UInt8}}
    finished::Channel{Int}
    active::Int
    completed::Bool
    aborted::Bool
    active_at_abort::Int
end

UploadStore(hook=(part, n) -> nothing) = UploadStore(
    "https://upload.example/", ReentrantLock(), hook,
    Dict{Int,Vector{UInt8}}(), Dict{Int,AbstractVector{UInt8}}(),
    Channel{Int}(1024), 0, false, false, -1,
)

API.startMultipartUpload(::UploadStore, key; kw...) = nothing
function API.uploadPart(store::UploadStore, url, part, n, state; kw...)
    lock(store.lock) do
        store.active += 1
        store.buffers[n] = part
    end
    try
        store.hook(part, n)
        lock(store.lock) do
            store.parts[n] = Vector{UInt8}(part)
        end
        return ("part-$n", length(part))
    finally
        lock(store.lock) do
            store.active -= 1
        end
        put!(store.finished, n)
    end
end
function API.completeMultipartUpload(store::UploadStore, url, tags, state; kw...)
    @test store.active == 0
    @test tags == ["part-$n" for n in 1:length(store.parts)]
    store.completed = true
    return "complete"
end
function API.abortMultipartUpload(store::UploadStore, url, state; kw...)
    store.active_at_abort = store.active
    store.aborted = true
    return nothing
end

uploaded(store) = reduce(vcat, (store.parts[n] for n in 1:length(store.parts)); init=UInt8[])

function takeevent(channel)
    timedwait(() -> isready(channel), 10) == :ok || error("upload test event timed out")
    return take!(channel)
end

mutable struct CustomInput <: IO
    data::IOBuffer
    chunk::Int
    reads::Int
    fail_at::Int
end
CustomInput(data; chunk=3, fail_at=0) = CustomInput(IOBuffer(data), chunk, 0, fail_at)
Base.eof(io::CustomInput) = eof(io.data)
Base.bytesavailable(io::CustomInput) = bytesavailable(io.data)
Base.isopen(io::CustomInput) = isopen(io.data)
function Base.read(io::CustomInput, n::Integer)
    io.reads += 1
    io.reads == io.fail_at && error("source read failure")
    return read(io.data, min(n, io.chunk))
end

struct FragmentedInput <: IO
    data::IOBuffer
    reads::Base.RefValue{Int}
    fail_at::Int
end
FragmentedInput(data; fail_at=0) = FragmentedInput(data, Ref(0), fail_at)
Base.eof(io::FragmentedInput) = eof(io.data)
Base.bytesavailable(io::FragmentedInput) = min(3, bytesavailable(io.data))
Base.read(io::FragmentedInput, ::Type{UInt8}) = read(io.data, UInt8)
function Base.unsafe_read(io::FragmentedInput, p::Ptr{UInt8}, n::UInt)
    io.reads[] += 1
    io.reads[] == io.fail_at && error("fragmented source failure")
    return unsafe_read(io.data, p, n)
end
Base.isopen(io::FragmentedInput) = isopen(io.data)
Base.close(io::FragmentedInput) = close(io.data)

@testset "Multipart read buffers" begin
    @testset "Bounded slots, final lengths, and compression" begin
        for n in (0, 1, 3, 4, 5, 8, 9, 17), (compress, zlibng) in ((false, false), (true, false), (true, true))
            data = UInt8.(0:n-1)
            mktemp() do path, input
                write(input, data)
                seekstart(input)
                store = UploadStore()
                obj = API.putObjectImpl(store, "data", input;
                    multipartThreshold=-1, partSize=4, batchSize=2, compress, zlibng)
                wire = uploaded(store)
                @test (compress ? transcode(GzipDecompressor, wire) : wire) == data
                @test obj.size == length(wire)
                @test store.completed && !store.aborted
                @test isopen(input)
                @test all(part -> !isempty(part), values(store.parts))
                @test all(part -> length(part) <= 4, values(store.parts))
                # Later batches must use the same two underlying buffers.
                @test all(i -> Base.mightalias(store.buffers[i], store.buffers[mod1(i, 2)]),
                    1:length(store.buffers))
            end
        end
    end

    @testset "Read-to-end part size" begin
        for (n, offset) in ((0, 0), (1, 0), (4097, 3)), (compress, zlibng) in ((false, false), (true, false), (true, true))
            data = rand(UInt8, n)
            mktemp() do path, input
                write(input, data)
                seek(input, offset)
                store = UploadStore()
                obj = API.putObjectImpl(store, "read-to-end", input;
                    multipartThreshold=-1, partSize=typemax(Int), batchSize=2, compress, zlibng)
                wire = uploaded(store)
                @test (compress ? transcode(GzipDecompressor, wire) : wire) == data[offset+1:end]
                @test length(store.parts) == (compress || n > offset ? 1 : 0)
                @test obj.size == length(wire)
                @test position(input) == n
                @test isopen(input)
            end
        end
    end

    @testset "File names and nonzero input positions" begin
        data = collect(UInt8(0):UInt8(16))
        mktemp() do path, input
            write(input, data)
            flush(input)
            store = UploadStore()
            API.putObjectImpl(store, "path", path; multipartThreshold=1, partSize=4, batchSize=3)
            @test uploaded(store) == data
            @test all(i -> Base.mightalias(store.buffers[i], store.buffers[mod1(i, 3)]), 1:5)
            seek(input, 3)
            store = UploadStore()
            API.putObjectImpl(store, "position", input; multipartThreshold=1, partSize=4, batchSize=1)
            @test uploaded(store) == data[4:end]
            @test position(input) == length(data)
            @test isopen(input)
        end
    end

    @testset "Custom reads and borrowed inputs" begin
        data = collect(UInt8(0):UInt8(16))
        input = CustomInput(data)
        store = UploadStore()
        API.putObjectImpl(store, "custom", input; multipartThreshold=1, partSize=4, batchSize=2)
        @test uploaded(store) == data
        @test input.reads == 6
        @test isopen(input)
        @test all(part -> length(part) <= 3, values(store.parts))

        for input in (data, view(data, 2:16), IOBuffer(data))
            expected = input isa IO ? data : input
            store = UploadStore()
            API.putObjectImpl(store, "borrowed", input; multipartThreshold=1, partSize=4, batchSize=2)
            @test uploaded(store) == expected
            @test all(part -> Base.mightalias(part, data), values(store.buffers))
        end

        # TranscodingStream.readbytes! must fill each part across short input reads.
        input = FragmentedInput(IOBuffer(data))
        stream = API.TranscodingStreams.NoopStream(input)
        try
            store = UploadStore()
            API.putObjectImpl(store, "fragmented", stream; multipartThreshold=-1, partSize=4, batchSize=2)
            @test uploaded(store) == data
            @test length.(getindex.(Ref(store.parts), 1:5)) == [4, 4, 4, 4, 1]
            @test isopen(input)
        finally
            close(stream)
        end
    end

    @testset "Every upload finishes before a batch buffer is reused" begin
        data = collect(UInt8(0):UInt8(15))
        mktemp() do path, input
            write(input, data)
            seekstart(input)
            started = Channel{Tuple{Int,AbstractVector{UInt8}}}(4)
            gates = [Channel{Nothing}(1) for _ in 1:4]
            store = UploadStore() do part, n
                put!(started, (n, part))
                take!(gates[n])
            end
            task = @async API.putObjectImpl(store, "blocked", input;
                multipartThreshold=1, partSize=4, batchSize=2)
            try
                first_batch = Dict([takeevent(started), takeevent(started)])
                put!(gates[1], nothing)
                @test takeevent(store.finished) == 1
                @test !isready(started)
                @test position(input) == 8
                @test first_batch[1] == data[1:4]
                @test first_batch[2] == data[5:8]
                GC.gc()
                put!(gates[2], nothing)
                second_batch = Dict([takeevent(started), takeevent(started)])
                @test Base.mightalias(first_batch[1], second_batch[3])
                @test Base.mightalias(first_batch[2], second_batch[4])
                @test second_batch[3] == data[9:12]
                @test second_batch[4] == data[13:16]
                put!(gates[3], nothing)
                put!(gates[4], nothing)
                fetch(task)
                @test uploaded(store) == data
                @test store.completed && !store.aborted
            finally
                foreach(gate -> isopen(gate) && close(gate), gates)
                try
                    wait(task)
                catch
                end
            end
        end
    end

    @testset "Read, upload, and progress failures drain before abort" begin
        data = collect(UInt8(0):UInt8(16))
        input = CustomInput(data; chunk=4, fail_at=3)
        store = UploadStore()
        @test_throws ErrorException API.putObjectImpl(store, "read-error", input;
            multipartThreshold=1, partSize=4, batchSize=2)
        @test store.aborted && store.active_at_abort == 0 && !store.completed
        @test sort!(collect(keys(store.parts))) == [1, 2]
        @test isopen(input)

        for zlibng in (false, true)
            # Preserve the dependency's error/close behavior across supported versions.
            reference = FragmentedInput(IOBuffer(data); fail_at=3)
            stream = API.compressorstream(zlibng)(reference; stop_on_end=true)
            expected_error = try
                try
                    read(stream)
                finally
                    close(stream)
                end
            catch err
                err
            end
            input = FragmentedInput(IOBuffer(data); fail_at=3)
            store = UploadStore()
            err = try
                API.putObjectImpl(store, "compressed-read-error", input;
                    multipartThreshold=-1, partSize=32, batchSize=2, compress=true, zlibng)
            catch err
                err
            end
            @test err isa Exception
            @test typeof(err) === typeof(expected_error)
            @test sprint(showerror, err) == sprint(showerror, expected_error)
            @test store.aborted && store.active_at_abort == 0 && !store.completed
            @test isopen(input) == isopen(reference)
        end

        mktemp() do path, input
            write(input, data)
            seekstart(input)
            started = Channel{Int}(2)
            gates = [Channel{Nothing}(1) for _ in 1:2]
            store = UploadStore() do part, n
                put!(started, n)
                take!(gates[n])
                n == 1 && error("upload retry failure")
            end
            task = @async try
                API.putObjectImpl(store, "upload-error", input;
                    multipartThreshold=1, partSize=4, batchSize=2)
            catch err
                err
            end
            err = try
                @test Set((takeevent(started), takeevent(started))) == Set((1, 2))
                put!(gates[1], nothing)
                @test takeevent(store.finished) == 1
                @test !store.aborted
                @test position(input) == 8
                put!(gates[2], nothing)
                fetch(task)
            finally
                foreach(gate -> isopen(gate) && close(gate), gates)
                wait(task)
            end
            @test occursin("upload retry failure", sprint(showerror, err))
            @test store.aborted && store.active_at_abort == 0 && !store.completed
            @test isopen(input)

            seekstart(input)
            store = UploadStore()
            marker = ErrorException("progress failure")
            err = try
                API.putObjectImpl(store, "progress-error", input;
                    multipartThreshold=1, partSize=4, batchSize=2,
                    progress=(total, written) -> throw(marker))
            catch err
                err
            end
            @test err === marker
            @test store.aborted && store.active_at_abort == 0 && !store.completed
            @test position(input) == 8
            @test isopen(input)
        end
    end

    @testset "HTTP part retries keep exact bytes" begin
        port, listener = Sockets.listenany(Sockets.ip"127.0.0.1", UInt16(0))
        close(listener)
        data = UInt8.(mod.(0:4098, 256))
        received = Dict{String,Dict{Int,Vector{Vector{UInt8}}}}()
        completed = Set{String}()
        aborted = Set{String}()
        hash_errors = String[]
        guard = ReentrantLock()
        server = HTTP.serve!("127.0.0.1", port; verbose=false) do request
            uri = HTTP.URI(request.target)
            path = String(uri.path)
            query = HTTP.queryparams(uri)
            if haskey(query, "uploads")
                return HTTP.Response(200, [], "<InitiateMultipartUploadResult><UploadId>fixture</UploadId></InitiateMultipartUploadResult>")
            elseif request.method == "DELETE"
                lock(guard) do
                    push!(aborted, path)
                end
                return HTTP.Response(204)
            elseif haskey(query, "partNumber") || get(query, "comp", "") == "block"
                n = haskey(query, "partNumber") ? parse(Int, query["partNumber"]) :
                    parse(Int, String(CloudBase.Base64.base64decode(query["blockid"]))) + 1
                bytes = Vector{UInt8}(request.body)
                digest = bytes2hex(CloudBase.SHA.sha256(bytes))
                declared = HTTP.header(request, "x-amz-content-sha256", digest)
                attempt = lock(guard) do
                    declared == digest || push!(hash_errors, path)
                    parts = get!(received, path, Dict{Int,Vector{Vector{UInt8}}}())
                    attempts = get!(parts, n, Vector{UInt8}[])
                    push!(attempts, bytes)
                    length(attempts)
                end
                return HTTP.Response(attempt == 1 ? 503 : 200,
                    ["ETag" => "\"$digest\"", "Retry-After" => "0"], UInt8[])
            end
            lock(guard) do
                push!(completed, path)
            end
            return HTTP.Response(200, ["ETag" => "\"complete\""],
                "<CompleteMultipartUploadResult><ETag>\"complete\"</ETag></CompleteMultipartUploadResult>")
        end
        host = "http://127.0.0.1:$port"
        options = isdefined(HTTP, :Client) ?
            (; client=HTTP.Client(), retry_bucket=HTTP.RetryBucket(backoff_scale_factor_ms=0, max_backoff_secs=0)) :
            (; retry_delays=[0.0])
        stores = ((CloudStore.S3.Bucket("uploads", "us-east-1"; host), CloudStore.AWS.Credentials("fixture", "fixture")),
            (CloudStore.Blobs.Container("uploads", "fixture"; host), CloudStore.Azure.Credentials("fixture", "Zml4dHVyZQ==")))
        try
            mktemp() do file, input
                write(input, data)
                flush(input)
                for (store, credentials) in stores
                    CloudStore.put(store, "retry", file; credentials,
                        multipartThreshold=1, partSize=1024, batchSize=2, retries=1,
                        require_ssl_verification=true, options...)
                    path = String(HTTP.URI(API.makeURL(store, "retry")).path)
                    @test path in completed
                    @test !(path in aborted)
                    @test sort!(collect(keys(received[path]))) == collect(1:5)
                    for n in 1:5
                        expected = data[(n - 1) * 1024 + 1:min(n * 1024, length(data))]
                        @test received[path][n] == [expected, expected]
                    end
                    @test_throws Exception CloudStore.put(store, "failure", file; credentials,
                        multipartThreshold=1, partSize=1024, batchSize=2, retries=0,
                        require_ssl_verification=true, options...)
                    path = String(HTTP.URI(API.makeURL(store, "failure")).path)
                    @test !(path in completed)
                    @test sort!(collect(keys(received[path]))) == [1, 2]
                    @test (path in aborted) == (store isa CloudStore.S3.Bucket)
                end
            end
            @test isempty(hash_errors)
        finally
            haskey(options, :client) && close(options.client)
            close(server)
        end
    end

    @testset "Signed file and compressed uploads across batches" begin
        # Five parts exercise reuse in three batches; S3 requires 5 MiB nonfinal parts.
        data = rand(UInt8, 20 * 1024^2 + 7)
        mktemp() do file, input
            write(input, data)
            flush(input)
            for emulator in (Minio, Azurite)
                emulator.with(; debug=true) do conf
                    credentials, store = conf
                    options = if isdefined(HTTP, :Client)
                        # Azurite's self-signed certificate is used only on loopback.
                        transport = emulator === Azurite ? HTTP.Transport(tls_config=HTTP.TLS.Config(verify_peer=false, verify_hostname=false)) : HTTP.Transport()
                        (; client=HTTP.Client(; transport), require_ssl_verification=true)
                    else
                        (;)
                    end
                    try
                        for (compress, zlibng) in ((false, false), (true, false), (true, true))
                            attempts = (Threads.Atomic{Int}(0), Threads.Atomic{Int}(0))
                            ispart = function(req)
                                req.method == "PUT" || return false
                                query = HTTP.queryparams(HTTP.URI(req.target))
                                return haskey(query, "partNumber") || get(query, "comp", "") == "block"
                            end
                            replay = if isdefined(HTTP, :Client)
                                trace = function(ev)
                                    ev isa HTTP.RequestEvent && ispart(ev.request) || return nothing
                                    Threads.atomic_add!(attempts[ev.attempt], 1)
                                    return nothing
                                end
                                retry_if = (attempt, err, req, resp) -> attempt == 1 && resp !== nothing && ispart(req)
                                (; trace, retry_if, retries=1,
                                    retry_bucket=HTTP.RetryBucket(backoff_scale_factor_ms=0, max_backoff_secs=0))
                            else
                                (;)
                            end
                            CloudStore.put(store, "file", file; credentials, compress, zlibng,
                                multipartThreshold=1, partSize=5 * 1024^2, batchSize=2, options..., replay...)
                            @test CloudStore.get(store, "file"; credentials, decompress=compress,
                                allowMultipart=false, options...) == data
                            @test isopen(input)
                            if isdefined(HTTP, :Client)
                                @test attempts[1][] == attempts[2][] == 5
                            end
                        end
                    finally
                        haskey(options, :client) && close(options.client)
                    end
                end
            end
        end
    end
end

end # module
