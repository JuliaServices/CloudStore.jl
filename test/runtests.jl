using Test, CloudStore, CloudBase.CloudTest
import CloudBase
import CloudStore: S3, Blobs
using CodecZlib
import HTTP
import Sockets
using HTTP: ConnectError, StatusError
using Sockets: DNSError, IPv4, listenany
using ExceptionUnwrapping: unwrap_exception

bytes(x) = codeunits(x)

function header_value(headers, name)
    for (key, value) in headers
        lowercase(key) == lowercase(name) && return value
    end
    return ""
end

function stringfile(x)
    path, io = mktemp()
    write(io, x)
    close(io)
    return path
end

iobuffer(x) = IOBuffer(x)

function iofile(x)
    io = TempFile()
    write(io, x)
    seekstart(io)
    return io
end

reset!(x::IO) = seekstart(x)
reset!(x) = nothing

resetOut!(x::IO) = truncate(x, 0)
resetOut!(x) = nothing

outType(_, ::Nothing) = nothing
outType(csv, ::Type{Vector{UInt8}}) = zeros(UInt8, sizeof(csv))
outType(_, ::Type{String}) = tempname()
outType(_, ::Type{IO}) = IOBuffer()

cleanup!(x::Union{String, TempFile}) = rm(x)
cleanup!(x::IO) = close(x)
cleanup!(x) = nothing

check(x::AbstractVector{UInt8}, y::AbstractVector{UInt8}) = x == y
check(x::AbstractVector{UInt8}, y::String) = x == read(y)
check(x::AbstractVector{UInt8}, y::IO) = begin; reset!(y); z = x == read(y); reset!(y); z end
check(x::String, y::AbstractVector{UInt8}) = read(x) == y
check(x::IO, y::AbstractVector{UInt8}) = begin; reset!(x); z = read(x) == y; reset!(x); z end
check(x, y) = begin; reset!(x); reset!(y); z = read(x) == read(y); reset!(x); reset!(y); z end

function is_dns_error(e, depth::Int=0)
    depth > 8 && return false
    e isa DNSError && return true
    if isdefined(HTTP, :DNSError) && e isa getproperty(HTTP, :DNSError)
        return true
    end
    for field in (:error, :cause, :err, :ex)
        hasproperty(e, field) || continue
        inner = getproperty(e, field)
        inner === e && continue
        is_dns_error(inner, depth + 1) && return true
    end
    return false
end

mutable struct RecordingStore <: CloudBase.AbstractStore
    baseurl::String
    lock::ReentrantLock
    parts::Dict{Int,Vector{UInt8}}
    completed_tags::Vector{String}
    fail_part::Union{Nothing,Int}
    aborted::Bool
end

RecordingStore(; fail_part=nothing) = RecordingStore(
    "https://recording.example/",
    ReentrantLock(),
    Dict{Int,Vector{UInt8}}(),
    String[],
    fail_part,
    false,
)

struct RecordingDownloadStore <: CloudBase.AbstractStore
    baseurl::String
    data::Vector{UInt8}
end

RecordingDownloadStore(data) = RecordingDownloadStore(
    "https://recording.example/", Vector{UInt8}(data))

function CloudStore.API.headObject(store::RecordingDownloadStore, url, _headers; kw...)
    request = HTTP.Request("HEAD", url)
    return HTTP.Response(
        200,
        ["Content-Length" => string(length(store.data))],
        UInt8[];
        request,
    )
end

function CloudStore.API.getObject(
    store::RecordingDownloadStore,
    url,
    headers;
    response_stream=nothing,
    kw...,
)
    range = HTTP.header(headers, "Range", "bytes=0-$(length(store.data) - 1)")
    match_result = match(r"bytes=(\d+)-(\d+)", range)
    first_byte = parse(Int, match_result[1]) + 1
    last_byte = parse(Int, match_result[2]) + 1
    part = store.data[first_byte:last_byte]
    if response_stream !== nothing
        copyto!(response_stream, part)
    end
    request = HTTP.Request("GET", url)
    return HTTP.Response(
        206,
        ["Content-Length" => string(length(part))],
        response_stream === nothing ? part : response_stream;
        request,
    )
end

@testset "object key URL encoding" begin
    store = RecordingStore()
    @test CloudStore.API.makeURL(store, "plain") == "https://recording.example/plain"
    @test CloudStore.API.makeURL(store, "nested/key") == "https://recording.example/nested/key"
    @test CloudStore.API.makeURL(store, "nested//key") == "https://recording.example/nested//key"
    @test CloudStore.API.makeURL(store, "with space") == "https://recording.example/with%20space"
    @test CloudStore.API.makeURL(store, "with%20space") == "https://recording.example/with%2520space"
    @test CloudStore.API.makeURL(store, "plus+plus") == "https://recording.example/plus%2Bplus"
    @test CloudStore.API.makeURL(store, "hash#hash") == "https://recording.example/hash%23hash"
    @test CloudStore.API.makeURL(store, "unicode-ü") == "https://recording.example/unicode-%C3%BC"
    @test CloudStore.API.makeURL(store, "literal?mark") == "https://recording.example/literal%3Fmark"
    signed = CloudStore.API.parsedURLResource("key?X-Amz-Signature=a%2Fb&partNumber=1")
    @test CloudStore.API.makeURL(store, signed) ==
        "https://recording.example/key?X-Amz-Signature=a%2Fb&partNumber=1"
end

@testset "signed URL request targets" begin
    port, socket = listenany(IPv4(0), 20_000)
    close(socket)
    targets = Channel{String}(4)
    server = HTTP.serve!(port; verbose=false) do request
        put!(targets, request.target)
        return HTTP.Response(200, ["Content-Length" => "2"], "ok")
    end
    try
        s3_query = "X-Amz-Signature=a%2Fb&X-Amz-SignedHeaders=host"
        @test S3.get(
            "http://127.0.0.1:$port/bucket-name/key?$s3_query";
            parseLocal=true,
            nowarn=true,
            allowMultipart=false,
        ) == b"ok"
        @test take!(targets) == "/bucket-name/key?$s3_query"
        @test S3.exists(
            "http://127.0.0.1:$port/bucket-name/key?$s3_query";
            parseLocal=true,
            nowarn=true,
        )
        @test take!(targets) == "/bucket-name/key?$s3_query"

        azure_query = "sv=2023-11-03&sig=c%2Fd&sp=r"
        @test Blobs.get(
            "azure://127.0.0.1:$port/account/container/blob?$azure_query";
            parseLocal=true,
            allowMultipart=false,
        ) == b"ok"
        @test take!(targets) == "/account/container/blob?$azure_query"
        @test Blobs.exists(
            "azure://127.0.0.1:$port/account/container/blob?$azure_query";
            parseLocal=true,
        )
        @test take!(targets) == "/account/container/blob?$azure_query"
    finally
        close(server)
    end
end

@testset "object existence" begin
    port, socket = Sockets.listenany(Sockets.IPv4(0), 20_000)
    close(socket)
    server = HTTP.serve!(port; verbose=false) do request
        status = endswith(request.target, "/present") ? 200 :
            endswith(request.target, "/missing") ? 404 : 403
        return HTTP.Response(status)
    end
    try
        bucket = S3.Bucket("bucket-name", "us-east-1"; host="http://127.0.0.1:$port")
        @test S3.exists(bucket, "present")
        @test !S3.exists(bucket, "missing")
        @test_throws StatusError S3.exists(bucket, "forbidden")
        @test CloudStore.exists(bucket, "present")

        container = Blobs.Container("container", "account"; host="http://127.0.0.1:$port")
        @test Blobs.exists(container, "present")
        @test !Blobs.exists(container, "missing")
        @test_throws StatusError Blobs.exists(container, "forbidden")
        @test CloudStore.exists(container, "present")

        object = CloudStore.Object(bucket, nothing, "present", 0, "")
        @test CloudStore.exists(object)
    finally
        close(server)
    end
end

CloudStore.API.startMultipartUpload(::RecordingStore, _key; kw...) = nothing

function CloudStore.API.putObject(store::RecordingStore, key, body; kw...)
    store.parts[0] = Vector{UInt8}(body)
    request = HTTP.Request("PUT", CloudStore.API.makeURL(store, key))
    return HTTP.Response(200, ["ETag" => "single-etag"], UInt8[]; request)
end

function CloudStore.API.uploadPart(store::RecordingStore, _url, part, part_number, _state; kw...)
    part_number == store.fail_part && error("synthetic upload failure")
    bytes = Vector{UInt8}(part)
    lock(store.lock) do
        store.parts[part_number] = bytes
    end
    return ("etag-$part_number", length(bytes))
end

function CloudStore.API.completeMultipartUpload(store::RecordingStore, _url, tags, _state; kw...)
    store.completed_tags = copy(tags)
    return "complete"
end

function CloudStore.API.abortMultipartUpload(store::RecordingStore, _url, _state; kw...)
    store.aborted = true
    return nothing
end

@testset "transfer progress callbacks" begin
    data = collect(UInt8(1):UInt8(10))
    download_updates = Tuple{Int,Int}[]
    result = CloudStore.API.getObjectImpl(
        RecordingDownloadStore(data),
        "data.bin";
        multipartThreshold=1,
        partSize=4,
        batchSize=2,
        progress=(total, transferred) -> push!(download_updates, (total, transferred)),
    )
    @test result == data
    @test download_updates == [(10, 8), (10, 10)]

    single_updates = Tuple{Int,Int}[]
    result = CloudStore.API.getObjectImpl(
        RecordingDownloadStore(data),
        "data.bin";
        allowMultipart=false,
        progress=(total, transferred) -> push!(single_updates, (total, transferred)),
    )
    @test result == data
    @test single_updates == [(10, 10)]

    upload_data = collect(UInt8(1):UInt8(16))
    upload_updates = Tuple{Int,Int}[]
    CloudStore.API.putObjectImpl(
        RecordingStore(),
        "data.bin",
        upload_data;
        multipartThreshold=1,
        partSize=4,
        batchSize=2,
        progress=(total, transferred) -> push!(upload_updates, (total, transferred)),
    )
    @test upload_updates == [(16, 4), (16, 8), (16, 12), (16, 16)]

    single_upload_updates = Tuple{Int,Int}[]
    single_store = RecordingStore()
    CloudStore.API.putObjectImpl(
        single_store,
        "single.bin",
        upload_data;
        allowMultipart=false,
        progress=(total, transferred) -> push!(single_upload_updates, (total, transferred)),
    )
    @test single_store.parts[0] == upload_data
    @test single_upload_updates == [(16, 16)]
end

@testset "prepBody IOBuffer bounds" begin
    API = CloudStore.API
    # `.data` is the buffer's allocated capacity, not its contents. Only the bytes in
    # [ptr, size] are real data; the rest of the allocation is whatever was there before.
    written() = (io = IOBuffer(); write(io, "hello world"); io)

    # a buffer written to and not rewound has nothing left to read, matching `nbytes`
    io = written()
    @test API.nbytes(io) == 0
    @test API.prepBody(io, false, false) == UInt8[]

    # rewound, it yields exactly the written bytes - not the padded capacity
    io = written(); seek(io, 0)
    body = API.prepBody(io, false, false)
    @test length(body) == 11
    @test String(copy(body)) == "hello world"
    @test length(io.data) > 11   # capacity really is larger, so this is a real bound

    # the read position is respected
    io = written(); seek(io, 6)
    @test String(copy(API.prepBody(io, false, false))) == "world"

    # compression works for buffers whose data is a Memory (Julia 1.11+), which
    # transcode does not accept directly
    io = written(); seek(io, 0)
    compressed = API.prepBody(io, true, false)
    @test !isempty(compressed)
    @test transcode(GzipDecompressor, Vector{UInt8}(compressed)) == Vector{UInt8}(codeunits("hello world"))

    # read-mode buffers over existing data are unchanged, and stay zero-copy
    data = collect(codeunits("hello world"))
    io = IOBuffer(data)
    @test API.prepBody(io, false, false) == data

    # an empty buffer
    @test API.prepBody(IOBuffer(), false, false) == UInt8[]

    @test API.iobufferbytes(written()) == UInt8[]
    let io = written()
        seek(io, 0)
        @test String(copy(API.iobufferbytes(io))) == "hello world"
    end
end

@testset "compressed multipart upload completeness and cleanup" begin
    API = CloudStore.API
    # Gzip framing makes this byte pattern larger than its source. The upload must
    # therefore emit more parts than cld(length(data), partSize).
    data = collect(UInt8(0):UInt8(255))
    input = IOBuffer(data)
    store = RecordingStore()
    obj = API.putObjectImpl(
        store,
        "incompressible.bin",
        input;
        multipartThreshold=1,
        partSize=64,
        batchSize=2,
        compress=true,
    )

    uploaded = reduce(vcat, (store.parts[i] for i in sort!(collect(keys(store.parts)))))
    @test length(uploaded) > length(data)
    @test transcode(GzipDecompressor, uploaded) == data
    @test length(store.parts) > cld(length(data), 64)
    @test store.completed_tags == ["etag-$i" for i in 1:length(store.parts)]
    @test obj.size == length(data)
    @test isopen(input)
    @test !store.aborted

    # A failed early part must not leave later workers blocked waiting for its tag,
    # and cleanup must still leave caller-owned IO usable.
    failing_input = IOBuffer(data)
    failing_store = RecordingStore(fail_part=1)
    err = try
        API.putObjectImpl(
            failing_store,
            "failure.bin",
            failing_input;
            multipartThreshold=1,
            partSize=64,
            batchSize=2,
            compress=true,
        )
        nothing
    catch e
        e
    end
    @test err !== nothing
    @test occursin("synthetic upload failure", sprint(showerror, err))
    @test isopen(failing_input)
    @test failing_store.aborted
end

@testset "CloudStore.jl" begin
@testset "S3" begin
    # conf, p = Minio.run(; debug=true)
    Minio.with(; debug=true) do conf
        credentials, bucket = conf
        empty = ""
        csv = "a,b,c\n1,2,3\n4,5,$(rand())"
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20MB
        for inBody in (bytes, stringfile, iobuffer, iofile)
            for outBody in (nothing, Vector{UInt8}, String, IO)
                body = inBody(csv)
                out = outType(csv, outBody)
                println("in: $inBody, out: $outBody, single part, no compression")
                obj = S3.put(bucket, "test.csv", body; contentType="text/csv", credentials)
                data = S3.get(bucket, "test.csv", out; objectMaxSize=sizeof(csv), credentials)
                @test check(body, data)
                resetOut!(out)
                # get on Object
                data = S3.get(obj, out; credentials)
                @test check(body, data)
                resetOut!(out)
                # object metadata
                meta = S3.head(bucket, "test.csv"; credentials)
                @test meta isa Dict && !isempty(meta)
                @test header_value(meta, "Content-Type") == "text/csv"

                # list
                objs = S3.list(bucket; credentials)
                @test length(objs) == 1
                @test objs[1].key == "test.csv"

                println("in: $inBody, out: $outBody, single part, compression")
                obj = S3.put(bucket, "test2.csv", body; compress=true, credentials)
                if outBody == Vector{UInt8}
                    # throws an error because compressed data is larger than original data
                    @test_throws ArgumentError S3.get(bucket, "test2.csv", out; decompress=true, credentials)
                    data = S3.get(bucket, "test2.csv", zeros(UInt8, 100); decompress=true, credentials)
                else
                    data = S3.get(bucket, "test2.csv", out; decompress=true, credentials)
                end
                @test check(body, data)
                resetOut!(out)

                # passing urls directly
                url = "$(bucket.baseurl)test5.csv"
                obj = S3.put(url, body; parseLocal=true, credentials)
                data = S3.get(url, out; parseLocal=true, credentials)
                @test check(body, data)
                resetOut!(out)
                cleanup!(body)

                # 0 byte file
                ebody = inBody(empty)
                out = outType(empty, outBody)
                obj = S3.put(bucket, "test6.csv", ebody; credentials)
                data = S3.get(bucket, "test6.csv", out; credentials)
                @test check(ebody, data)
                resetOut!(out)
                cleanup!(ebody)

                mbody = inBody(multicsv);
                out = outType(multicsv, outBody)
                println("in: $inBody, out: $outBody, multipart, no compression")
                obj = S3.put(bucket, "test3.csv", mbody; contentType="text/csv", multipartThreshold=5_000_000, partSize=5_500_000, lograte=true, credentials)
                data = S3.get(bucket, "test3.csv", out; objectMaxSize=sizeof(multicsv), lograte=true, credentials)
                @test check(mbody, data)
                meta = S3.head(bucket, "test3.csv"; credentials)
                @test header_value(meta, "Content-Type") == "text/csv"
                resetOut!(out)
                println("in: $inBody, out: $outBody, multipart, compression")
                obj = S3.put(bucket, "test4.csv", mbody; compress=true, zlibng=true, multipartThreshold=5_000_000, partSize=5_500_000, credentials)
                data = S3.get(bucket, "test4.csv", out; decompress=true, zlibng=true, credentials)
                @test check(mbody, data)
                resetOut!(out)
                cleanup!(mbody)

                # list
                objs = S3.list(bucket; credentials)
                @test map(x -> x.key, objs) == ["test.csv", "test2.csv", "test3.csv", "test4.csv", "test5.csv", "test6.csv"]
                objs = S3.list(bucket; maxKeys=1, credentials)
                @test map(x -> x.key, objs) == ["test.csv", "test2.csv", "test3.csv", "test4.csv", "test5.csv", "test6.csv"]

                # delete
                S3.delete(bucket, "test.csv"; credentials)
                S3.delete(bucket, "test2.csv"; credentials)
                S3.delete(bucket, "test3.csv"; credentials)
                S3.delete(bucket, "test4.csv"; credentials)
                S3.delete(bucket, "test5.csv"; credentials)
                S3.delete(bucket, "test6.csv"; credentials)

                objs = S3.list(bucket; credentials)
                @test length(objs) == 0
            end
        end
    end

    @testset "Exceptions" begin
        # conf, p = Minio.run(; debug=true)
        Minio.with(; debug=true) do conf
            credentials, bucket = conf
            global _stale_bucket = bucket
            csv = "a,b,c\n1,2,3\n4,5"
            obj = S3.put(bucket, "test.csv", bytes(csv); credentials)
            @assert obj.size == sizeof(csv)

            @testset "Insufficient output buffer size" begin
                out = zeros(UInt8, sizeof(csv) - 1)
                try
                    S3.get(bucket, "test.csv", out; credentials, allowMultipart=false) # single request
                    @test false # Should have thrown an error
                catch e
                    @test e isa ArgumentError
                    @test e.msg == "Unable to grow response stream IOBuffer $(sizeof(out)) large enough for response body size: $(sizeof(csv))"
                end

                try
                    S3.get(bucket, "test.csv", out; credentials, allowMultipart=true, multipartThreshold=1) # multipart request
                    @test false # Should have thrown an error
                catch e
                    @test e isa ArgumentError
                    @test e.msg == "Unable to grow response stream IOBuffer $(sizeof(out)) large enough for response body size: $(sizeof(csv))"
                end
            end

            @testset "Missing credentials" begin
                try
                    S3.get(bucket, "test.csv") # single request
                    @test false # Should have thrown an error
                catch e
                    @test e isa StatusError
                    @test e.status == 403
                end

                try
                    S3.get(bucket, "test.csv"; allowMultipart=true, multipartThreshold=1) # multipart request
                    @test false # Should have thrown an error
                catch e
                    @test e isa StatusError
                    @test e.status == 403
                end

                try
                    S3.put(bucket, "test2.csv", bytes(csv))
                    @test false # Should have thrown an error
                catch e
                    @test e isa StatusError
                    @test e.status == 403
                end
            end

            @testset "Non-existing file" begin
                try
                    S3.get(bucket, "doesnt_exist.csv"; credentials, allowMultipart=false) # single request
                    @test false # Should have thrown an error
                catch e
                    @test e isa StatusError
                    @test e.status == 404
                end

                try
                    S3.get(bucket, "doesnt_exist.csv"; credentials, allowMultipart=true, multipartThreshold=1) # multipart request
                    @test false # Should have thrown an error
                catch e
                    @test e isa StatusError
                    @test e.status == 404
                end
            end

            @testset "Connection error: DNSError" begin
                non_existent_bucket_name = string(bucket.name, "doesntexist")
                non_existent_bucket = S3.Bucket(non_existent_bucket_name;
                    host="http://cloudstore-invalid-hostname-for-tests.invalid")
                try
                    S3.get(non_existent_bucket, "doesnt_exist.csv"; credentials)
                    @test false # Should have thrown an error
                catch e
                    @test e isa ConnectError || is_dns_error(e)
                    @test is_dns_error(e)
                end

                try
                    S3.put(non_existent_bucket, "doesnt_exist.csv", bytes(csv); credentials)
                    @test false # Should have thrown an error
                catch e
                    @test e isa ConnectError || is_dns_error(e)
                    @test is_dns_error(e)
                end
            end
        end
        # Minio doesn't run at this point
        @testset "Connection error: IOError" begin
            try
                S3.get(_stale_bucket, "doesnt_exist.csv")
                @test false # Should have thrown an error
            catch e
                @test e isa ConnectError
            end

            try
                S3.put(_stale_bucket, "doesnt_exist.csv", bytes("my,da,ta"))
                @test false # Should have thrown an error
            catch e
                @test e isa ConnectError
            end
        end
    end
end

@time @testset "Blobs" begin
    # conf, p = Azurite.run(; debug=true)
    Azurite.with(; debug=true) do conf
        credentials, container = conf
        empty = ""
        csv = "a,b,c\n1,2,3\n4,5,$(rand())"
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20MB
        for inBody in (bytes, stringfile, iobuffer, iofile)
            for outBody in (nothing, Vector{UInt8}, String, IO)
                body = inBody(csv)
                out = outType(csv, outBody)
                println("in: $inBody, out: $outBody, single part, no compression")
                obj = Blobs.put(container, "test.csv", body; contentType="text/csv", credentials)
                data = Blobs.get(container, "test.csv", out; credentials)
                @test check(body, data)
                resetOut!(out)
                # get on Object
                data = Blobs.get(obj, out; credentials)
                @test check(body, data)
                resetOut!(out)
                # object metadata
                meta = Blobs.head(container, "test.csv"; credentials)
                @test meta isa Dict && !isempty(meta)
                @test header_value(meta, "Content-Type") == "text/csv"

                # list
                objs = Blobs.list(container; credentials)
                @test length(objs) == 1
                @test objs[1].key == "test.csv"

                if outBody == Vector{UInt8}
                    @warn "Skipping compression test for Vector{UInt8} output"
                    obj = Blobs.put(container, "test2.csv", body; credentials)
                    data = Blobs.get(container, "test2.csv", out; credentials)
                    @test check(body, data)
                    resetOut!(out)
                else
                    obj = Blobs.put(container, "test2.csv", body; compress=true, credentials)
                    data = Blobs.get(container, "test2.csv", out; decompress=true, credentials)
                    @test check(body, data)
                    resetOut!(out)
                end

                # passing urls directly
                url = "$(container.baseurl)test5.csv"
                obj = Blobs.put(url, body; parseLocal=true, credentials)
                data = Blobs.get(url, out; parseLocal=true, credentials)
                @test check(body, data)
                resetOut!(out)
                cleanup!(body)

                # 0 byte file
                ebody = inBody(empty)
                out = outType(empty, outBody)
                obj = Blobs.put(container, "test6.csv", ebody; credentials)
                data = Blobs.get(container, "test6.csv", out; credentials)
                @test check(ebody, data)
                resetOut!(out)
                cleanup!(ebody)

                mbody = inBody(multicsv);
                out = outType(multicsv, outBody)
                println("in: $inBody, out: $outBody, multipart, no compression")
                obj = Blobs.put(container, "test3.csv", mbody; contentType="text/csv", multipartThreshold=5_000_000, partSize=5_500_000, credentials)
                data = Blobs.get(container, "test3.csv", out; credentials)
                @test check(mbody, data)
                meta = Blobs.head(container, "test3.csv"; credentials)
                @test header_value(meta, "Content-Type") == "text/csv"
                resetOut!(out)
                println("in: $inBody, out: $outBody, multipart, compression")
                obj = Blobs.put(container, "test4.csv", mbody; compress=true, multipartThreshold=5_000_000, partSize=5_500_000, credentials)
                data = Blobs.get(container, "test4.csv", out; decompress=true, credentials)
                @test check(mbody, data)
                resetOut!(out)
                cleanup!(mbody)

                # list
                objs = Blobs.list(container; credentials)
                @test map(x -> x.key, objs) == ["test.csv", "test2.csv", "test3.csv", "test4.csv", "test5.csv", "test6.csv"]
                objs = Blobs.list(container; maxKeys=1, credentials)
                @test map(x -> x.key, objs) == ["test.csv", "test2.csv", "test3.csv", "test4.csv", "test5.csv", "test6.csv"]

                # list with properties
                objs = Blobs.list(container; credentials, get_properties=true)
                @test length(objs) == 6
                for obj in objs
                    @test obj.properties["Properties"]["Creation-Time"] != ""
                end

                # delete
                Blobs.delete(container, "test.csv"; credentials)
                Blobs.delete(container, "test2.csv"; credentials)
                Blobs.delete(container, "test3.csv"; credentials)
                Blobs.delete(container, "test4.csv"; credentials)
                Blobs.delete(container, "test5.csv"; credentials)
                Blobs.delete(container, "test6.csv"; credentials)

                objs = Blobs.list(container; credentials)
                @test length(objs) == 0
            end
        end
    end

    @testset "Exceptions" begin
        # conf, p = Azurite.run(; debug=true)
        Azurite.with(; debug=true) do conf
            credentials, container = conf
            global _stale_container = container
            csv = "a,b,c\n1,2,3\n4,5"
            obj = Blobs.put(container, "test.csv", bytes(csv); credentials)
            @assert obj.size == sizeof(csv)

            @testset "Insufficient output buffer size" begin
                out = zeros(UInt8, sizeof(csv) - 1)
                try
                    Blobs.get(container, "test.csv", out; credentials, allowMultipart=false) # single request
                    @test false # Should have thrown an error
                catch e
                    @test e isa ArgumentError
                    @test e.msg == "Unable to grow response stream IOBuffer $(sizeof(out)) large enough for response body size: $(sizeof(csv))"
                end

                try
                    Blobs.get(container, "test.csv", out; credentials, allowMultipart=true, multipartThreshold=1) # multipart request
                    @test false # Should have thrown an error
                catch e
                    @test e isa ArgumentError
                    @test e.msg == "Unable to grow response stream IOBuffer $(sizeof(out)) large enough for response body size: $(sizeof(csv))"
                end
            end

            @testset "Missing credentials" begin
                try
                    Blobs.get(container, "test.csv") # single request
                    @test false # Should have thrown an error
                catch e
                    @test e isa StatusError
                    @test e.status == 403
                end

                try
                    Blobs.get(container, "test.csv"; allowMultipart=true, multipartThreshold=1) # multipart request
                    @test false # Should have thrown an error
                catch e
                    @test e isa StatusError
                    @test e.status == 403
                end

                try
                    Blobs.put(container, "test2.csv", bytes(csv))
                    @test false # Should have thrown an error
                catch e
                    @test e isa StatusError
                    @test e.status == 403
                end
            end

            @testset "Non-existing file" begin
                try
                    Blobs.get(container, "doesnt_exist.csv"; credentials, allowMultipart=false) # single request
                    @test false # Should have thrown an error
                catch e
                    @test e isa StatusError
                    @test e.status == 404
                end

                try
                    Blobs.get(container, "doesnt_exist.csv"; credentials, allowMultipart=true, multipartThreshold=1) # multipart request
                    @test false # Should have thrown an error
                catch e
                    @test e isa StatusError
                    @test e.status == 404
                end
            end

            @testset "Connection error: DNSError" begin
                non_existent_container_name = string(container.name, "doesntexist")
                non_existent_container = Blobs.Container(
                    non_existent_container_name,
                    "account";
                    host="http://cloudstore-invalid-hostname-for-tests.invalid",
                )
                try
                    Blobs.get(non_existent_container, "doesnt_exist.csv"; credentials)
                    @test false # Should have thrown an error
                catch e
                    @test e isa ConnectError || is_dns_error(e)
                    @test is_dns_error(e)
                end

                try
                    Blobs.put(non_existent_container, "doesnt_exist.csv", bytes(csv); credentials)
                    @test false # Should have thrown an error
                catch e
                    @test e isa ConnectError || is_dns_error(e)
                    @test is_dns_error(e)
                end
            end
        end
        # Azurite is not running at this point
        @testset "Connection error: IOError" begin
            try
                Blobs.get(_stale_container, "doesnt_exist.csv")
                @test false # Should have thrown an error
            catch e
                @test e isa ConnectError
            end

            try
                Blobs.put(_stale_container, "doesnt_exist.csv", bytes("my,da,ta"))
                @test false # Should have thrown an error
            catch e
                @test e isa ConnectError
            end
        end
    end
end

@testset "URL Parsing Unit Tests" begin
    azure = [
        ("https://myaccount.blob.core.windows.net/mycontainer/myblob", (true, nothing, "myaccount", "mycontainer", "myblob")),
        ("https://myaccount.blob.core.windows.net/mycontainer", (true, nothing, "myaccount", "mycontainer", "")),
        ("azure://myaccount.blob.core.windows.net/mycontainer/myblob", (true, nothing, "myaccount", "mycontainer", "myblob")),
        ("azure://myaccount.blob.core.windows.net/mycontainer", (true, nothing, "myaccount", "mycontainer", "")),
        ("https://127.0.0.1:45942/myaccount/mycontainer", (true, "https://127.0.0.1:45942", "myaccount", "mycontainer", "")),
        ("https://127.0.0.1:45942/myaccount/mycontainer/myblob", (true, "https://127.0.0.1:45942", "myaccount", "mycontainer", "myblob")),
        ("azure://127.0.0.1:45942/myaccount/mycontainer", (true, "http://127.0.0.1:45942", "myaccount", "mycontainer", "")),
        ("azure://127.0.0.1:45942/myaccount/mycontainer/myblob", (true, "http://127.0.0.1:45942", "myaccount", "mycontainer", "myblob")),
        ("azure://myaccount", (true, nothing, "myaccount", "", "")),

        ("HTTPS://myaccount.BLOB.core.windows.net/mycontainer/myblob", (true, nothing, "myaccount", "mycontainer", "myblob")),
        ("httpS://myaccount.blob.CORE.windows.net/mycontainer", (true, nothing, "myaccount", "mycontainer", "")),
        ("AZURE://myaccount.blob.core.WINDOWS.net/mycontainer/myblob", (true, nothing, "myaccount", "mycontainer", "myblob")),
        ("azurE://myaccount.blob.core.windows.NET/mycontainer", (true, nothing, "myaccount", "mycontainer", "")),
        ("Https://127.0.0.1:45942/myaccount/mycontainer", (true, "Https://127.0.0.1:45942", "myaccount", "mycontainer", "")),
        ("hTTPs://127.0.0.1:45942/myaccount/mycontainer/myblob", (true, "hTTPs://127.0.0.1:45942", "myaccount", "mycontainer", "myblob")),
        ("Azure://127.0.0.1:45942/myaccount/mycontainer", (true, "http://127.0.0.1:45942", "myaccount", "mycontainer", "")),
        ("aZURe://127.0.0.1:45942/myaccount/mycontainer/myblob", (true, "http://127.0.0.1:45942", "myaccount", "mycontainer", "myblob")),
        ("Azure://myaccount", (true, nothing, "myaccount", "", ""))
    ]
    for (url, parts) in azure
        ok, host, account, container, blob = CloudStore.parseAzureAccountContainerBlob(url; parseLocal=true)
        @test ok
        @test host == parts[2]
        @test account == parts[3]
        @test container == parts[4]
        @test blob == parts[5]
    end


    azure_sas = "?sp=r&sig=$("a"^1500)"
    ok, host, account, container, blob = CloudStore.parseAzureAccountContainerBlob(
        "https://myaccount.blob.core.windows.net/mycontainer/myblob$azure_sas")
    @test (ok, host, account, container, blob) ==
        (true, nothing, "myaccount", "mycontainer", "myblob$azure_sas")

    s3 = [
        ("https://bucket-name.s3-accelerate.us-east-1.amazonaws.com/key-name", (true, true, nothing, "bucket-name", "us-east-1", "key-name")),
        ("https://bucket-name.s3-accelerate.us-east-1.amazonaws.com", (true, true, nothing, "bucket-name", "us-east-1", "")),
        ("https://bucket-name.s3-accelerate.amazonaws.com/key-name", (true, true, nothing, "bucket-name", "", "key-name")),
        ("https://bucket-name.s3-accelerate.amazonaws.com", (true, true, nothing, "bucket-name", "", "")),
        ("https://bucket-name.s3.us-east-1.amazonaws.com/key-name", (true, false, nothing, "bucket-name", "us-east-1", "key-name")),
        ("https://bucket-name.s3.us-east-1.amazonaws.com", (true, false, nothing, "bucket-name", "us-east-1", "")),
        ("https://bucket-name.s3.amazonaws.com/key-name", (true, false, nothing, "bucket-name", "", "key-name")),
        ("https://bucket-name.s3.amazonaws.com", (true, false, nothing, "bucket-name", "", "")),
        ("https://s3.us-east-1.amazonaws.com/bucket-name/key-name", (true, false, nothing, "bucket-name", "us-east-1", "key-name")),
        ("https://s3.us-east-1.amazonaws.com/bucket-name", (true, false, nothing, "bucket-name", "us-east-1", "")),
        ("https://s3.amazonaws.com/bucket-name/key-name", (true, false, nothing, "bucket-name", "", "key-name")),
        ("https://s3.amazonaws.com/bucket-name", (true, false, nothing, "bucket-name", "", "")),
        ("s3://bucket-name/key-name", (true, false, nothing, "bucket-name", "", "key-name")),
        ("s3://bucket-name", (true, false, nothing, "bucket-name", "", "")),
        ("http://127.0.0.1:27181/bucket-name/key-name", (true, false, "http://127.0.0.1:27181", "bucket-name", "", "key-name")),
        ("http://127.0.0.1:27181/bucket-name", (true, false, "http://127.0.0.1:27181", "bucket-name", "", "")),

        ("Https://bucket-name.s3-ACCELERATE.us-east-1.amazonaws.com/key-name", (true, true, nothing, "bucket-name", "us-east-1", "key-name")),
        ("HTTPS://bucket-name.s3-accelerate.us-east-1.AMAZONAWS.com", (true, true, nothing, "bucket-name", "us-east-1", "")),
        ("httpS://bucket-name.S3-ACCELERATE.AMAZONAWS.com/key-name", (true, true, nothing, "bucket-name", "", "key-name")),
        ("hTTPs://bucket-name.s3-accelerate.amazonaws.com", (true, true, nothing, "bucket-name", "", "")),
        ("HTTPs://bucket-name.s3.us-east-1.amazonaws.COM/key-name", (true, false, nothing, "bucket-name", "us-east-1", "key-name")),
        ("httpS://bucket-name.S3.us-east-1.AMAZONAWS.COM", (true, false, nothing, "bucket-name", "us-east-1", "")),
        ("HTTPs://bucket-name.S3.amazonaws.COM/key-name", (true, false, nothing, "bucket-name", "", "key-name")),
        ("hTTPS://bucket-name.S3.AMAZONAWS.COM", (true, false, nothing, "bucket-name", "", "")),
        ("hTTpS://s3.us-east-1.AMAZONAWS.com/bucket-name/key-name", (true, false, nothing, "bucket-name", "us-east-1", "key-name")),
        ("HTTPS://s3.us-east-1.amazonaws.COM/bucket-name", (true, false, nothing, "bucket-name", "us-east-1", "")),
        ("hTTPs://S3.AMAZONAWS.COM/bucket-name/key-name", (true, false, nothing, "bucket-name", "", "key-name")),
        ("httPS://S3.AmAzonAws.com/bucket-name", (true, false, nothing, "bucket-name", "", "")),
        ("S3://bucket-name/key-name", (true, false, nothing, "bucket-name", "", "key-name")),
        ("S3://bucket-name", (true, false, nothing, "bucket-name", "", "")),
        ("HTtp://127.0.0.1:27181/bucket-name/key-name", (true, false, "HTtp://127.0.0.1:27181", "bucket-name", "", "key-name")),
        ("htTP://127.0.0.1:27181/bucket-name", (true, false, "htTP://127.0.0.1:27181", "bucket-name", "", "")),

        ("https://bucket.vpce-1a2b3c4d-5e6f.s3.us-west-2.vpce.amazonaws.com/bucket-name", (true, false, "https://bucket.vpce-1a2b3c4d-5e6f.s3.us-west-2.vpce.amazonaws.com", "bucket-name", "us-west-2", "")),
        ("https://bucket.vpce-1a2b3c4d-5e6f.s3.us-west-2.vpce.amazonaws.com/bucket-name/key-name", (true, false, "https://bucket.vpce-1a2b3c4d-5e6f.s3.us-west-2.vpce.amazonaws.com", "bucket-name", "us-west-2", "key-name")),
    ]
    for (url, parts) in s3
        ok, accelerate, host, bucket, reg, key = CloudStore.parseAWSBucketRegionKey(url; parseLocal=true)
        @test ok
        @test accelerate == parts[2]
        @test host == parts[3]
        @test bucket == parts[4]
        @test reg == parts[5]
        @test key == parts[6]
    end


    aws_query = "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=$("b"^1500)"
    ok, accelerate, host, bucket, reg, key = CloudStore.parseAWSBucketRegionKey(
        "https://bucket-name.s3.us-east-1.amazonaws.com/key-name$aws_query")
    @test (ok, accelerate, host, bucket, reg, key) ==
        (true, false, nothing, "bucket-name", "us-east-1", "key-name$aws_query")

    # Only accept https, not http
    invalid_azure = [
        "http://myaccount.blob.core.windows.net/mycontainer/myblob",
        "http://myaccount.blob.core.windows.net/mycontainer",
        "http://myaccount",
        "HTTP://myaccount.BLOB.core.windows.net/mycontainer/myblob",
        "http://myaccount.blob.CORE.windows.net/mycontainer",
        "Http://127.0.0.1:45942/myaccount/mycontainer",
        "hTTP://127.0.0.1:45942/myaccount/mycontainer/myblob",
    ]
    for url in invalid_azure
        ok, host, account, container, blob = CloudStore.parseAzureAccountContainerBlob(url)
        @test !ok
    end

    invalid_s3 = [
        "http://bucket-name.s3-accelerate.us-east-1.amazonaws.com/key-name",
        "http://bucket-name.s3-accelerate.us-east-1.amazonaws.com",
        "http://bucket-name.s3-accelerate.amazonaws.com/key-name",
        "http://bucket-name.s3-accelerate.amazonaws.com",
        "http://bucket-name.s3.us-east-1.amazonaws.com/key-name",
        "http://bucket-name.s3.us-east-1.amazonaws.com",
        "http://bucket-name.s3.amazonaws.com/key-name",
        "http://bucket-name.s3.amazonaws.com",
        "http://s3.us-east-1.amazonaws.com/bucket-name/key-name",
        "http://s3.us-east-1.amazonaws.com/bucket-name",
        "http://s3.amazonaws.com/bucket-name/key-name",
        "http://s3.amazonaws.com/bucket-name",
        "http://bucket-name/key-name",
        "http://bucket-name",

        "Http://bucket-name.s3-ACCELERATE.us-east-1.amazonaws.com/key-name",
        "HTTP://bucket-name.s3-accelerate.us-east-1.AMAZONAWS.com",
        "http://bucket-name.S3-ACCELERATE.AMAZONAWS.com/key-name",
        "hTTP://bucket-name.s3-accelerate.amazonaws.com",
        "HTTP://bucket-name.s3.us-east-1.amazonaws.COM/key-name",
        "http://bucket-name.S3.us-east-1.AMAZONAWS.COM",
        "HTTP://bucket-name.S3.amazonaws.COM/key-name",
        "hTTP://bucket-name.S3.AMAZONAWS.COM",
        "hTTp://s3.us-east-1.AMAZONAWS.com/bucket-name/key-name",
        "HTTP://s3.us-east-1.amazonaws.COM/bucket-name",
        "hTTP://S3.AMAZONAWS.COM/bucket-name/key-name",
        "httP://S3.AmAzonAws.com/bucket-name",
        "httP://bucket-name/key-name",
        "httP://bucket-name",

        "http://bucket.vpce-1a2b3c4d-5e6f.s3.us-west-2.vpce.amazonaws.com/bucket-name",
        "http://bucket.vpce-1a2b3c4d-5e6f.s3.us-west-2.vpce.amazonaws.com/bucket-name/key-name",
        "https://bucket.vpce-1a2b3c4d-5e6f.s3.us-west-2.XvpceX.amazonaws.com/bucket-name",
        "https://bucket.Xvpce-1a2b3c4d-5e6f.s3.us-west-2.vpce.amazonaws.com/bucket-name",
        "https://XbucketX.vpce-1a2b3c4d-5e6f.s3.us-west-2.vpce.amazonaws.com/bucket-name/key-name",
    ]
    for url in invalid_s3
        ok, accelerate, host, bucket, reg, key = CloudStore.parseAWSBucketRegionKey(url; parseLocal=true)
        @test !ok
    end
end

@testset "CloudStore.PrefetchedDownloadStream small readbytes!" begin
    Minio.with(; debug=true) do conf
        credentials, bucket = conf
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^10; # 200 B
        S3.put(bucket, "test.csv", codeunits(multicsv); credentials)
        obj = CloudStore.Object(bucket, "test.csv"; credentials)
        @test length(obj) == sizeof(multicsv)

        N = 19
        buf = Vector{UInt8}(undef, N)
        copyto!(buf, 1, obj, 1, N)
        @test buf == view(codeunits(multicsv), 1:N)

        ioobj = CloudStore.PrefetchedDownloadStream(bucket, "test.csv", 16; credentials)
        i = 1
        while i < sizeof(multicsv)
            nb = i + N > length(multicsv) ? length(multicsv) - i : N
            readbytes!(ioobj, buf, N)
            @test view(buf, 1:nb) == view(codeunits(multicsv), i:i+nb-1)
            i += N
        end
    end
end

@testset "CloudStore.PrefetchedDownloadStream large readbytes!" begin
    Minio.with(; debug=true) do conf
        credentials, bucket = conf
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20 MB
        S3.put(bucket, "test.csv", codeunits(multicsv); credentials)
        obj = CloudStore.Object(bucket, "test.csv"; credentials)
        @test length(obj) == sizeof(multicsv)

        N = 1024*1024
        buf = Vector{UInt8}(undef, N)
        copyto!(buf, 1, obj, 1, N)
        @test buf == view(codeunits(multicsv), 1:N)

        ioobj = CloudStore.PrefetchedDownloadStream(bucket, "test.csv", 1024*1024; credentials)
        i = 1
        while i < sizeof(multicsv)
            nb = i + N > length(multicsv) ? length(multicsv) - i : N
            readbytes!(ioobj, buf, N)
            @test view(buf, 1:nb) == view(codeunits(multicsv), i:i+nb-1)
            i += N
        end
    end
end

@testset "CloudStore.PrefetchedDownloadStream small unsafe_read" begin
    Minio.with(; debug=true) do conf
        credentials, bucket = conf
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^10; # 200 B
        S3.put(bucket, "test.csv", codeunits(multicsv); credentials)
        obj = CloudStore.Object(bucket, "test.csv"; credentials)
        @test length(obj) == sizeof(multicsv)

        N = 19
        buf = Vector{UInt8}(undef, N)
        copyto!(buf, 1, obj, 1, N)
        @test buf == view(codeunits(multicsv), 1:N)

        ioobj = CloudStore.PrefetchedDownloadStream(bucket, "test.csv", 16; credentials)
        i = 1
        @time while i < sizeof(multicsv)
            nb = i + N > length(multicsv) ? length(multicsv) - i : N
            unsafe_read(ioobj, pointer(buf), nb)
            @test view(buf, 1:nb) == view(codeunits(multicsv), i:i+nb-1)
            i += N
        end
    end
end

@testset "CloudStore.PrefetchedDownloadStream large unsafe_read" begin
    Minio.with(; debug=true) do conf
        credentials, bucket = conf
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20 MB
        S3.put(bucket, "test.csv", codeunits(multicsv); credentials)
        obj = CloudStore.Object(bucket, "test.csv"; credentials)
        @test length(obj) == sizeof(multicsv)

        N = 1024*1024
        buf = Vector{UInt8}(undef, N)
        copyto!(buf, 1, obj, 1, N)
        @test buf == view(codeunits(multicsv), 1:N)

        ioobj = CloudStore.PrefetchedDownloadStream(bucket, "test.csv", 1024*1024; credentials)
        i = 1
        while i < sizeof(multicsv)
            nb = i + N > length(multicsv) ? length(multicsv) - i : N
            unsafe_read(ioobj, pointer(buf), nb)
            @test view(buf, 1:nb) == view(codeunits(multicsv), i:i+nb-1)
            i += N
        end
    end
end

@testset "CloudStore.PrefetchedDownloadStream small readbytes! decompress" begin
    Minio.with(; debug=true) do conf
        credentials, bucket = conf
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^100; # 2000 B
        codec = ZlibCompressor();
        CodecZlib.initialize(codec)
        compressed = transcode(codec, codeunits(multicsv))
        S3.put(bucket, "test.csv.gz", compressed; credentials)
        CodecZlib.finalize(codec)
        obj = CloudStore.Object(bucket, "test.csv.gz"; credentials)
        @test length(obj) == sizeof(compressed)

        N = 19
        buf = Vector{UInt8}(undef, N)
        ioobj = GzipDecompressorStream(CloudStore.PrefetchedDownloadStream(bucket, "test.csv.gz", 16; credentials))
        i = 1
        while i < sizeof(multicsv)
            nb = i + N > length(multicsv) ? length(multicsv) - i : N
            readbytes!(ioobj, buf, N)
            @test view(buf, 1:nb) == view(codeunits(multicsv), i:i+nb-1)
            i += N
        end
    end
end

@testset "CloudStore.PrefetchedDownloadStream large readbytes! decompress" begin
    Minio.with(; debug=true) do conf
        credentials, bucket = conf
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20 MB
        codec = ZlibCompressor();
        CodecZlib.initialize(codec)
        compressed = transcode(codec, codeunits(multicsv))
        S3.put(bucket, "test.csv.gz", compressed; credentials)
        CodecZlib.finalize(codec)
        obj = CloudStore.Object(bucket, "test.csv.gz"; credentials)
        @test length(obj) == sizeof(compressed)

        N = 1024*1024
        buf = Vector{UInt8}(undef, N)
        ioobj = GzipDecompressorStream(CloudStore.PrefetchedDownloadStream(bucket, "test.csv.gz", 16*1024; credentials))
        i = 1
        while i < sizeof(multicsv)
            nb = i + N > length(multicsv) ? length(multicsv) - i : N
            readbytes!(ioobj, buf, N)
            @test view(buf, 1:nb) == view(codeunits(multicsv), i:i+nb-1)
            i += N
        end
    end
end

@testset "CloudStore.PrefetchedDownloadStream empty file readbytes! decompress" begin
    Minio.with(; debug=true) do conf
        credentials, bucket = conf
        multicsv = ""; # 0 MB
        codec = ZlibCompressor();
        CodecZlib.initialize(codec)
        compressed = transcode(codec, codeunits(multicsv))
        S3.put(bucket, "test.csv.gz", compressed; credentials)
        CodecZlib.finalize(codec)
        obj = CloudStore.Object(bucket, "test.csv.gz"; credentials)
        @test length(obj) == sizeof(compressed)

        N = 1024*1024
        buf = ones(UInt8, N)
        ioobj = GzipDecompressorStream(CloudStore.PrefetchedDownloadStream(bucket, "test.csv.gz", 1024*1024; credentials))
        readbytes!(ioobj, buf, N)
        @test eof(ioobj)
        @test all(buf .== 1)
    end
end


@testset "CloudStore.PrefetchedDownloadStream empty file readbytes!" begin
    Minio.with(; debug=true) do conf
        credentials, bucket = conf
        multicsv = ""; # 0 MB
        S3.put(bucket, "test.csv", codeunits(multicsv); credentials)
        obj = CloudStore.Object(bucket, "test.csv"; credentials)
        @test length(obj) == sizeof(multicsv)

        N = 1024*1024
        buf = ones(UInt8, N)
        ioobj = CloudStore.PrefetchedDownloadStream(bucket, "test.csv", 1024*1024; credentials)
        readbytes!(ioobj, buf, N)
        @test eof(ioobj)
        @test all(buf .== 1)
    end
end

@testset "CloudStore.PrefetchedDownloadStream empty file unsafe_read" begin
    Minio.with(; debug=true) do conf
        credentials, bucket = conf
        multicsv = ""; # 0 MB
        S3.put(bucket, "test.csv", codeunits(multicsv); credentials)
        obj = CloudStore.Object(bucket, "test.csv"; credentials)
        @test length(obj) == sizeof(multicsv)

        N = 1024*1024
        buf = ones(UInt8, N)
        ioobj = CloudStore.PrefetchedDownloadStream(bucket, "test.csv", 1024*1024; credentials)
        @test_throws EOFError unsafe_read(ioobj, pointer(buf), N)
        @test eof(ioobj)
        @test all(buf .== 1)
    end
end

@testset "CloudStore.PrefetchedDownloadStream peek" begin
    Minio.with(; debug=true) do conf
        credentials, bucket = conf
        data = "0123456789123456"; # 16 B
        S3.put(bucket, "test.txt", codeunits(data); credentials)
        obj = CloudStore.Object(bucket, "test.txt"; credentials)
        @test length(obj) == sizeof(data)

        ioobj = CloudStore.PrefetchedDownloadStream(bucket, "test.txt", 16; credentials)
        iobuf = IOBuffer(data)
        @test peek(ioobj, Int8) == peek(iobuf, Int8)
        @test peek(ioobj, UInt8) == peek(iobuf, UInt8)
        @test peek(ioobj, Int16) == peek(iobuf, Int16)
        @test peek(ioobj, UInt16) == peek(iobuf, UInt16)
        @test peek(ioobj, Int32) == peek(iobuf, Int32)
        @test peek(ioobj, UInt32) == peek(iobuf, UInt32)
        @test peek(ioobj, Int64) == peek(iobuf, Int64)
        @test peek(ioobj, UInt64) == peek(iobuf, UInt64)
        @test peek(ioobj, Int128) == peek(iobuf, Int128)
        @test peek(ioobj, UInt128) == peek(iobuf, UInt128)
    end
end

@testset "_ndownload_tasks" begin
    MB = 1024*1024
    @test CloudStore.API._ndownload_tasks(32MB, 64MB, 8) == 1
    @test CloudStore.API._ndownload_tasks(32MB, 32MB, 8) == 1
    @test CloudStore.API._ndownload_tasks(32MB, 16MB, 8) == 2
    @test CloudStore.API._ndownload_tasks(32MB, 8MB, 8) == 4
    @test CloudStore.API._ndownload_tasks(32MB, 4MB, 8) == 8
    @test CloudStore.API._ndownload_tasks(32MB, 2MB, 8) == 8

    @test CloudStore.API._ndownload_tasks(32MB, 16MB + 1, 8) == 2
    @test CloudStore.API._ndownload_tasks(32MB, 16MB - 1, 8) == 3
    @test CloudStore.API._ndownload_tasks(32MB, 8MB + 1, 8) == 4
    @test CloudStore.API._ndownload_tasks(32MB, 8MB - 1, 8) == 5
    @test CloudStore.API._ndownload_tasks(32MB, 4MB + 1, 8) == 8
    @test CloudStore.API._ndownload_tasks(32MB, 4MB - 1, 8) == 8

    @test CloudStore.API._ndownload_tasks(32MB, 1, 8) == 8
    @test CloudStore.API._ndownload_tasks(32MB, 32MB, 8) == 1
    @test CloudStore.API._ndownload_tasks(32MB, 64MB, 8) == 1

    @test CloudStore.API._ndownload_tasks(32MB, 64MB, 1) == 1
    @test CloudStore.API._ndownload_tasks(32MB, 32MB, 1) == 1
    @test CloudStore.API._ndownload_tasks(32MB, 16MB, 1) == 1
    @test CloudStore.API._ndownload_tasks(32MB, 8MB, 1) == 1
    @test CloudStore.API._ndownload_tasks(32MB, 4MB, 1) == 1
    @test CloudStore.API._ndownload_tasks(32MB, 2MB, 1) == 1

    @test CloudStore.API._ndownload_tasks(0, 1, 1) == 1
end

@testset "parse" begin
    function parse_s3_url(;bucket="bucket-name", accelerate=true, region="us-east-1", key="key-name")
        isempty(region) || (region *= ".")
        accelerate_str = accelerate ? "s3-accelerate" : "s3"
        url = "https://$(bucket).$(accelerate_str).$(region)amazonaws.com/$(key)"
        return CloudStore.parseAWSBucketRegionKey(url; parseLocal=true)
    end

    function parse_azure_url(;account="myaccount", container="mycontainer", blob="myblob")
        url = "https://$account.blob.core.windows.net/$container/$blob"
        return CloudStore.parseAzureAccountContainerBlob(url; parseLocal=true)
    end

    @testset "validate_bucket_name" begin
        @test_throws ArgumentError CloudStore.validate_bucket_name("", false)
        @test !parse_s3_url(bucket="", accelerate=false)[1]
        @test_throws ArgumentError CloudStore.validate_bucket_name("a", false)
        @test_throws ArgumentError parse_s3_url(bucket="a", accelerate=false)
        @test_throws ArgumentError CloudStore.validate_bucket_name("ab", false)
        @test_throws ArgumentError parse_s3_url(bucket="ab", accelerate=false)
        @test_throws ArgumentError CloudStore.validate_bucket_name("a"^64, false)
        @test_throws ArgumentError parse_s3_url(bucket="a"^64, accelerate=false)
        @test_throws ArgumentError CloudStore.validate_bucket_name("a..b", false)
        @test !parse_s3_url(bucket="a..b", accelerate=false)[1]
        @test_throws ArgumentError CloudStore.validate_bucket_name("abcA", false)
        @test_throws ArgumentError parse_s3_url(bucket="abcA", accelerate=false)
        @test_throws ArgumentError CloudStore.validate_bucket_name("abc-", false)
        @test_throws ArgumentError parse_s3_url(bucket="abc-", accelerate=false)
        @test_throws ArgumentError CloudStore.validate_bucket_name("-abc", false)
        @test_throws ArgumentError parse_s3_url(bucket="-abc", accelerate=false)
        @test_throws ArgumentError CloudStore.validate_bucket_name("a/bc", false)
        @test_throws ArgumentError parse_s3_url(bucket="a/bc", accelerate=false)
        @test_throws ArgumentError CloudStore.validate_bucket_name("192.168.5.4", false)
        @test !parse_s3_url(bucket="192.168.5.4", accelerate=false)[1]

        @test_throws ArgumentError CloudStore.validate_bucket_name("a.bc", true)
        @test !parse_s3_url(bucket="a.bc", accelerate=true)[1]
        @test_throws ArgumentError CloudStore.validate_bucket_name("", true)
        @test !parse_s3_url(bucket="", accelerate=true)[1]
        @test_throws ArgumentError CloudStore.validate_bucket_name("a", true)
        @test_throws ArgumentError parse_s3_url(bucket="a", accelerate=true)
        @test_throws ArgumentError CloudStore.validate_bucket_name("ab", true)
        @test_throws ArgumentError parse_s3_url(bucket="ab", accelerate=true)
        @test_throws ArgumentError CloudStore.validate_bucket_name("a"^64, true)
        @test_throws ArgumentError parse_s3_url(bucket="a"^64, accelerate=true)
        @test_throws ArgumentError CloudStore.validate_bucket_name("a..b", true)
        @test !parse_s3_url(bucket="a..b", accelerate=true)[1]
        @test_throws ArgumentError CloudStore.validate_bucket_name("abcA", true)
        @test_throws ArgumentError parse_s3_url(bucket="abcA", accelerate=true)
        @test_throws ArgumentError CloudStore.validate_bucket_name("abc-", true)
        @test_throws ArgumentError parse_s3_url(bucket="abc-", accelerate=true)
        @test_throws ArgumentError CloudStore.validate_bucket_name("-abc", true)
        @test_throws ArgumentError parse_s3_url(bucket="-abc", accelerate=true)
        @test_throws ArgumentError CloudStore.validate_bucket_name("a/bc", true)
        @test_throws ArgumentError parse_s3_url(bucket="a/bc", accelerate=true)
        @test_throws ArgumentError CloudStore.validate_bucket_name("192.168.5.4", true)
        @test !parse_s3_url(bucket="192.168.5.4", accelerate=true)[1]

        @test CloudStore.validate_bucket_name("a.b-c1", false) == "a.b-c1"
        @test CloudStore.validate_bucket_name("a"^63, false) == "a"^63
        @test CloudStore.validate_bucket_name("a"^3, false) == "a"^3
        # xn-- prefix and -s3alias suffix are apparently illegal in bucket names create by
        # the user but can be received from AWS, see e.g.
        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-points-alias.html
        @test CloudStore.validate_bucket_name("xn--a", false) == "xn--a"
        @test CloudStore.validate_bucket_name("a-s3alias", false) == "a-s3alias"

        @test_throws ArgumentError("Validation failed for `region` \"xx-xxxx-x\"") CloudStore.parseAWSBucketRegionKey("https://bucket.vpce-1a2b3c4d-5e6f.s3.xx-xxxx-x.vpce.amazonaws.com/bucket-name")
        @test_throws ArgumentError("Validation failed for `bucket` name \"bn\": Bucket names must be between 3 (min) and 63 (max) characters long.") CloudStore.parseAWSBucketRegionKey("https://bucket.vpce-1a2b3c4d-5e6f.s3.us-west-2.vpce.amazonaws.com/bn")
        @test_throws ArgumentError("Validation failed for `key` \"key-n$("a" ^ 1024)me\": The key name must be shorter than 1025 bytes.") CloudStore.parseAWSBucketRegionKey("https://bucket.vpce-1a2b3c4d-5e6f.s3.us-west-2.vpce.amazonaws.com/bucket-name/key-n$("a" ^ 1024)me")
    end

    @testset "validate_container_name" begin
        @test_throws ArgumentError CloudStore.validate_container_name("")
        @test !parse_azure_url(container="")[1]
        @test_throws ArgumentError CloudStore.validate_container_name("a")
        @test_throws ArgumentError parse_azure_url(container="a")
        @test_throws ArgumentError CloudStore.validate_container_name("ab")
        @test_throws ArgumentError parse_azure_url(container="ab")
        @test_throws ArgumentError CloudStore.validate_container_name("a"^64)
        @test_throws ArgumentError parse_azure_url(container="a"^64)
        @test_throws ArgumentError CloudStore.validate_container_name("a--b")
        @test_throws ArgumentError parse_azure_url(container="a--b")
        @test_throws ArgumentError CloudStore.validate_container_name("abcA")
        @test_throws ArgumentError parse_azure_url(container="abcA")
        @test_throws ArgumentError CloudStore.validate_container_name("abc-")
        @test_throws ArgumentError parse_azure_url(container="abc-")
        @test_throws ArgumentError CloudStore.validate_container_name("-abc")
        @test_throws ArgumentError parse_azure_url(container="-abc")
        @test_throws ArgumentError CloudStore.validate_container_name("a/bc")
        @test_throws ArgumentError parse_azure_url(container="a/bc")
        @test_throws ArgumentError CloudStore.validate_container_name("a.bc")
        @test_throws ArgumentError parse_azure_url(container="a.bc")
        @test_throws ArgumentError CloudStore.validate_container_name("192.168.5.4")
        @test_throws ArgumentError parse_azure_url(container="192.168.5.4")

        @test CloudStore.validate_container_name("ab-c1") == "ab-c1"
        @test CloudStore.validate_container_name("a"^63) == "a"^63
        @test CloudStore.validate_container_name("a"^3) == "a"^3
    end

    @testset "validate_key" begin
        @test_throws ArgumentError CloudStore.validate_key("a"^1025)
        @test_throws ArgumentError parse_s3_url(key="a"^1025)
        @test_throws ArgumentError CloudStore.validate_key("a"^1026)
        @test_throws ArgumentError parse_s3_url(key="a"^1026)

        @test CloudStore.validate_key("a"^1024) == "a"^1024
    end

    @testset "validate_region" begin
        @test_throws ArgumentError CloudStore.validate_region("no-region-1")
        @test_throws ArgumentError parse_s3_url(region="no-region-1")

        @test CloudStore.validate_key("us-east-1") == "us-east-1"
    end

    @testset "validate_blob" begin
        @test_throws ArgumentError CloudStore.validate_blob("a"^1025)
        @test_throws ArgumentError parse_azure_url(blob="a"^1025)
        @test_throws ArgumentError CloudStore.validate_blob("a"^1026)
        @test_throws ArgumentError parse_azure_url(blob="a"^1026)
        @test_throws ArgumentError CloudStore.validate_blob(join(fill("a", 255), '/'))
        @test_throws ArgumentError parse_azure_url(blob=join(fill("a", 255), '/'))
        @test_throws ArgumentError CloudStore.validate_blob(join(fill("a", 256), '/'))
        @test_throws ArgumentError parse_azure_url(blob=join(fill("a", 256), '/'))

        @test CloudStore.validate_blob("a"^1024) == "a"^1024
        @test CloudStore.validate_blob(join(fill("a", 254), '/')) == join(fill("a", 254), '/')
    end

    @testset "validate_account_name" begin
        @test_throws ArgumentError CloudStore.validate_account_name("")
        @test !parse_azure_url(account="")[1]
        @test_throws ArgumentError CloudStore.validate_account_name("a")
        @test_throws ArgumentError parse_azure_url(account="a")
        @test_throws ArgumentError CloudStore.validate_account_name("aa")
        @test_throws ArgumentError parse_azure_url(account="aa")
        @test_throws ArgumentError CloudStore.validate_account_name("a"^25)
        @test_throws ArgumentError parse_azure_url(account="a"^25)
        @test_throws ArgumentError CloudStore.validate_account_name("a"^26)
        @test_throws ArgumentError parse_azure_url(account="a"^26)
        @test_throws ArgumentError CloudStore.validate_account_name("a.b")
        @test !parse_azure_url(account="a.b")[1]
        @test_throws ArgumentError CloudStore.validate_account_name("a-b")
        @test_throws ArgumentError parse_azure_url(account="a-b")
        @test_throws ArgumentError CloudStore.validate_account_name("a/b")
        @test_throws ArgumentError parse_azure_url(account="a/b")

        @test CloudStore.validate_account_name("abcd123456") == "abcd123456"
        @test CloudStore.validate_account_name("1a1") == "1a1"
    end
end

@testset "CloudStore.PrefetchedDownloadStream read last byte" begin
    Minio.with(; debug=true) do conf
        credentials, bucket = conf
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20 MB
        S3.put(bucket, "test.csv", codeunits(multicsv); credentials)
        obj = CloudStore.Object(bucket, "test.csv"; credentials)
        @test length(obj) == sizeof(multicsv)

        N = length(multicsv) - 1
        buf = Vector{UInt8}(undef, N)
        copyto!(buf, 1, obj, 1, N)
        @assert buf == view(codeunits(multicsv), 1:N)

        ioobj = CloudStore.PrefetchedDownloadStream(bucket, "test.csv", 16*1024; credentials)
        readbytes!(ioobj, buf, N)
        @test buf == view(codeunits(multicsv), 1:N)
        @test read(ioobj, UInt8) == UInt8(last(multicsv))
    end
end

# When using Minio, the minimum upload size per part is 5MB according to
# S3 specifications: https://github.com/minio/minio/issues/11076
# I couldn't find a minimum upload size for Azure blob storage.
@testset "CloudStore.MultipartUploadStream write large bytes - S3" begin
    Minio.with(; debug=true) do conf
        credentials, bucket = conf
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20MB

        N = 5500000
        mus_obj = CloudStore.MultipartUploadStream(bucket, "test.csv"; credentials)

        i = 1
        while i < sizeof(multicsv)
            nb = i + N > length(multicsv) ? length(multicsv)-i+1 : N
            buf = Vector{UInt8}(undef, nb)
            copyto!(buf, 1, codeunits(multicsv), i, nb)
            CloudStore.write(mus_obj, buf;)
            i += N
        end

        CloudStore.wait(mus_obj)
        CloudStore.close(mus_obj; credentials)
        obj = CloudStore.Object(bucket, "test.csv"; credentials)
        @test length(obj) == sizeof(multicsv)
    end
end

@testset "CloudStore.MultipartUploadStream abort - S3" begin
    Minio.with(; debug=true) do conf
        credentials, bucket = conf
        @test_throws ArgumentError CloudStore.MultipartUploadStream(
            bucket, "invalid.bin"; credentials, concurrent_writes_to_channel=0)

        io = CloudStore.MultipartUploadStream(bucket, "aborted.bin"; credentials)
        write(io, fill(0x2a, 5_500_000))
        wait(io)
        CloudStore.abort(io; credentials)

        @test io.aborted
        @test !isopen(io)
        @test_throws InvalidStateException write(io, UInt8[0x01])
        @test_throws StatusError S3.head(bucket, "aborted.bin"; credentials)
    end
end

@testset "CloudStore.MultipartUploadStream failure due to too small upload size - S3" begin
    Minio.with(; debug=true) do conf
        credentials, bucket = conf
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20MB

        N = 55000
        mus_obj = CloudStore.MultipartUploadStream(bucket, "test.csv"; credentials)
        try
            i = 1
            nb = i + N > length(multicsv) ? length(multicsv)-i+1 : N
            buf = Vector{UInt8}(undef, nb)
            copyto!(buf, 1, codeunits(multicsv), i, nb)
            CloudStore.write(mus_obj, buf;)
            CloudStore.wait(mus_obj)
            CloudStore.close(mus_obj; credentials) # This should fail
        catch e
            @test isnothing(mus_obj.exc) == false
        end
    end
end

@testset "CloudStore.MultipartUploadStream failure due to changed url - S3" begin
    Minio.with(; debug=true) do conf
        credentials, bucket = conf
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20MB

        N = 5500000
        mus_obj = CloudStore.MultipartUploadStream(bucket, "test.csv"; credentials)
        try
            i = 1
            nb = i + N > length(multicsv) ? length(multicsv)-i+1 : N
            buf = Vector{UInt8}(undef, nb)
            copyto!(buf, 1, codeunits(multicsv), i, nb)
            # Changing the url after the MultipartUploadStream object was created
            mus_obj.url = "http://127.0.0.1:23252/jl-minio-22377/test_nantia.csv"
            CloudStore.write(mus_obj, buf;) # This should fail
            CloudStore.wait(mus_obj)
        catch e
            @test isnothing(mus_obj.exc) == false
        end
    end
end

@testset "CloudStore.MultipartUploadStream write large bytes - Azure" begin
    Azurite.with(; debug=true) do conf
        credentials, bucket = conf
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20MB

        N = 2000000
        mus_obj = CloudStore.MultipartUploadStream(bucket, "test.csv"; credentials)

        i = 1
        while i < sizeof(multicsv)
            nb = i + N > length(multicsv) ? length(multicsv)-i+1 : N
            buf = Vector{UInt8}(undef, nb)
            copyto!(buf, 1, codeunits(multicsv), i, nb)
            CloudStore.write(mus_obj, buf;)
            i += N
        end

        CloudStore.wait(mus_obj)
        CloudStore.close(mus_obj; credentials)
        obj = CloudStore.Object(bucket, "test.csv"; credentials)
        @test length(obj) == sizeof(multicsv)
    end
end

@testset "CloudStore.MultipartUploadStream test alternative syntax - Azure" begin
    Azurite.with(; debug=true) do conf
        credentials, bucket = conf
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20MB

        N = 2000000
        function uploading_loop(multicsv, batch_size, mus_obj)
            i = 1
            while i < sizeof(multicsv)
                nb = i + batch_size > length(multicsv) ? length(multicsv)-i+1 : batch_size
                buf = Vector{UInt8}(undef, nb)
                copyto!(buf, 1, codeunits(multicsv), i, nb)
                CloudStore.write(mus_obj, buf;)
                i += batch_size
            end
        end

       CloudStore.MultipartUploadStream(bucket, "test.csv"; credentials) do mus_obj
            uploading_loop(multicsv, N, mus_obj)
        end

        obj = CloudStore.Object(bucket, "test.csv"; credentials)
        @test length(obj) == sizeof(multicsv)
    end
end

end # @testset "CloudStore.jl"
