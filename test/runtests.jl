using Test, CloudStore, CloudBase.CloudTest
import CloudStore: S3, Blobs
using CodecZlib

bytes(x) = codeunits(x)

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

outType(::Type{Vector{UInt8}}) = nothing
outType(::Type{String}) = tempname()
outType(::Type{IO}) = IOBuffer()

cleanup!(x::Union{String, TempFile}) = rm(x)
cleanup!(x::IO) = close(x)
cleanup!(x) = nothing

check(x::AbstractVector{UInt8}, y::AbstractVector{UInt8}) = x == y
check(x::AbstractVector{UInt8}, y::String) = x == read(y)
check(x::AbstractVector{UInt8}, y::IO) = begin; reset!(y); z = x == read(y); reset!(y); z end
check(x::String, y::AbstractVector{UInt8}) = read(x) == y
check(x::IO, y::AbstractVector{UInt8}) = begin; reset!(x); z = read(x) == y; reset!(x); z end
check(x, y) = begin; reset!(x); reset!(y); z = read(x) == read(y); reset!(x); reset!(y); z end

@testset "CloudStore.jl" begin
@testset "S3" begin
    # conf, p = Minio.run(; debug=true)
    Minio.with(; debug=true) do conf
        credentials, bucket = conf
        empty = ""
        csv = "a,b,c\n1,2,3\n4,5,$(rand())"
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20MB
        for inBody in (bytes, stringfile, iobuffer, iofile)
            for outBody in (Vector{UInt8}, String, IO)
                body = inBody(csv)
                out = outType(outBody)
                println("in: $inBody, out: $outBody, single part, no compression")
                obj = S3.put(bucket, "test.csv", body; credentials)
                data = S3.get(bucket, "test.csv", out; credentials)
                @test check(body, data)
                resetOut!(out)
                # get on Object
                data = S3.get(obj, out; credentials)
                @test check(body, data)
                resetOut!(out)
                # object metadata
                meta = S3.head(bucket, "test.csv"; credentials)
                @test meta isa Dict && !isempty(meta)

                # list
                objs = S3.list(bucket; credentials)
                @test length(objs) == 1
                @test objs[1].key == "test.csv"

                println("in: $inBody, out: $outBody, single part, compression")
                obj = S3.put(bucket, "test2.csv", body; compress=true, credentials)
                data = S3.get(bucket, "test2.csv", out; decompress=true, credentials)
                @test check(body, data)
                resetOut!(out)

                # passing urls directly
                url = "$(bucket.baseurl)test5.csv"
                obj = S3.put(url, body; credentials)
                data = S3.get(url, out; credentials)
                @test check(body, data)
                resetOut!(out)
                cleanup!(body)

                # 0 byte file
                ebody = inBody(empty)
                obj = S3.put(bucket, "test6.csv", ebody; credentials)
                data = S3.get(bucket, "test6.csv", out; credentials)
                @test check(ebody, data)
                resetOut!(out)
                cleanup!(ebody)

                mbody = inBody(multicsv);
                out = outType(outBody)
                println("in: $inBody, out: $outBody, multipart, no compression")
                obj = S3.put(bucket, "test3.csv", mbody; multipartThreshold=5_000_000, partSize=5_500_000, credentials)
                data = S3.get(bucket, "test3.csv", out; credentials)
                @test check(mbody, data)
                resetOut!(out)
                println("in: $inBody, out: $outBody, multipart, compression")
                obj = S3.put(bucket, "test4.csv", mbody; compress=true, multipartThreshold=5_000_000, partSize=5_500_000, credentials)
                data = S3.get(bucket, "test4.csv", out; decompress=true, credentials)
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
end

@time @testset "Blobs" begin
    # conf, p = Azurite.run(; debug=true)
    Azurite.with(; debug=true) do conf
        credentials, container = conf
        empty = ""
        csv = "a,b,c\n1,2,3\n4,5,$(rand())"
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20MB
        for inBody in (bytes, stringfile, iobuffer, iofile)
            for outBody in (Vector{UInt8}, String, IO)
                body = inBody(csv)
                out = outType(outBody)
                println("in: $inBody, out: $outBody, single part, no compression")
                obj = Blobs.put(container, "test.csv", body; credentials)
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

                # list
                objs = Blobs.list(container; credentials)
                @test length(objs) == 1
                @test objs[1].key == "test.csv"

                println("in: $inBody, out: $outBody, single part, compression")
                obj = Blobs.put(container, "test2.csv", body; compress=true, credentials)
                data = Blobs.get(container, "test2.csv", out; decompress=true, credentials)
                @test check(body, data)
                resetOut!(out)

                # passing urls directly
                url = "$(container.baseurl)test5.csv"
                obj = Blobs.put(url, body; credentials)
                data = Blobs.get(url, out; credentials)
                @test check(body, data)
                resetOut!(out)
                cleanup!(body)

                # 0 byte file
                ebody = inBody(empty)
                obj = Blobs.put(container, "test6.csv", ebody; credentials)
                data = Blobs.get(container, "test6.csv", out; credentials)
                @test check(ebody, data)
                resetOut!(out)
                cleanup!(ebody)

                mbody = inBody(multicsv);
                out = outType(outBody)
                println("in: $inBody, out: $outBody, multipart, no compression")
                obj = Blobs.put(container, "test3.csv", mbody; multipartThreshold=5_000_000, partSize=5_500_000, credentials)
                data = Blobs.get(container, "test3.csv", out; credentials)
                @test check(mbody, data)
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
end

@testset "URL Parsing Unit Tests" begin

    azure = [
        ("https://myaccount.blob.core.windows.net/mycontainer/myblob", (true, nothing, "myaccount", "mycontainer", "myblob")),
        ("https://myaccount.blob.core.windows.net/mycontainer", (true, nothing, "myaccount", "mycontainer", "")),
        ("azure://myaccount.blob.core.windows.net/mycontainer/myblob", (true, nothing, "myaccount", "mycontainer", "myblob")),
        ("azure://myaccount.blob.core.windows.net/mycontainer", (true, nothing, "myaccount", "mycontainer", "")),
        ("https://127.0.0.1:45942/myaccount/mycontainer", (true, "https://127.0.0.1:45942", "myaccount", "mycontainer", "")),
        ("https://127.0.0.1:45942/myaccount/mycontainer/myblob", (true, "https://127.0.0.1:45942", "myaccount", "mycontainer", "myblob")),
        ("azure://127.0.0.1:45942/myaccount/mycontainer", (true, "https://127.0.0.1:45942", "myaccount", "mycontainer", "")),
        ("azure://127.0.0.1:45942/myaccount/mycontainer/myblob", (true, "https://127.0.0.1:45942", "myaccount", "mycontainer", "myblob")),
        ("azure://myaccount", (true, nothing, "myaccount", "", ""))
    ]
    for (url, parts) in azure
        ok, host, account, container, blob = CloudStore.parseAzureAccountContainerBlob(url; parseLocal=true)
        @test ok
        @test host == parts[2]
        @test account == parts[3]
        @test container == parts[4]
        @test blob == parts[5]
    end

    s3 = [
        ("https://bucket-name.s3-accelerate.region-code.amazonaws.com/key-name", (true, true, nothing, "bucket-name", "region-code", "key-name")),
        ("https://bucket-name.s3-accelerate.region-code.amazonaws.com", (true, true, nothing, "bucket-name", "region-code", "")),
        ("https://bucket-name.s3-accelerate.amazonaws.com/key-name", (true, true, nothing, "bucket-name", "", "key-name")),
        ("https://bucket-name.s3-accelerate.amazonaws.com", (true, true, nothing, "bucket-name", "", "")),
        ("https://bucket-name.s3.region-code.amazonaws.com/key-name", (true, false, nothing, "bucket-name", "region-code", "key-name")),
        ("https://bucket-name.s3.region-code.amazonaws.com", (true, false, nothing, "bucket-name", "region-code", "")),
        ("https://bucket-name.s3.amazonaws.com/key-name", (true, false, nothing, "bucket-name", "", "key-name")),
        ("https://bucket-name.s3.amazonaws.com", (true, false, nothing, "bucket-name", "", "")),
        ("https://s3.region-code.amazonaws.com/bucket-name/key-name", (true, false, nothing, "bucket-name", "region-code", "key-name")),
        ("https://s3.region-code.amazonaws.com/bucket-name", (true, false, nothing, "bucket-name", "region-code", "")),
        ("https://s3.amazonaws.com/bucket-name/key-name", (true, false, nothing, "bucket-name", "", "key-name")),
        ("https://s3.amazonaws.com/bucket-name", (true, false, nothing, "bucket-name", "", "")),
        ("s3://bucket-name/key-name", (true, false, nothing, "bucket-name", "", "key-name")),
        ("s3://bucket-name", (true, false, nothing, "bucket-name", "", "")),
        ("http://127.0.0.1:27181/bucket-name/key-name", (true, false, "http://127.0.0.1:27181", "bucket-name", "", "key-name")),
        ("http://127.0.0.1:27181/bucket-name", (true, false, "http://127.0.0.1:27181", "bucket-name", "", "")),
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

end # @testset "CloudStore.jl"
