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

@testset "CloudStore.jl" begin
#=@testset "S3" begin
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
                obj = S3.put(bucket, "test.csv", body; credentials)
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
                obj = S3.put(bucket, "test3.csv", mbody; multipartThreshold=5_000_000, partSize=5_500_000, lograte=true, credentials)
                data = S3.get(bucket, "test3.csv", out; objectMaxSize=sizeof(multicsv), lograte=true, credentials)
                @test check(mbody, data)
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
end=#

#=@time @testset "Blobs" begin
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
        S3.put(bucket, "test_nantia.csv", codeunits(multicsv); credentials)
        obj = CloudStore.Object(bucket, "test_nantia.csv"; credentials)
        @test length(obj) == sizeof(multicsv)

        N = 19
        buf = Vector{UInt8}(undef, N)
        copyto!(buf, 1, obj, 1, N)
        @test buf == view(codeunits(multicsv), 1:N)

        ioobj = CloudStore.PrefetchedDownloadStream(bucket, "test_nantia.csv", 16; credentials)
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
        @test_throws ArgumentError CloudStore.validate_bucket_name("xn--abc", false)
        @test_throws ArgumentError parse_s3_url(bucket="xn--abc", accelerate=false)
        @test_throws ArgumentError CloudStore.validate_bucket_name("abcs-s3alias", false)
        @test_throws ArgumentError parse_s3_url(bucket="abcs-s3alias", accelerate=false)
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
        @test_throws ArgumentError CloudStore.validate_bucket_name("xn--abc", true)
        @test_throws ArgumentError parse_s3_url(bucket="xn--abc", accelerate=true)
        @test_throws ArgumentError CloudStore.validate_bucket_name("abcs-s3alias", true)
        @test_throws ArgumentError parse_s3_url(bucket="abcs-s3alias", accelerate=true)
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
end=#

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
            @test view(buf, 1:nb) == view(codeunits(multicsv), i:i+nb-1)
            CloudStore.write(mus_obj, buf;)
            i += N
        end

        CloudStore.close(mus_obj; credentials)
        obj = CloudStore.Object(bucket, "test.csv"; credentials)
        @test length(obj) == sizeof(multicsv)
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
            @test view(buf, 1:nb) == view(codeunits(multicsv), i:i+nb-1)
            CloudStore.write(mus_obj, buf;)
            i += N
        end

        CloudStore.close(mus_obj; credentials)
        obj = CloudStore.Object(bucket, "test.csv"; credentials)
        @test length(obj) == sizeof(multicsv)
    end
end

end # @testset "CloudStore.jl"
