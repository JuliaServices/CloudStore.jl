using Test, CloudStore, CloudBase.CloudTest
import CloudStore: S3, Blobs

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

@time @testset "S3" begin
    # conf, p = Minio.run(; debug=true)
    Minio.with(; debug=true) do conf
        credentials, bucket = conf
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
                cleanup!(body)

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
                @test length(objs) == 4
                @test map(x -> x.key, objs) == ["test.csv", "test2.csv", "test3.csv", "test4.csv"]
                objs = S3.list(bucket; maxKeys=1, credentials)
                @test length(objs) == 4
                @test map(x -> x.key, objs) == ["test.csv", "test2.csv", "test3.csv", "test4.csv"]

                # delete
                S3.delete(bucket, "test.csv"; credentials)
                S3.delete(bucket, "test2.csv"; credentials)
                S3.delete(bucket, "test3.csv"; credentials)
                S3.delete(bucket, "test4.csv"; credentials)
                
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
                cleanup!(body)

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
                @test length(objs) == 4
                @test map(x -> x.key, objs) == ["test.csv", "test2.csv", "test3.csv", "test4.csv"]
                objs = Blobs.list(container; maxKeys=1, credentials)
                @test length(objs) == 4
                @test map(x -> x.key, objs) == ["test.csv", "test2.csv", "test3.csv", "test4.csv"]

                # delete
                Blobs.delete(container, "test.csv"; credentials)
                Blobs.delete(container, "test2.csv"; credentials)
                Blobs.delete(container, "test3.csv"; credentials)
                Blobs.delete(container, "test4.csv"; credentials)

                objs = Blobs.list(container; credentials)
                @test length(objs) == 0
            end
        end
    end
end
