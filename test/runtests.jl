using Test, CloudStore, CloudBase.CloudTest
import CloudStore: S3, Azure

bytes(x) = codeunits(x)

function stringfile(x)
    path, io = mktemp()
    write(io, x)
    close(io)
    return path
end

iobuffer(x) = IOBuffer(x)

struct TempFile <: IO
    path
    io
end
TempFile() = TempFile(mktemp()...)
Base.close(x::TempFile) = close(x.io)
Base.unsafe_write(x::TempFile, p::Ptr{UInt8}, n::UInt) = Base.unsafe_write(x.io, p, n)
Base.write(x::TempFile, y::AbstractString) = write(x.io, y)
Base.write(x::TempFile, y::Union{SubString{String}, String}) = write(x.io, y)
Base.unsafe_read(x::TempFile, p::Ptr{UInt8}, n::UInt) = Base.unsafe_read(x.path, p, n)
Base.readbytes!(s::TempFile, b::AbstractArray{UInt8}, nb=length(b)) = readbytes!(s.io, b, nb)
Base.eof(x::TempFile) = eof(x.io)
Base.bytesavailable(x::TempFile) = bytesavailable(x.io)
Base.read(x::TempFile, ::Type{UInt8}) = read(x.io, UInt8)
Base.seekstart(x::TempFile) = seekstart(x.io)
Base.rm(x::TempFile) = rm(x.path)

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
        account, bucket = conf
        csv = "a,b,c\n1,2,3\n4,5,$(rand())"
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20MB
        for inBody in (bytes, stringfile, iobuffer, iofile)
            for outBody in (Vector{UInt8}, String, IO)
                body = inBody(csv)
                out = outType(outBody)
                println("in: $inBody, out: $outBody, single part, no compression")
                obj = S3.put(bucket, "test.csv", body; account)
                data = S3.get(bucket, "test.csv", out; account)
                @test check(body, data)
                resetOut!(out)
                # get on Object
                data = S3.get(obj, out; account)
                @test check(body, data)
                resetOut!(out)
                
                println("in: $inBody, out: $outBody, single part, compression")
                obj = S3.put(bucket, "test2.csv", body; compress=true, account)
                data = S3.get(bucket, "test2.csv", out; decompress=true, account)
                @test check(body, data)
                resetOut!(out)
                cleanup!(body)
                
                mbody = inBody(multicsv);
                out = outType(outBody)
                println("in: $inBody, out: $outBody, multipart, no compression")
                obj = S3.put(bucket, "test3.csv", mbody; multipartThreshold=5_000_000, multipartSize=5_500_000, account)
                data = S3.get(bucket, "test3.csv", out; account)
                @test check(mbody, data)
                resetOut!(out)
                println("in: $inBody, out: $outBody, multipart, compression")
                obj = S3.put(bucket, "test4.csv", mbody; compress=true, multipartThreshold=5_000_000, multipartSize=5_500_000, account)
                data = S3.get(bucket, "test4.csv", out; decompress=true, account)
                @test check(mbody, data)
                resetOut!(out)
                cleanup!(mbody)
            end
        end
    end
end
