module UploadValidationTests

using Test, CloudStore
import CloudBase, HTTP
const API = CloudStore.API

struct ValidationStore <: CloudBase.AbstractStore
    baseurl::String
    calls::Vector{Symbol}
    bytes::Vector{UInt8}
end
ValidationStore() = ValidationStore("https://upload.invalid/", Symbol[], UInt8[])
API.startMultipartUpload(store::ValidationStore, key; kw...) = (push!(store.calls, :start); nothing)
API.completeMultipartUpload(store::ValidationStore, args...; kw...) = (push!(store.calls, :complete); "complete")
API.abortMultipartUpload(store::ValidationStore, args...; kw...) = (push!(store.calls, :abort); nothing)
function API.putObject(store::ValidationStore, key, body; kw...)
    push!(store.calls, :put)
    append!(store.bytes, body)
    return HTTP.Response(200, ["ETag" => "\"single\""])
end

mutable struct UnknownInput <: IO
    buffer::IOBuffer
    reads::Int
end
Base.eof(input::UnknownInput) = eof(input.buffer)
Base.isopen(input::UnknownInput) = isopen(input.buffer)
function Base.read(input::UnknownInput, n::Integer)
    input.reads += 1
    return read(input.buffer, n)
end
Base.read(input::UnknownInput) = (input.reads += 1; read(input.buffer))

@testset "Multipart upload size validation" begin
    data = UInt8[1, 2, 3, 4]
    inputs = (() -> copy(data), () -> IOBuffer(copy(data)), () -> UnknownInput(IOBuffer(copy(data)), 0))
    for (partSize, batchSize) in ((0, 2), (-1, 2), (2, 0), (2, -1)),
            threshold in (1, length(data)), makeinput in inputs
        input = makeinput()
        store = ValidationStore()
        @test_throws ArgumentError API.putObjectImpl(store, "data", input;
            multipartThreshold=threshold, partSize, batchSize)
        @test isempty(store.calls)
        if input isa IOBuffer
            @test position(input) == 0
            @test isopen(input)
        elseif input isa UnknownInput
            @test input.reads == 0
            @test isopen(input)
        end
    end

    for makeinput in inputs
        input = makeinput()
        store = ValidationStore()
        object = API.putObjectImpl(store, "single", input;
            allowMultipart=false, partSize=0, batchSize=-1)
        @test store.calls == [:put]
        @test store.bytes == data
        @test object.size == length(data)
        input isa IO && @test isopen(input)
    end
end

end # module
