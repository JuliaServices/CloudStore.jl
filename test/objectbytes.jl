using ZipArchives

@testset "ObjectBytes indexing, views and bulk copies" begin
    expected = repeat(collect(UInt8(0):UInt8(255)), 2)
    with_range_fixture(; data=expected) do host, requests
        for (store, credentials) in range_stores(host)
            object = CloudStore.Object(store, credentials, "bytes", length(expected), "v1")
            bytes = CloudStore.ObjectBytes(object; blocksize=31, retries=0)
            @test size(bytes) == size(expected)
            @test eltype(bytes) == UInt8
            @test axes(bytes) == axes(expected)
            start = length(requests)
            @test bytes[1] == expected[1]
            @test bytes[31] == expected[31]
            @test bytes[5:15] == expected[5:15]
            @test length(requests) == start + 1
            @test bytes[32] == expected[32]
            @test bytes[end] == expected[end]
            @test bytes[end - 1] == expected[end - 1]
            @test length(requests) == start + 3

            portion = view(bytes, 6:100)
            nested = view(portion, 8:70)
            @test portion isa CloudStore.ObjectBytes
            @test copy(nested) == expected[13:75]
            @test collect(view(bytes, 1:2:20)) == expected[1:2:20]
            @test bytes[:] == expected
            owned = bytes[6:100]
            owned[1] = 0xff
            @test bytes[6] == expected[6]

            destination = fill(0xff, 12)
            @test copyto!(destination, 3, portion, 9, 8) === destination
            @test destination == vcat(0xff, 0xff, expected[14:21], 0xff, 0xff)
            fill!(destination, 0xff)
            strided = view(destination, 1:2:11)
            @test copyto!(strided, 2, bytes, 10, 4) === strided
            @test destination[3:2:9] == expected[10:13]
            @test all(==(0xff), destination[2:2:12])
            @test length(bytes.window.bytes) == 31
            @test bytes.window === nested.window
            @test_throws Exception setindex!(bytes, 0xff, 1)

            @static if isdefined(Core, :Memory)
                memory = Memory{UInt8}(undef, 7)
                @test copyto!(memory, 1, bytes, 25, 7) === memory
                @test collect(memory) == expected[25:31]
            end
        end
    end
end

@testset "ObjectBytes bounds and empty arrays" begin
    with_range_fixture() do host, requests
        store, credentials = first(range_stores(host))
        object = CloudStore.Object(store, credentials, "bytes", 12, "v1")
        bytes = CloudStore.ObjectBytes(object; blocksize=4)
        for index in (0, -1, 13, typemax(Int))
            @test_throws BoundsError bytes[index]
        end
        for indices in (0:1, 1:13, typemax(Int):typemax(Int))
            @test_throws BoundsError view(bytes, indices)
        end
        @test isempty(view(bytes, 13:12))
        @test isempty(view(bytes, 14:13)) == isempty(view(zeros(UInt8, 12), 14:13))
        @test isempty(view(view(bytes, 4:7), 5:4))
        @test_throws BoundsError copyto!(zeros(UInt8, 4), 1, bytes, 11, 4)
        @test_throws BoundsError copyto!(zeros(UInt8, 4), 2, bytes, 1, 4)
        @test_throws BoundsError copyto!(zeros(UInt8, 4), 1, bytes, 1, typemax(Int))
        @test_throws BoundsError copyto!(zeros(UInt8, 4), 1, bytes, 1, typemax(UInt))
        @test_throws ArgumentError copyto!(zeros(UInt8, 4), 1, bytes, 1, -1)
        @test isempty(copyto!(UInt8[], typemax(Int), bytes, typemax(Int), 0))
        for blocksize in (0, -1, big(typemax(Int)) + 1)
            @test_throws ArgumentError CloudStore.ObjectBytes(object; blocksize)
        end
        @test_throws ArgumentError CloudStore.ObjectBytes(CloudStore.Object(store, credentials, "bytes", -1, "v1"))

        empty_object = CloudStore.Object(store, credentials, "empty", 0, "v1")
        empty_bytes = CloudStore.ObjectBytes(empty_object)
        @test isempty(empty_bytes)
        @test copy(empty_bytes) == UInt8[]
        @test isempty(view(empty_bytes, 1:0))
        @test_throws BoundsError empty_bytes[1]
        @test isempty(empty_bytes.window.bytes)

        huge = CloudStore.ObjectBytes(CloudStore.Object(store, credentials, "huge", typemax(Int), "v1"); blocksize=4)
        tail = view(huge, typemax(Int):typemax(Int))
        @test size(tail) == (1,)
        @test isempty(view(tail, 2:1))
        @test isempty(view(huge, (big(typemax(Int)) + 1):big(typemax(Int))))
        @test isempty(requests)
    end
end

@testset "ObjectBytes snapshot and transport failures" begin
    with_range_fixture() do host, requests
        for (store, credentials) in range_stores(host)
            headers = HTTP.Headers(["X-Custom" => "retained"])
            object = CloudStore.Object(store, credentials, "bytes", 12, "")
            start = length(requests)
            bytes = CloudStore.ObjectBytes(object; blocksize=4, headers,
                query=Dict("versionId" => "fixture-v1"), retries=0)
            HTTP.setheader(headers, "X-Custom" => "changed")
            @test bytes[1] == 0x61
            @test copy(view(bytes, 6:10)) == codeunits("fghij")
            observed = requests[start + 1:end]
            @test count(r -> r.method == "HEAD", observed) == 1
            @test all(r -> occursin("versionId=fixture-v1", r.target), observed)
            @test all(r -> r.headers["x-custom"] == "retained", observed)
            @test all(r -> r.method == "HEAD" || r.headers["if-match"] == "\"v1\"", observed)
        end
    end
    for mode in (:short, :chunked_short, :wrong_range, :wrong_total, :malformed_range,
                 :missing_range, :oversized, :ignored, :changed, :changed_ignores)
        with_range_fixture(mode) do host, requests
            for (store, credentials) in range_stores(host)
                object = CloudStore.Object(store, credentials, "bytes", 12, "v1")
                bytes = CloudStore.ObjectBytes(object; blocksize=4, retries=0)
                mode in (:changed, :changed_ignores) && @test bytes[1] == 0x61
                @test_throws Exception bytes[5]
                # A failed fill must not make partially received bytes readable.
                @test_throws Exception bytes[5]
                @test bytes.window.offset == -1
                destination = fill(0xff, 8)
                @test_throws Exception copyto!(destination, 3, view(bytes, 3:10), 3, 4)
                @test destination[1:2] == destination[7:8] == [0xff, 0xff]
            end
        end
    end
    for mode in (:missing_tag, :weak_tag)
        with_range_fixture(mode) do host, requests
            store, credentials = first(range_stores(host))
            object = CloudStore.Object(store, credentials, "bytes", 12, "")
            @test_throws ArgumentError CloudStore.ObjectBytes(object)
            @test all(r -> r.method == "HEAD", requests)
        end
    end
end

@testset "ObjectBytes concurrent reads" begin
    expected = repeat(collect(UInt8(0):UInt8(255)), 2)
    with_range_fixture(; data=expected) do host, requests
        store, credentials = first(range_stores(host))
        bytes = CloudStore.ObjectBytes(CloudStore.Object(store, credentials, "bytes", length(expected), "v1"); blocksize=31)
        tasks = [Threads.@spawn([bytes[i] for i in indices]) for indices in (1:60, 300:340, 200:-1:150, 60:100)]
        @test fetch.(tasks) == [expected[indices] for indices in (1:60, 300:340, 200:-1:150, 60:100)]
        @test length(bytes.window.bytes) == 31
    end
    arrived = Threads.Atomic{Int}(0)
    before_range = (lo, hi) -> begin
        Threads.atomic_add!(arrived, 1)
        timedwait(() -> arrived[] == 4, 10) == :ok || error("bulk reads did not overlap")
    end
    with_range_fixture(; data=expected, before_range) do host, requests
        store, credentials = first(range_stores(host))
        bytes = CloudStore.ObjectBytes(CloudStore.Object(store, credentials, "bytes", length(expected), "v1"); blocksize=31)
        tasks = [Threads.@spawn(copy(view(bytes, indices))) for indices in (1:60, 100:159, 200:259, 400:459)]
        @test fetch.(tasks) == [expected[indices] for indices in (1:60, 100:159, 200:259, 400:459)]
        @test arrived[] == 4
    end
end

function objectbytes_zip_fixture(; large_blocks=32)
    io = IOBuffer()
    ZipArchives.ZipWriter(io) do writer
        ZipArchives.zip_writefile(writer, "selected.txt", codeunits("selected content\n"))
        ZipArchives.zip_newfile(writer, "large.bin")
        block = zeros(UInt8, 1 << 20)
        for _ in 1:large_blocks
            write(writer, block)
        end
        ZipArchives.zip_newfile(writer, "compressed.txt"; compress=true)
        write(writer, "compressed content\n"^100)
    end
    return take!(io)
end

@testset "ObjectBytes signed S3 and Azure ZIP reads" begin
    data = objectbytes_zip_fixture(; large_blocks=1)
    for emulator in (Minio, Azurite)
        emulator.with() do conf
            credentials, store = conf
            object = CloudStore.put(store, "archive.zip", data; credentials)
            bytes = CloudStore.ObjectBytes(object; blocksize=1024, retries=0)
            archive = ZipArchives.ZipReader(bytes)
            @test ZipArchives.zip_names(archive) == ["selected.txt", "large.bin", "compressed.txt"]
            @test ZipArchives.zip_readentry(archive, "selected.txt", String) == "selected content\n"
            @test ZipArchives.zip_readentry(archive, "compressed.txt", String) == "compressed content\n"^100
            @test bytes[1] == data[1]

            replacement = fill(0x7f, length(data))
            CloudStore.put(store, "archive.zip", replacement; credentials)
            @test bytes[1] == data[1]
            @test_throws Exception bytes[end - 4096]
            @test_throws Exception copy(view(bytes, 2049:2080))
            current = CloudStore.Object(store, credentials, "archive.zip", length(data), "")
            @test copy(view(CloudStore.ObjectBytes(current), 2049:2080)) == replacement[2049:2080]
        end
    end
end

function objectbytes_transferred(requests, total)
    sum(requests; init=0) do request
        request.method == "GET" || return 0
        range = get(request.headers, "range", "")
        isempty(range) && return total
        lo, hi = parse.(Int, match(r"^bytes=(\d+)-(\d+)$", range).captures)
        return hi - lo + 1
    end
end

@testset "ObjectBytes reads a selected ZIP entry without the full archive" begin
    data = objectbytes_zip_fixture()
    with_range_fixture(; data) do host, requests
        for (store, credentials) in range_stores(host)
            object = CloudStore.Object(store, "archive.zip"; credentials)
            empty!(requests)
            full = CloudStore.get(store, "archive.zip"; credentials, allowMultipart=false)
            baseline = ZipArchives.ZipReader(full)
            @test ZipArchives.zip_readentry(baseline, "selected.txt", String) == "selected content\n"
            @test objectbytes_transferred(requests, length(data)) == length(data)
            empty!(requests)

            bytes = CloudStore.ObjectBytes(object)
            archive = ZipArchives.ZipReader(bytes)
            @test ZipArchives.zip_names(archive) == ZipArchives.zip_names(baseline)
            @test ZipArchives.zip_readentry(archive, "selected.txt", String) == "selected content\n"
            transferred = objectbytes_transferred(requests, length(data))
            @test transferred < length(data) ÷ 100
            @test length(requests) <= 8
            @test length(bytes.window.bytes) == 64 * 1024
            @test Base.summarysize(bytes) < 2 * 64 * 1024
            @test Base.summarysize(archive) < 3 * 64 * 1024
            @test Base.summarysize(baseline) > length(data)
            @info "ZIP selective read" provider=nameof(typeof(store)) object_bytes=length(data) transferred_bytes=transferred requests=length(requests) retained_reader_bytes=Base.summarysize(bytes) retained_archive_bytes=Base.summarysize(archive)
            @test ZipArchives.zip_readentry(archive, "compressed.txt", String) == "compressed content\n"^100
        end
    end
end
