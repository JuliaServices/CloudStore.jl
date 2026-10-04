# Use an environment containing CloudStore and the test-only ZipArchives dependency.
# Make a fixture: julia --project=... bench/objectbytes.jl --make /tmp/objectbytes.zip
# Serve separately: julia --project=... bench/range_server.jl 63088 /tmp/objectbytes.zip
# Measure: julia --project=... bench/objectbytes.jl http://127.0.0.1:63088 /tmp/objectbytes.zip
# Server memory, fixture generation and content verification are outside measurements.
using CloudStore, ZipArchives
const HTTP = CloudStore.API.HTTP

if length(ARGS) == 2 && ARGS[1] == "--make"
    ZipArchives.ZipWriter(ARGS[2]) do writer
        ZipArchives.zip_writefile(writer, "selected.txt", codeunits("selected content\n"))
        ZipArchives.zip_newfile(writer, "large.bin")
        block = zeros(UInt8, 1 << 20)
        for _ in 1:32
            write(writer, block)
        end
        ZipArchives.zip_newfile(writer, "compressed.txt"; compress=true)
        write(writer, "compressed content\n"^100)
    end
    exit()
end

length(ARGS) == 2 || error("expected loopback fixture URL and archive path")
host, path = ARGS
startswith(host, "http://127.0.0.1:") || error("benchmark requires the loopback fixture")
const EXPECTED = read(path)
const SIZE = length(EXPECTED)
const KEY = string(SIZE)
const INDICES = [1 + (i * 104729) % SIZE for i in 1:32]
stats() = parse.(Int, split(String(HTTP.get(host * "/stats").body), ','))
median(values) = sort(values)[cld(length(values), 2)]

function measure(f, provider, operation; check)
    for _ in 1:2
        check(f())
    end
    times, allocated, retained, requests, transferred = Float64[], Int[], Int[], Int[], Int[]
    for _ in 1:5
        stats()
        GC.gc()
        result = @timed f()
        counts = stats()
        check(result.value)
        push!(times, result.time)
        push!(allocated, result.bytes)
        push!(retained, Base.summarysize(result.value[1]))
        push!(requests, counts[1])
        push!(transferred, counts[2])
    end
    println(join((provider, operation, SIZE, median(times), median(allocated), median(retained),
        median(requests), median(transferred)), ','))
    flush(stdout)
end

println("Julia=", VERSION, " threads=", Threads.nthreads(), " CloudStore=", pathof(CloudStore))
println("provider,operation,object_bytes,median_seconds,allocated_bytes,retained_reader_bytes,requests,payload_bytes")
stores = ((CloudStore.S3.Bucket("ranges", "us-east-1"; host), CloudStore.AWS.Credentials("fixture", "fixture")),
    (CloudStore.Blobs.Container("ranges", "fixture"; host), CloudStore.Azure.Credentials("fixture", "Zml4dHVyZQ==")))
for (store, credentials) in stores
    provider = nameof(typeof(store))
    object = CloudStore.Object(store, KEY; credentials)
    checkzip = result -> (@assert result[2] == "selected content\n")
    measure(provider, "full-zip"; check=checkzip) do
        bytes = CloudStore.get(store, KEY; credentials, allowMultipart=false)
        archive = ZipArchives.ZipReader(bytes)
        return archive, ZipArchives.zip_readentry(archive, "selected.txt", String)
    end
    measure(provider, "range-zip"; check=checkzip) do
        archive = ZipArchives.ZipReader(CloudStore.ObjectBytes(object))
        return archive, ZipArchives.zip_readentry(archive, "selected.txt", String)
    end
    measure(provider, "scattered-scalars"; check=result -> (@assert result[2] == EXPECTED[INDICES])) do
        bytes = CloudStore.ObjectBytes(object)
        return bytes, [bytes[index] for index in INDICES]
    end
    destination = zeros(UInt8, 4 << 20)
    measure(provider, "bulk-copy"; check=result -> (@assert result[2] === destination && destination == EXPECTED[1025:1024 + length(destination)])) do
        bytes = CloudStore.ObjectBytes(object)
        copyto!(destination, 1, bytes, 1025, length(destination))
        return bytes, destination
    end
end
