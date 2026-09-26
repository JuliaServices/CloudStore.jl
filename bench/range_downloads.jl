# First start bench/range_server.jl in another process. Then run this script in
# matched baseline/candidate environments, e.g. julia --threads=2 --project=... \
# bench/range_downloads.jl http://127.0.0.1:63088
# The fixture uses fake credentials and loopback HTTP; these are not cloud
# service throughput measurements. Setup and content verification are untimed.
using CloudStore
const HTTP = CloudStore.API.HTTP

struct DelayedRangeSink <: IO
    buffer::IOBuffer
    delay::Float64
end
function Base.unsafe_write(io::DelayedRangeSink, bytes::Ptr{UInt8}, n::UInt)
    sleep(io.delay)
    return unsafe_write(io.buffer, bytes, n)
end
Base.flush(io::DelayedRangeSink) = flush(io.buffer)

median(x) = sort(x)[cld(length(x), 2)]
cpuclock() = ccall(:clock, Clong, ()) / 1.0e6
function measure(f, check, provider, operation, n; samples=9)
    for _ in 1:2
        check(f())
    end
    wall, cpu, allocated, allocs = Float64[], Float64[], Int[], Int[]
    for _ in 1:samples
        GC.gc()
        start_cpu = cpuclock()
        result = @timed f()
        push!(cpu, cpuclock() - start_cpu)
        push!(wall, result.time)
        push!(allocated, result.bytes)
        stats = result.gcstats
        push!(allocs, stats.malloc + stats.realloc + stats.poolalloc + stats.bigalloc)
        check(result.value)
    end
    println(join((provider, operation, n, median(wall), minimum(wall), maximum(wall),
        median(cpu), median(allocated), median(allocs)), ','))
    flush(stdout)
end

function main()
    host = isempty(ARGS) ? "http://127.0.0.1:63088" : first(ARGS)
    startswith(host, "http://127.0.0.1:") || error("benchmark requires the loopback fixture")
    println("Julia=", VERSION, " threads=", Threads.nthreads(), " source=", pathof(CloudStore))
    println("provider,operation,payload_bytes,wall_median,wall_min,wall_max,cpu_median,allocated_bytes,allocations")
    stores = ((CloudStore.S3.Bucket("ranges", "us-east-1"; host), CloudStore.AWS.Credentials("fixture", "fixture")),
        (CloudStore.Blobs.Container("ranges", "fixture"; host), CloudStore.Azure.Credentials("fixture", "Zml4dHVyZQ==")))
    for (store, credentials) in stores
        provider = nameof(typeof(store))
        for n in (1 << 10, 1 << 16, 1 << 20, 8 << 20)
            dest = zeros(UInt8, n)
            expected = repeat(collect(UInt8(0):UInt8(255)), n ÷ 256)
            object = CloudStore.Object(store, credentials, string(n), n, "n$n")
            measure(() -> copyto!(dest, 1, object, 1, n),
                result -> (@assert result == n && dest == expected), provider, "copy", n)
        end
        n = 16 << 20
        expected = repeat(collect(UInt8(0):UInt8(255)), n ÷ 256)
        dest = zeros(UInt8, n)
        key = string(n)
        options = (; credentials, partSize=1 << 20, batchSize=4, multipartThreshold=1)
        measure(() -> CloudStore.get(store, key, dest; credentials),
            result -> (@assert result == expected), provider, "default-vector", n)
        measure(() -> CloudStore.get(store, key, dest; options...),
            result -> (@assert result == expected), provider, "multipart-vector", n)
        measure(() -> CloudStore.get(store, key; options...),
            result -> (@assert result == expected), provider, "multipart-allocated", n)
        object = CloudStore.Object(store, credentials, key, n, "n$n")
        measure(() -> begin
            io = CloudStore.PrefetchedDownloadStream(object, 4 << 20; prefetch_multipart_size=1 << 20)
            try
                readbytes!(io, dest, n)
            finally
                close(io)
            end
        end, result -> (@assert result == n && dest == expected), provider, "prefetch", n)
        for (operation, delay, keyprefix) in (("multipart-io", 0.0, ""),
                ("staggered-io", 0.0, "staggered/"), ("staggered-slow-io", 0.002, "staggered/"))
            buffer = IOBuffer(dest; write=true, maxsize=n)
            sink = delay == 0 ? buffer : DelayedRangeSink(buffer, delay)
            measure(() -> begin
                seekstart(buffer)
                CloudStore.get(store, keyprefix * key, sink; options...)
                position(buffer)
            end, result -> (@assert result == n && dest == expected), provider, operation, n)
        end
        path = tempname()
        try
            measure(() -> CloudStore.get(store, key, path; options...),
                result -> (@assert read(result) == expected), provider, "multipart-file", n)
        finally
            isfile(path) && rm(path)
        end
    end
end
main()
