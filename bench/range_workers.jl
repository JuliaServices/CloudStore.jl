# Run bench/range_server.jl separately, then alternate baseline/candidate environments.
# The staggered case models unequal range completion times; it is not an Azure result.
using CloudStore, HTTP, SHA

const HOST = isempty(ARGS) ? "http://127.0.0.1:63088" : first(ARGS)
startswith(HOST, "http://127.0.0.1:") || error("benchmark requires the loopback fixture")
const N = 32 << 20
const DEST = zeros(UInt8, N)
const EXPECTED = sha256(repeat(collect(UInt8(0):UInt8(255)), N ÷ 256))
const STORE = CloudStore.Blobs.Container("ranges", "fixture"; host=HOST)
const CREDENTIALS = CloudStore.Azure.Credentials("fixture", "Zml4dHVyZQ==")
cpuclock() = ccall(:clock, Clong, ()) / 1.0e6

client = HTTP.Client()
try
    println("source,case,seconds,cpu_seconds,allocated_bytes,gc_seconds,requests,bytes")
    for mode in ("steady", "staggered")
        key = mode == "steady" ? string(N) : "staggered/$N"
        operation() = CloudStore.get(STORE, key, DEST; credentials=CREDENTIALS, client,
            partSize=1 << 20, batchSize=4, multipartThreshold=1)
        for _ in 1:2
            @assert sha256(operation()) == EXPECTED
        end
        HTTP.get("$HOST/stats"; client)
        GC.gc()
        cpu = cpuclock()
        sample = @timed operation()
        cpu = cpuclock() - cpu
        @assert sha256(sample.value) == EXPECTED
        requests, bytes = parse.(Int, split(String(HTTP.get("$HOST/stats"; client).body), ','))
        @assert requests == 32 && bytes == N
        println(join((pathof(CloudStore), mode, sample.time, cpu, sample.bytes,
            sample.gctime, requests, bytes), ','))
        flush(stdout)
    end
finally
    close(client)
end
