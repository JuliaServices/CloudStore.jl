# Loopback-only fixture for range-download and ObjectBytes benchmarks. Run separately so
# server allocations are not included in client allocation measurements.
using CloudStore, Sockets
const HTTP = CloudStore.API.HTTP
const DATA = length(ARGS) >= 2 ? read(ARGS[2]) : repeat(collect(UInt8(0):UInt8(255)), 1 << 17)
const REQUESTS = Threads.Atomic{Int}(0)
const TRANSFERRED = Threads.Atomic{Int}(0)
port = isempty(ARGS) ? 63088 : parse(Int, ARGS[1])
println("range benchmark server: http://127.0.0.1:$port")
flush(stdout)
HTTP.serve("127.0.0.1", port; verbose=false) do req
    path = first(split(req.target, '?'; limit=2))
    if path == "/stats"
        counts = string(Threads.atomic_xchg!(REQUESTS, 0), ',', Threads.atomic_xchg!(TRANSFERRED, 0))
        return HTTP.Response(200, ["Content-Type" => "text/plain"], counts)
    end
    n = parse(Int, last(split(path, '/')))
    0 <= n <= length(DATA) || return HTTP.Response(400)
    tag = "\"n$n\""
    headers = ["ETag" => tag, "Content-Type" => "application/octet-stream"]
    if req.method == "HEAD"
        return HTTP.Response(200, headers, view(DATA, 1:n))
    end
    range = HTTP.header(req, "Range", "")
    Threads.atomic_add!(REQUESTS, 1)
    Threads.atomic_add!(TRANSFERRED, isempty(range) ? n : 0)
    isempty(range) && return HTTP.Response(200, headers, view(DATA, 1:n))
    m = match(r"^bytes=(\d+)-(\d+)$", range)
    lo, hi = parse.(Int, m.captures)
    HTTP.header(req, "If-Match", tag) == tag || return HTTP.Response(412)
    Threads.atomic_add!(TRANSFERRED, hi - lo + 1)
    if occursin("/staggered/", path)
        sleep(0.002 * ((lo ÷ (1 << 20)) % 4))
    end
    push!(headers, "Content-Range" => "bytes $lo-$hi/$n")
    return HTTP.Response(206, headers, view(DATA, lo + 1:hi + 1))
end
