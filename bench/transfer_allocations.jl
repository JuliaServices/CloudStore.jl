# Run with an environment containing the CloudStore/CloudBase/HTTP stack to check:
# julia --threads=4 --project=... bench/transfer_allocations.jl
# Uses local MinIO and Azurite processes. No cloud account is accessed.
using CloudStore, CloudBase, HTTP
using CloudBase.CloudTest

const RESULT = Ref{Any}()
function measure(f)
    RESULT[] = f()
    allocated = Int[]
    seconds = Float64[]
    for _ in 1:3
        GC.gc()
        sample = @timed f()
        RESULT[] = sample.value
        push!(allocated, sample.bytes)
        push!(seconds, sample.time)
    end
    return minimum(allocated), minimum(seconds)
end

function replay(f, parts)
    attempts = (Threads.Atomic{Int}(0), Threads.Atomic{Int}(0))
    # These fixtures have 1 MiB or 8 MiB data requests. Exclude metadata PUTs
    # such as Azure's block-list commit, and leave multipart selection unchanged.
    isdata(req) = req.method == "GET" || (req.method == "PUT" && req.content_length >= 1 << 20)
    trace = function(ev)
        if ev isa HTTP.RequestEvent && isdata(ev.request)
            @assert ev.attempt in (1, 2)
            Threads.atomic_add!(attempts[ev.attempt], 1)
        end
        return nothing
    end
    retry_if = (attempt, err, req, resp) -> attempt == 1 && resp !== nothing && isdata(req)
    result = f((; trace, retry_if, retries=1,
        retry_bucket=HTTP.RetryBucket(backoff_scale_factor_ms=0, max_backoff_secs=0)))
    @assert attempts[1][] == attempts[2][] == parts
    return result
end

function main()
    println("Julia=", VERSION, " threads=", Threads.nthreads())
    for mod in (CloudStore, CloudBase, HTTP)
        println(nameof(mod), "=", Base.pkgversion(mod), " path=", pathof(mod))
    end
    println("provider,operation,payload_bytes,allocated_bytes,seconds")
    for emulator in (Minio, Azurite)
        emulator.with(; debug=true) do conf
            credentials, store = conf
            # Azurite uses its bundled self-signed certificate on loopback only.
            transport = emulator === Azurite ? HTTP.Transport(tls_config=HTTP.TLS.Config(verify_peer=false, verify_hostname=false)) : HTTP.Transport()
            client = HTTP.Client(; transport)
            try
                for n in (1 << 20, 16 << 20)
                    data = fill(0x61, n)
                    stringbytes = codeunits(repeat("a", n))
                    io = IOBuffer()
                    write(io, data)
                    seekstart(io)
                    out = similar(data)
                    parts = cld(n, 8 << 20)
                    for (name, operation) in (
                        ("put-vector", () -> CloudStore.put(store, "bench", data; credentials, client, require_ssl_verification=true)),
                        ("put-codeunits", () -> CloudStore.put(store, "bench", stringbytes; credentials, client, require_ssl_verification=true)),
                        ("put-view", () -> CloudStore.put(store, "bench", view(data, 1:n); credentials, client, require_ssl_verification=true)),
                        ("put-iobuffer", () -> begin
                            seekstart(io)
                            CloudStore.put(store, "bench", io; credentials, client, require_ssl_verification=true)
                        end),
                        ("get-preallocated", () -> CloudStore.get(store, "bench", out; credentials, client, require_ssl_verification=true)),
                        ("get-allocated", () -> CloudStore.get(store, "bench"; credentials, client, require_ssl_verification=true)),
                        ("put-vector-retry", () -> replay(parts) do retry_options
                            CloudStore.put(store, "bench", data; credentials, client, require_ssl_verification=true, retry_options...)
                        end),
                        ("get-preallocated-retry", () -> replay(parts) do retry_options
                            fill!(out, 0xff)
                            CloudStore.get(store, "bench", out; credentials, client, require_ssl_verification=true, retry_options...)
                        end),
                    )
                        allocated, seconds = measure(operation)
                        startswith(name, "get") && @assert RESULT[] == data
                        println(nameof(emulator), ',', name, ',', n, ',', allocated, ',', seconds)
                        flush(stdout)
                        if "--check" in ARGS
                            # Per-request/part metadata may vary by runtime. Reject
                            # an extra payload-sized allocation on buffered paths.
                            budget = (256 << 10) * (1 + parts)
                            name == "get-allocated" && (budget += n)
                            @assert allocated < budget
                        end
                    end
                    @assert out == data
                end
            finally
                close(client)
            end
        end
    end
end

main()
