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
                    io = IOBuffer()
                    write(io, data)
                    seekstart(io)
                    out = similar(data)
                    for (name, operation) in (
                        ("put-vector", () -> CloudStore.put(store, "bench", data; credentials, client, require_ssl_verification=true)),
                        ("put-view", () -> CloudStore.put(store, "bench", view(data, 1:n); credentials, client, require_ssl_verification=true)),
                        ("put-iobuffer", () -> begin
                            seekstart(io)
                            CloudStore.put(store, "bench", io; credentials, client, require_ssl_verification=true)
                        end),
                        ("get-preallocated", () -> CloudStore.get(store, "bench", out; credentials, client, require_ssl_verification=true)),
                        ("get-allocated", () -> CloudStore.get(store, "bench"; credentials, client, require_ssl_verification=true)),
                    )
                        allocated, seconds = measure(operation)
                        startswith(name, "get") && @assert RESULT[] == data
                        println(nameof(emulator), ',', name, ',', n, ',', allocated, ',', seconds)
                        flush(stdout)
                        if "--check" in ARGS
                            # Per-request/part metadata may vary by runtime. Reject
                            # an extra payload-sized allocation on buffered paths.
                            parts = cld(n, 8 << 20)
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
