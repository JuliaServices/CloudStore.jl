# Run with an environment containing the CloudStore/CloudBase/HTTP versions to compare:
# julia --threads=4 --project=... bench/multipart_uploads.jl
# Local MinIO/Azurite only. Counts exclude input creation and emulator allocations.
using CloudStore, CloudBase, HTTP
using CloudBase.CloudTest: Minio, Azurite

function main()
    println("Julia=", VERSION, " threads=", Threads.nthreads())
    for mod in (CloudStore, CloudBase, HTTP)
        println(nameof(mod), " path=", pathof(mod))
    end
    println("provider,input,payload_bytes,batch_size,sample,allocated_bytes,seconds,gc_seconds")
    for emulator in (Minio, Azurite)
        # Plain HTTP stays on loopback and avoids unrelated TLS first-use costs.
        local_options = emulator === Azurite ? (; use_ssl=false) : (;)
        emulator.with(; debug=true, local_options...) do conf
            credentials, store = conf
            options = isdefined(HTTP, :Client) ? (; client=HTTP.Client()) : (;)
            try
                for n in (1 << 20, 16 << 20, 64 << 20)
                    data = rand(UInt8, n)
                    mktemp() do file, output
                        write(output, data)
                        flush(output)
                        for batchSize in (1, 4), kind in ("vector", "file", "gzip-file")
                            input = kind == "vector" ? data : file
                            compress = kind == "gzip-file"
                            operation() = CloudStore.put(store, "bench", input; credentials, compress,
                                multipartThreshold=1, partSize=8 << 20, batchSize,
                                require_ssl_verification=true, options...)
                            operation()
                            for sample in 1:3
                                GC.gc()
                                result = @timed operation()
                                println(nameof(emulator), ',', kind, ',', n, ',', batchSize, ',',
                                    sample, ',', result.bytes, ',', result.time, ',', result.gctime)
                                flush(stdout)
                            end
                            @assert CloudStore.get(store, "bench"; credentials, decompress=compress,
                                allowMultipart=false, require_ssl_verification=true, options...) == data
                        end
                    end
                end
            finally
                haskey(options, :client) && close(options.client)
            end
        end
    end
end

main()
