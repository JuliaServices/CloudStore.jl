using Base64: base64encode

@testset "Azure commit after a lost response" begin
    # The first block-list commit stores the blob but its response fails (HTTP 500, which
    # HTTP retries), so the retry fails the write condition against that blob: 409 for
    # If-None-Match, 412 for If-Match. The committed block IDs, unique to the upload,
    # show whether it is this upload.
    other = base64encode(fill(UInt8('x'), 64))
    for (status, ours) in ((409, true), (412, true), (409, false)), upload in (:put, :stream)
        methods = Channel{String}(16)
        committed = String[]
        commits = Ref(0)
        server = HTTP.serve!(0; listenany=true, verbose=false) do request
            put!(methods, request.method)
            query = HTTP.queryparams(HTTP.URI(request.target))
            if request.method == "GET"
                ids = ours ? committed : [other, other]
                blocks = join("<Block><Name>$id</Name><Size>3</Size></Block>" for id in ids)
                return HTTP.Response(200, ["ETag" => "\"0x8DOURS\""],
                    "<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList><CommittedBlocks>$blocks</CommittedBlocks></BlockList>")
            end
            query["comp"] == "block" && return HTTP.Response(201)
            commits[] += 1
            commits[] > 1 && return HTTP.Response(status, "<Error><Code>Conflict</Code></Error>")
            append!(committed, m[1] for m in eachmatch(r"<Latest>([^<]+)</Latest>", String(request.body)))
            return HTTP.Response(500, "<Error><Code>InternalError</Code></Error>")
        end
        try
            container = Blobs.Container("container", "account"; host="http://127.0.0.1:$(HTTP.port(server))")
            credentials = Blobs.Credentials("account", base64encode(fill(UInt8(1), 64)))
            headers = status == 409 ? ["If-None-Match" => "*"] : ["If-Match" => "\"0x8DOLD\""]
            result = try
                if upload == :put
                    Blobs.put(container, "key", UInt8[1, 2, 3, 4, 5, 6]; credentials, headers,
                        multipartThreshold=1, partSize=3, batchSize=1).eTag
                else
                    CloudStore.MultipartUploadStream(container, "key"; credentials, headers) do io
                        write(io, UInt8[1, 2, 3])
                        write(io, UInt8[4, 5, 6])
                    end
                end
            catch exception
                exception
            end
            close(methods)
            # Two blocks, the failed commit and its retries (HTTP 1 also retries a 409), then Get Block List.
            sent = collect(methods)
            @test length(sent) >= 5 && all(==("PUT"), sent[1:end-1]) && sent[end] == "GET"
            @test length(committed) == 2
            if ours
                @test result == "0x8DOURS"
            else
                @test result isa StatusError && result.status == status
            end
        finally
            close(server)
        end
    end
end

@testset "Multipart write conditions on S3 and Azure servers" begin
    # Another writer may store the key while the parts upload. S3 checks write conditions
    # only on completion and Azure only on the block-list commit, so they must carry them.
    data = rand(UInt8, 5 * 2^20 + 1024)
    theirs = UInt8[9, 9, 9]
    for (with, created) in ((Minio.with, 412), (Azurite.with, 409))
        with() do conf
            credentials, store = conf
            for condition in (:create, :match), mode in (:put, :stream, :streamdo), race in (false, true)
                key = "$condition-$mode-$race"
                headers = condition == :create ? ["If-None-Match" => "*"] :
                    ["If-Match" => "\"$(CloudStore.put(store, key, UInt8[1]; credentials).eTag)\""]
                competitor() = race && CloudStore.put(store, key, theirs; credentials)
                result = try
                    if mode == :put
                        CloudStore.put(store, key, data; credentials, headers, multipartThreshold=2^20,
                            partSize=5 * 2^20, batchSize=1, progress=(_, n) -> n < length(data) && competitor())
                    elseif mode == :stream
                        io = CloudStore.MultipartUploadStream(store, key; credentials, headers)
                        write(io, data[1:5*2^20])
                        wait(io)
                        competitor()
                        write(io, data[5*2^20+1:end])
                        close(io)
                    else
                        CloudStore.MultipartUploadStream(store, key; credentials, headers,
                                concurrent_writes_to_channel=2) do io
                            write(io, data[1:5*2^20])
                            wait(io)
                            competitor()
                            write(io, data[5*2^20+1:end])
                        end
                    end
                catch exception
                    exception
                end
                if race
                    @test result isa StatusError && result.status == (condition == :create ? created : 412)
                    @test CloudStore.get(store, key; credentials) == theirs
                else
                    @test !(result isa Exception)
                    @test CloudStore.get(store, key; credentials) == data
                end
            end
        end
    end
end
