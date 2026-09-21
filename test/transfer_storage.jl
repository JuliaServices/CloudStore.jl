@testset "Borrowed upload buffers" begin
    for n in (1024, 1 << 20)
        input = IOBuffer()
        write(input, fill(0x61, n))
        seek(input, 2)
        prepared = CloudStore.API.prepBody(input, false, false)
        @test length(prepared) == n - 2
        @test pointer(prepared) == pointer(input.data, 3)
        @test position(input) == 2
        CloudStore.API.prepBody(input, false, false)
        @test minimum(@allocated(CloudStore.API.prepBody(input, false, false)) for _ in 1:5) < 1024
    end
    data = fill(0x61, 1024)
    for input in (data, view(data, 2:1023))
        body = CloudStore.API.prepBodyMultipart(input, false, false)
        part = CloudStore.API._read(body, 100)
        @test pointer(part) == pointer(input)
        @test length(part) == 100
        @test position(body) == 100
        @test pointer(CloudStore.API.prepBody(IOBuffer(input), false, false)) == pointer(input)
    end
    text = codeunits(repeat("hello", 100))
    part = CloudStore.API._read(IOBuffer(text), 100)
    @test part == text[1:100]
    if isdefined(HTTP, :BytesBody)
        @test pointer(part) == pointer(text)
    else
        @test part isa Vector{UInt8}
    end
    headers = HTTP.Headers(["X-Example" => "value"])
    copied = CloudStore.API.transferheaders(headers)
    HTTP.setheader(copied, "X-Example" => "changed")
    @test HTTP.header(headers, "X-Example") == "value"
end

if isdefined(HTTP, :Client)
@testset "Default signed transfer ownership" begin
    for emulator in (Minio, Azurite)
        emulator.with(; debug=true) do conf
            credentials, store = conf
            # Azurite uses its bundled self-signed certificate on loopback only.
            transport = emulator === Azurite ? HTTP.Transport(tls_config=HTTP.TLS.Config(verify_peer=false, verify_hostname=false)) : HTTP.Transport()
            client = HTTP.Client(; transport)
            try
                for n in (1024, 10 * 1024^2 + 1)
                    data = rand(UInt8, n)
                    headers = HTTP.Headers(["Content-Type" => "application/octet-stream"])
                    lock = ReentrantLock()
                    attempts = Any[]
                    trace = function(ev)
                        if ev isa HTTP.RequestEvent && ev.request.method == "PUT" && ev.request.content_length >= 1024
                            Base.lock(lock) do
                                push!(attempts, ev.request)
                            end
                            @test Base.mightalias(ev.request.body.data, data)
                        end
                    end
                    # Keep default multipart selection. Force replay after a successful
                    # first PUT; the emulator validates both attempts' signatures.
                    retry_if = (attempt, err, req, resp) -> req.method == "PUT" && attempt == 1 && resp !== nothing
                    CloudStore.put(store, "owned", data; credentials, client, require_ssl_verification=true, headers, trace, retry_if,
                        retries=1, retry_bucket=HTTP.RetryBucket(backoff_scale_factor_ms=0, max_backoff_secs=0))
                    @test length(attempts) >= 2
                    @test length(Set(objectid(req.headers) for req in attempts)) == length(attempts)
                    @test collect(headers) == ["Content-Type" => "application/octet-stream"]
                    @test CloudStore.get(store, "owned"; credentials, client, require_ssl_verification=true) == data
                    out = zeros(UInt8, n)
                    @test CloudStore.get(store, "owned", out; credentials, client, require_ssl_verification=true) === out
                    @test out == data
                    padded = fill(0xff, n + 2)
                    result = CloudStore.get(store, "owned", view(padded, 2:n+1); credentials, client, require_ssl_verification=true)
                    @test result == data
                    @test padded[1] == padded[end] == 0xff
                end
            finally
                close(client)
            end
        end
    end
end

end # HTTP 2 client ownership contract
