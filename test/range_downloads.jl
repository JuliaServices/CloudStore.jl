# A raw loopback fixture can send truncated and chunked bodies without a server
# library repairing the intentionally inconsistent response headers.
function with_range_fixture(f, mode=:valid; data=collect(codeunits("abcdefghijkl")), before_range=nothing, content_encoding=nothing)
    listener = Sockets.listen(Sockets.IPv4("127.0.0.1"), 0)
    port = Sockets.getsockname(listener)[2]
    requests = Any[]
    attempts = Dict{String,Int}()
    request_lock = ReentrantLock()
    server = @async begin
        @sync while isopen(listener)
            socket = try
                Sockets.accept(listener)
            catch
                isopen(listener) && rethrow()
                break
            end
            @async try
                line = readline(socket)
                isempty(line) && return nothing
                method, target, _ = split(line; limit=3)
                headers = Dict{String,String}()
                while true
                    line = readline(socket)
                    isempty(line) && break
                    name, value = split(line, ':'; limit=2)
                    headers[lowercase(name)] = strip(value)
                end
                range = get(headers, "range", "bytes=0-$(length(data) - 1)")
                attempt = lock(request_lock) do
                    push!(requests, (; method, target, headers))
                    id = string(method, ' ', target, ' ', range)
                    attempts[id] = get(attempts, id, 0) + 1
                end
                tag = mode == :weak_tag ? "W/\"v1\"" : "\"v1\""
                status = 200
                response_headers = Pair{String,String}[]
                content_encoding === nothing || push!(response_headers, "Content-Encoding" => content_encoding)
                body = data
                if method == "GET"
                    m = match(r"^bytes=(\d+)-(\d+)$", range)
                    lo, hi = parse.(Int, m.captures)
                    before_range === nothing || before_range(lo, hi)
                    body = data[lo + 1:hi + 1]
                    status = haskey(headers, "range") ? 206 : 200
                    range_lo = mode == :wrong_range ? lo + 1 : lo
                    range_hi = mode == :wrong_range ? hi + 1 : hi
                    total = length(data) + (mode == :wrong_total)
                    content_range = "bytes $range_lo-$range_hi/$total"
                    mode == :malformed_range && (content_range *= " garbage")
                    mode == :missing_range || push!(response_headers, "Content-Range" => content_range)
                    if mode in (:short, :chunked_short)
                        body = body[1:end - 1]
                    elseif mode == :oversized
                        body = vcat(body, 0xff)
                    elseif mode == :ignored
                        status, body = 200, data
                    elseif mode in (:changed, :changed_ignores) && lo > 0
                        tag = "\"v2\""
                        body = collect(codeunits(uppercase(String(body))))
                        if mode == :changed && get(headers, "if-match", "") == "\"v1\""
                            status, body = 412, UInt8[]
                        end
                    elseif mode == :status_retry && attempt == 1
                        status, body = 503, UInt8[]
                    end
                end
                mode == :missing_tag || push!(response_headers, "ETag" => tag)
                chunked = method == "GET" && mode in (:chunked, :chunked_short)
                push!(response_headers, chunked ? "Transfer-Encoding" => "chunked" : "Content-Length" => string(length(body)))
                push!(response_headers, "Connection" => "close")
                write(socket, "HTTP/1.1 $status fixture\r\n")
                for (name, value) in response_headers
                    write(socket, name, ": ", value, "\r\n")
                end
                write(socket, "\r\n")
                if method != "HEAD"
                    if mode == :truncated_retry && attempt == 1
                        write(socket, view(body, 1:2))
                    elseif chunked
                        write(socket, string(length(body); base=16), "\r\n", body, "\r\n0\r\n\r\n")
                    else
                        write(socket, body)
                    end
                end
            finally
                close(socket)
            end
        end
    end
    try
        return f("http://127.0.0.1:$port", requests)
    finally
        close(listener)
        wait(server)
    end
end

function range_stores(host)
    return ((S3.Bucket("ranges", "us-east-1"; host), CloudStore.AWS.Credentials("fixture", "fixture")),
        (Blobs.Container("ranges", "fixture"; host), CloudStore.Azure.Credentials("fixture", "Zml4dHVyZQ==")))
end

function read_prefetched(object; kw...)
    io = CloudStore.PrefetchedDownloadStream(object, 8; prefetch_multipart_size=4, kw...)
    try
        return read(io)
    finally
        close(io)
    end
end

@testset "Validated ranged downloads" begin
    expected = collect(codeunits("abcdefghijkl"))
    for mode in (:valid, :chunked)
        with_range_fixture(mode) do host, requests
            for (store, credentials) in range_stores(host)
                object = CloudStore.Object(store, credentials, "data", 12, "v1")
                dest = fill(0xff, 10)
                @test copyto!(view(dest, 2:9), 2, object, 5, 4) == 4
                @test dest == vcat(fill(0xff, 2), expected[5:8], fill(0xff, 4))
                fill!(dest, 0xff)
                @test copyto!(view(dest, 1:2:9), 2, object, 5, 4) == 4
                @test dest[3:2:9] == expected[5:8]
                @test dest[1] == 0xff && all(==(0xff), dest[2:2:10])
                @test CloudStore.API.getRange(object, 12, 1) == expected[12:12]
                for output in (nothing, zeros(UInt8, 12), IOBuffer(), tempname())
                    try
                        result = CloudStore.get(store, "data", output; credentials,
                            multipartThreshold=1, partSize=4, batchSize=3, retries=0)
                        actual = result isa IO ? take!(result) : result isa String ? read(result) : result
                        @test actual == expected
                    finally
                        output isa String && isfile(output) && rm(output)
                    end
                end
                @test read_prefetched(object; retries=0) == expected
            end
            @test all(r -> r.method != "GET" || r.headers["if-match"] == "\"v1\"", requests)
        end
    end

    for mode in (:short, :chunked_short, :wrong_range, :wrong_total, :malformed_range,
                 :missing_range, :oversized, :ignored, :changed, :changed_ignores)
        with_range_fixture(mode) do host, requests
            for (store, credentials) in range_stores(host)
                object = CloudStore.Object(store, credentials, "data", 12, "v1")
                dest = fill(0xff, 10)
                @test_throws Exception copyto!(dest, 3, object, 5, 4)
                @test all(==(0xff), dest[1:2]) && all(==(0xff), dest[7:10])
                for output in (zeros(UInt8, 12), IOBuffer(), tempname())
                    try
                        @test_throws Exception CloudStore.get(store, "data", output; credentials,
                            multipartThreshold=1, partSize=4, batchSize=3, retries=0)
                        output isa IO && @test isopen(output)
                    finally
                        output isa String && isfile(output) && rm(output)
                    end
                end
                @test_throws Exception read_prefetched(object; retries=0)
            end
        end
    end

    for mode in (:missing_tag, :weak_tag)
        with_range_fixture(mode) do host, requests
            for (store, credentials) in range_stores(host)
                @test_throws ArgumentError CloudStore.get(store, "data"; credentials,
                    multipartThreshold=1, partSize=4, retries=0)
                object = CloudStore.Object(store, credentials, "data", 12, mode == :weak_tag ? "W/\"v1\"" : "")
                @test_throws ArgumentError read_prefetched(object; retries=0)
                @test_throws ArgumentError copyto!(zeros(UInt8, 4), 1, object, 1, 4)
                @test CloudStore.get(store, "data"; credentials, allowMultipart=false, retries=0) == expected
            end
            @test all(r -> r.method != "GET" || !haskey(r.headers, "range"), requests)
        end
    end
end

@testset "Range bounds, metadata refresh and caller controls" begin
    with_range_fixture() do host, requests
        for (store, credentials) in range_stores(host)
            object = CloudStore.Object(store, credentials, "data", 12, "")
            start = length(requests)
            @test read_prefetched(object; retries=0) == codeunits("abcdefghijkl")
            @test count(r -> r.method == "HEAD", requests[start + 1:end]) == 1
            @test copyto!(zeros(UInt8, 4), 1, object, 9, 4) == 4
            @test_throws ArgumentError copyto!(zeros(UInt8, 4), 1, object, 1, -1)
            @test_throws BoundsError copyto!(zeros(UInt8, 4), 0, object, 1, 1)
            @test_throws BoundsError copyto!(zeros(UInt8, 4), 1, object, 0, 1)
            @test_throws BoundsError copyto!(UInt8[], 1, object, 1, 1)
            @test_throws BoundsError copyto!(zeros(UInt8, 4), 1, object, 13, 1)
            @test_throws ArgumentError copyto!(zeros(UInt8, 4), 1, object, 11, 4)
            @test_throws ArgumentError copyto!(zeros(UInt8, 4), 2, object, 1, 4)
            @test_throws ArgumentError copyto!(zeros(UInt8, 4), 1, object, 1, typemax(Int))
            start = length(requests)
            @test copyto!(UInt8[], 1, object, 13, 0) == 0
            empty_object = CloudStore.Object(store, credentials, "empty", 0, "")
            @test copyto!(UInt8[], 1, empty_object, 1, 0) == 0
            @test read_prefetched(empty_object) == UInt8[]
            @test length(requests) == start
            stale = CloudStore.Object(store, credentials, "data", 11, "")
            @test_throws ArgumentError copyto!(zeros(UInt8, 4), 1, stale, 1, 4)

            for condition in ("If-Match" => "\"v1\", \"v2\"", "If-None-Match" => "\"v2\"",
                              "If-Unmodified-Since" => "Wed, 21 Oct 2015 07:28:00 GMT")
                headers = HTTP.Headers([condition, "X-Custom" => "retained"])
                original = collect(headers)
                start = length(requests)
                key = CloudStore.API.parsedURLResource("data?versionId=fixture-v1")
                @test CloudStore.get(store, key; credentials, headers,
                    multipartThreshold=1, partSize=4, batchSize=2, retries=0) == codeunits("abcdefghijkl")
                @test collect(headers) == original
                for request in requests[start + 1:end]
                    @test occursin("versionId=fixture-v1", request.target)
                    @test request.headers[lowercase(first(condition))] == last(condition)
                    @test request.headers["x-custom"] == "retained"
                    first(condition) != "If-Match" && @test !haskey(request.headers, "if-match")
                end
            end
            headers = HTTP.Headers(["Range" => "bytes=4-7", "If-Match" => "\"v1\""])
            @test CloudStore.get(store, "data"; credentials, headers,
                allowMultipart=false, retries=0) == codeunits("efgh")
        end
    end
end

@testset "Ranged download retry destination" begin
    modes = isdefined(HTTP, :BytesBody) ? (:status_retry,) : (:status_retry, :truncated_retry)
    for mode in modes
        with_range_fixture(mode) do host, requests
            for (store, credentials) in range_stores(host)
                object = CloudStore.Object(store, credentials, "data", 12, "v1")
                dest = fill(0xff, 6)
                retry_kw = isdefined(HTTP, :BytesBody) ?
                    (; retry_bucket=HTTP.RetryBucket(backoff_scale_factor_ms=0, max_backoff_secs=0)) :
                    (; retry_delays=Base.ExponentialBackOff(n=1, first_delay=0.0, max_delay=0.0))
                @test CloudStore.API.getRange!(view(dest, 2:5), object, 5, 4; retries=1, retry_kw...) == 4
                @test dest == vcat(0xff, codeunits("efgh"), 0xff)
                @test count(r -> r.method == "GET" && occursin("/data", r.target), requests) >= 2
            end
        end
    end
end

@testset "Compressed range reassembly" begin
    plain = collect(codeunits(repeat("range-safe gzip bytes\n", 100)))
    compressed = transcode(CloudStore.API.CodecZlib.GzipCompressor, plain)
    with_range_fixture(; data=compressed, content_encoding="gzip") do host, requests
        for (store, credentials) in range_stores(host)
            object = CloudStore.Object(store, credentials, "data", length(compressed), "v1")
            @test read_prefetched(object; retries=0) == compressed
            partial = zeros(UInt8, 4)
            @test copyto!(partial, 1, object, 3, 4) == 4
            @test partial == compressed[3:6]
            for decompress in (false, true), output in (nothing, zeros(UInt8, length(plain)), IOBuffer(), tempname())
                try
                    result = CloudStore.get(store, "data", output; credentials, decompress,
                        multipartThreshold=1, partSize=7, batchSize=3, retries=0)
                    actual = result isa IO ? take!(result) : result isa String ? read(result) : result
                    @test actual == (decompress ? plain : compressed)
                finally
                    output isa String && isfile(output) && rm(output)
                end
            end
        end
    end
end

struct RangeTestSink{F} <: IO
    buffer::IOBuffer
    onwrite::F
end
function Base.unsafe_write(sink::RangeTestSink, bytes::Ptr{UInt8}, n::UInt)
    sink.onwrite(n)
    return unsafe_write(sink.buffer, bytes, n)
end
Base.flush(sink::RangeTestSink) = flush(sink.buffer)

@testset "Ranged stream overlap and write failure" begin
    written = Channel{Nothing}(1)
    # The second response cannot finish until the first part reaches the sink.
    # This checks overlap without depending on task scheduling or elapsed time.
    before_range = (lo, hi) -> if lo == 4
        timedwait(() -> isready(written), 30) == :ok || error("first part was not streamed")
        take!(written)
    end
    with_range_fixture(; before_range) do host, requests
        store, credentials = first(range_stores(host))
        sink = RangeTestSink(IOBuffer(), bytes -> position(sink.buffer) == 0 && put!(written, nothing))
        CloudStore.get(store, "data", sink; credentials,
            multipartThreshold=1, partSize=4, batchSize=3, retries=0)
        @test take!(sink.buffer) == codeunits("abcdefghijkl")
    end
    with_range_fixture() do host, requests
        store, credentials = first(range_stores(host))
        sink = RangeTestSink(IOBuffer(), _ -> error("destination write failed"))
        @test_throws Exception CloudStore.get(store, "data", sink; credentials,
            multipartThreshold=1, partSize=4, batchSize=3, retries=0)
        @test count(r -> r.method == "GET", requests) == 3
    end
end
