@testset "Prefetch request concurrency" begin
    part_size = 4
    range_count = 2 * (Threads.nthreads() + 1) + 1
    data = UInt8[mod(i, 256) for i in 1:part_size * range_count]
    for provider in 1:2, constructor in (:object, :store),
            cap in (nothing, 1, Threads.nthreads() + 1, typemax(Int))
        expected = min(range_count, something(cap, Threads.nthreads()))
        started = Threads.Atomic{Int}(0)
        release = Base.Event()
        before_range = (_, _) -> begin
            Threads.atomic_add!(started, 1)
            wait(release)
        end
        with_range_fixture(; data, before_range) do host, requests
            store, credentials = range_stores(host)[provider]
            options = cap === nothing ? (;) : (; prefetch_concurrency=cap)
            io = if constructor == :object
                # Missing metadata exercises forwarding through rangeObject too.
                object = CloudStore.Object(store, credentials, "data", length(data), "")
                CloudStore.PrefetchedDownloadStream(object, length(data);
                    prefetch_multipart_size=part_size, retries=0, options...)
            else
                CloudStore.PrefetchedDownloadStream(store, "data", length(data);
                    credentials, prefetch_multipart_size=part_size, retries=0, options...)
            end
            try
                # Held responses prove overlap without relying on transfer speed.
                @test timedwait(() -> started[] >= expected, 10.0) == :ok
                @test timedwait(() -> started[] > expected, 0.1) == :timed_out
                notify(release)
                @test read(io) == data
                @test eof(io)
            finally
                notify(release)
                close(io)
            end
            @test !isopen(io)
            @test count(r -> r.method == "HEAD", requests) == 1
            @test count(r -> r.method == "GET", requests) == range_count
        end
    end
end

@testset "Prefetch concurrency validation precedes requests" begin
    with_range_fixture() do host, requests
        for (store, credentials) in range_stores(host), cap in (0, -1)
            object = CloudStore.Object(store, credentials, "data", 12, "")
            @test_throws ArgumentError CloudStore.PrefetchedDownloadStream(object;
                prefetch_concurrency=cap)
            @test_throws ArgumentError CloudStore.PrefetchedDownloadStream(store, "data";
                credentials, prefetch_concurrency=cap)
        end
        @test isempty(requests)
    end
end

@testset "Prefetch concurrency is bounded by short objects" begin
    for n in (0, 3, 5)
        data = collect(codeunits("abcde"))[1:n]
        with_range_fixture(; data) do host, requests
            for (store, credentials) in range_stores(host)
                first_request = length(requests) + 1
                io = CloudStore.PrefetchedDownloadStream(store, "data", 8;
                    credentials, prefetch_multipart_size=4,
                    prefetch_concurrency=typemax(Int), retries=0)
                try
                    @test read(io) == data
                    @test eof(io)
                finally
                    close(io)
                end
                @test count(r -> r.method == "GET", requests[first_request:end]) == cld(n, 4)
                @test !isopen(io)
            end
        end
    end
end

@testset "Prefetch range errors with explicit concurrency" begin
    for mode in (:changed, :wrong_range, :short)
        with_range_fixture(mode) do host, requests
            for (store, credentials) in range_stores(host)
                object = CloudStore.Object(store, credentials, "data", 12, "v1")
                io = CloudStore.PrefetchedDownloadStream(object, 8;
                    prefetch_multipart_size=4, prefetch_concurrency=Threads.nthreads() + 1,
                    retries=0)
                try
                    @test_throws Exception read(io)
                finally
                    close(io)
                end
                close(io)
                @test !isopen(io)
            end
        end
    end
end
