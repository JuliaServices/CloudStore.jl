module UploadWorkerTests

using Test, CloudStore, CodecZlib
import HTTP
using ..UploadBufferTests: UploadStore, uploaded, takeevent
const API = CloudStore.API

@testset "Vector upload workers" begin
    @testset "A completed part starts another before a slow part finishes" begin
        data = UInt8.(1:19)
        started = Channel{Int}(5)
        gates = [Channel{Nothing}(1) for _ in 1:5]
        peak = Ref(0)
        order = Int[]
        store = UploadStore() do part, n
            lock(store.lock) do
                peak[] = max(peak[], store.active)
                push!(order, n)
            end
            put!(started, n)
            take!(gates[n])
        end
        task = @async API.putObjectImpl(store, "rolling", data;
            multipartThreshold=1, partSize=4, batchSize=2)
        try
            first_parts = Set((takeevent(started), takeevent(started)))
            @test first_parts == Set((1, 2))
            for n in 2:4
                put!(gates[n], nothing)
                finished = takeevent(store.finished)
                next = takeevent(started)
                @test finished == n
                @test next == n + 1
                @test !store.completed
            end
            put!(gates[5], nothing)
            finished = takeevent(store.finished)
            @test finished == 5
            @test !istaskdone(task)
            @test !store.completed
            GC.gc()
            @test store.buffers[1] == data[1:4]
            @test pointer(store.buffers[1]) == pointer(data)
            put!(gates[1], nothing)
            object = fetch(task)
            @test uploaded(store) == data
            @test object.size == length(data)
            @test peak[] == 2
            @test sort(order) == collect(1:5)
            @test store.completed && !store.aborted
        finally
            foreach(gate -> isopen(gate) && close(gate), gates)
            try
                wait(task)
            catch
            end
        end
    end

    @testset "Part failure drains active uploads before aborting" begin
        data = UInt8.(1:20)
        started = Channel{Int}(5)
        gates = [Channel{Nothing}(1) for _ in 1:2]
        failed_worker = Ref{Task}()
        store = UploadStore() do part, n
            n == 2 && (failed_worker[] = current_task())
            put!(started, n)
            take!(gates[n])
            n == 2 && error("part retry failed")
        end
        task = @async try
            API.putObjectImpl(store, "failure", data;
                multipartThreshold=1, partSize=4, batchSize=2)
        catch err
            err
        end
        try
            first_parts = Set((takeevent(started), takeevent(started)))
            @test first_parts == Set((1, 2))
            put!(gates[2], nothing)
            finished = takeevent(store.finished)
            @test finished == 2
            timedwait(() -> istaskfailed(failed_worker[]), 10) == :ok || error("failed upload worker did not exit")
            @test !store.aborted && !store.completed && !istaskdone(task)
            @test !isready(started)
            @test store.buffers[1] == data[1:4]
            put!(gates[1], nothing)
            err = fetch(task)
            @test occursin("part retry failed", sprint(showerror, err))
            @test store.aborted && store.active_at_abort == 0 && !store.completed
            @test !isready(started)
            @test sort!(collect(keys(store.parts))) == [1]
        finally
            foreach(gate -> isopen(gate) && close(gate), gates)
            wait(task)
        end
    end

    @testset "Progress failure keeps its exception and drains uploads" begin
        data = UInt8.(1:20)
        started = Channel{Int}(5)
        called = Channel{Tuple{Int,Int}}(5)
        gates = [Channel{Nothing}(1) for _ in 1:2]
        marker = ErrorException("progress failed")
        store = UploadStore() do part, n
            put!(started, n)
            take!(gates[n])
        end
        progress = function(total, written)
            put!(called, (total, written))
            throw(marker)
        end
        task = @async try
            API.putObjectImpl(store, "progress-failure", data;
                multipartThreshold=1, partSize=4, batchSize=2, progress)
        catch err
            err
        end
        try
            first_parts = Set((takeevent(started), takeevent(started)))
            @test first_parts == Set((1, 2))
            put!(gates[2], nothing)
            update = takeevent(called)
            @test update == (20, 4)
            @test !store.aborted && !store.completed && !istaskdone(task)
            @test !isready(started)
            put!(gates[1], nothing)
            @test fetch(task) === marker
            @test store.aborted && store.active_at_abort == 0 && !store.completed
            @test !isready(started)
            @test !isready(called)
            @test sort!(collect(keys(store.parts))) == [1, 2]
        finally
            foreach(gate -> isopen(gate) && close(gate), gates)
            wait(task)
        end
    end

    @testset "Progress callbacks are serial and count completed bytes" begin
        data = UInt8.(1:37)
        updates = Channel{Tuple{Int,Int}}(10)
        active = Threads.Atomic{Int}(0)
        overlapped = Threads.Atomic{Bool}(false)
        progress = function(total, written)
            Threads.atomic_add!(active, 1) == 0 || (overlapped[] = true)
            try
                sleep(0.001)
                put!(updates, (total, written))
            finally
                Threads.atomic_sub!(active, 1)
            end
        end
        store = UploadStore((part, n) -> yield())
        object = API.putObjectImpl(store, "progress", data;
            multipartThreshold=1, partSize=4, batchSize=4, progress)
        values = [takeevent(updates) for _ in 1:10]
        @test !overlapped[]
        @test all(value -> first(value) == length(data), values)
        @test all(>(0), diff([0; last.(values)]))
        @test last(values) == (length(data), length(data))
        @test object.size == length(data)
        @test uploaded(store) == data
    end

    @testset "Part boundaries and borrowed vector storage" begin
        for n in (0, 1, 3, 4, 5, 9, 17), batchSize in (1, 2, 8), kind in (:vector, :view, :codeunits)
            data = fill(0x61, n)
            input = kind === :view ? view([0xff; data; 0xff], 2:n+1) :
                kind === :codeunits ? codeunits(repeat("a", n)) : data
            store = UploadStore()
            object = API.putObjectImpl(store, "bounds", input;
                multipartThreshold=-1, partSize=4, batchSize)
            @test uploaded(store) == data
            @test object.size == n
            @test length(store.parts) == cld(n, 4)
            @test store.completed && !store.aborted
            if n > 0 && isdefined(HTTP, :BytesBody)
                @test pointer(store.buffers[1]) == pointer(input)
            end
        end
        store = UploadStore()
        data = UInt8[1, 2, 3]
        @test API.putObjectImpl(store, "read-to-end", data;
            multipartThreshold=-1, partSize=typemax(Int), batchSize=2).size == length(data)
        @test uploaded(store) == data
    end

    @testset "IOBuffer cursors and compressed vectors keep their upload path" begin
        data = UInt8.(0:255)
        input = IOBuffer(data)
        seek(input, 3)
        store = UploadStore()
        marker = ErrorException("buffer progress failed")
        err = try
            API.putObjectImpl(store, "buffer", input;
                multipartThreshold=1, partSize=4, batchSize=2,
                progress=(_, _) -> throw(marker))
        catch err
            err
        end
        @test err === marker
        @test position(input) == 11
        @test isopen(input)
        @test store.aborted && store.active_at_abort == 0 && !store.completed
        @test sort!(collect(keys(store.parts))) == [1, 2]
        for zlibng in (false, true)
            store = UploadStore()
            object = API.putObjectImpl(store, "compressed", data;
                multipartThreshold=1, partSize=64, batchSize=2, compress=true, zlibng)
            wire = uploaded(store)
            @test transcode(GzipDecompressor, wire) == data
            @test object.size == length(wire) > length(data)
            @test length(store.parts) > cld(length(data), 64)
            @test store.completed && !store.aborted
        end
    end
end

end # module
