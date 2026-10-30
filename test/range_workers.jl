module RangeWorkerTests

using Test, CloudStore
import CloudBase
const API = CloudStore.API

struct RangeStore{F} <: CloudBase.AbstractStore
    hook::F
end

function API.getRangeOnce!(dest, store::RangeStore, url, headers, rng, total, tag; kw...)
    store.hook(first(rng))
    copyto!(dest, UInt8.(rng))
    return length(rng)
end

function takeevent(channel)
    timedwait(() -> isready(channel), 10) == :ok || error("range worker event timed out")
    return take!(channel)
end

@testset "A completed range starts another without waiting for a slow range" begin
    started = Channel{Int}(16)
    release = Channel{Nothing}(1)
    store = RangeStore() do lo
        put!(started, lo)
        lo == 4 && take!(release)
    end
    result = fill(0xff, 24)
    nbytes = Threads.Atomic{Int}(4)
    task = @async API.getparts!(result, nothing, nothing, store, "", nothing,
        4, 4, 24, "tag", 2, nbytes, nothing)
    try
        # One worker remains blocked at 4 while the other finishes all later parts.
        @test Set(takeevent(started) for _ in 1:5) == Set(4:4:20)
        @test !istaskdone(task)
        put!(release, nothing)
        @test fetch(task) == false
        @test result[5:end] == UInt8.(4:23)
        @test all(==(0xff), result[1:4])
        @test nbytes[] == 24
    finally
        isopen(release) && close(release)
        try
            wait(task)
        catch
        end
    end
end

@testset "Range failure drains in-flight writes before returning" begin
    entered = Channel{Nothing}(1)
    release = Channel{Nothing}(1)
    failed = Channel{Nothing}(1)
    requested = Threads.Atomic{Int}(0)
    store = RangeStore() do lo
        Threads.atomic_add!(requested, 1)
        if lo == 4
            put!(entered, nothing)
            take!(release)
        else
            takeevent(entered)
            put!(failed, nothing)
            error("range failure")
        end
    end
    result = fill(0xff, 12)
    task = @async try
        API.getparts!(result, nothing, nothing, store, "", nothing,
            4, 4, 12, "tag", 2, Threads.Atomic{Int}(4), nothing)
    catch e
        e
    end
    try
        takeevent(failed)
        @test !istaskdone(task)
        put!(release, nothing)
        err = fetch(task)
        @test err isa CompositeException
        @test occursin("range failure", sprint(showerror, err))
        @test requested[] == 2
        @test result[5:8] == UInt8.(4:7)
        @test all(==(0xff), result[9:end])
    finally
        isopen(release) && close(release)
        wait(task)
    end
end

@testset "Progress callbacks remain serial and count validated bytes" begin
    progress_active = Ref(false)
    updates = Int[]
    progress = function(total, received)
        @test !progress_active[]
        progress_active[] = true
        yield()
        @test total == 64
        push!(updates, received)
        progress_active[] = false
    end
    result = fill(0xff, 64)
    @test API.getparts!(result, nothing, nothing, RangeStore(_ -> yield()), "", nothing,
        4, 4, 64, "tag", 4, Threads.Atomic{Int}(4), progress)
    @test issorted(updates) && last(updates) == 64
    @test result[5:end] == UInt8.(4:63)
end

end
