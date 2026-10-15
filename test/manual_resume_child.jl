using CloudStore
include(joinpath(@__DIR__, "..", "examples", "manual_resume.jl"))

provider, phase, host, name, source, checkpoint, key = ARGS
if provider == "s3"
    store = CloudStore.S3.Bucket(name; host)
    credentials = CloudStore.S3.Credentials("minioadmin", "minioadmin")
else
    store = CloudStore.Blobs.Container(name, "devstoreaccount1"; host)
    credentials = CloudStore.Blobs.Credentials("devstoreaccount1",
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
end

if phase == "start"
    state = ManualResumeExample.start(store, key, source, checkpoint; credentials, initial=(3, 1))
    # The provider accepted part 2, but the caller exited before saving its receipt.
    ManualResumeExample.stage(store, state, state["parts"][2], credentials)
elseif phase == "resume"
    ManualResumeExample.resume(store, checkpoint; credentials)
else
    error("invalid test phase")
end
println("completed ", phase)
