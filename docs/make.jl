using Documenter, CloudStore

makedocs(;
    modules=[CloudStore],
    authors="CloudStore.jl contributors",
    pages=[
        "Home" => "index.md",
        "Object operations" => "objects.md",
        "Streaming transfers" => "streaming.md",
        "API Reference" => "reference.md",
    ],
    sitename="CloudStore.jl",
    format=Documenter.HTML(;
        canonical="https://juliaservices.github.io/CloudStore.jl/stable",
        prettyurls=get(ENV, "CI", "false") == "true",
        repolink="https://github.com/JuliaServices/CloudStore.jl",
    ),
    checkdocs=:exports,
    warnonly=false,
)

if get(ENV, "DOCUMENTER_BUILD_ONLY", "false") != "true"
    deploydocs(;
        repo="github.com/JuliaServices/CloudStore.jl",
        devbranch="main",
        push_preview=true,
    )
end
