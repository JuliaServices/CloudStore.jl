using Documenter, CloudStore

makedocs(;
    pages=[
        "Home" => "index.md",
        "API Reference" => "reference.md",
    ],
    sitename="CloudStore.jl",
)

if get(ENV, "DOCUMENTER_BUILD_ONLY", "false") != "true"
    deploydocs(;
        repo="github.com/JuliaServices/CloudStore.jl",
        devbranch = "main",
        push_preview = true,
    )
end
