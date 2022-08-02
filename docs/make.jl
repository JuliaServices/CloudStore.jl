using Documenter, CloudStore

makedocs(;
    pages=[
        "Home" => "index.md",
        "API Reference" => "reference.md",
    ],
    sitename="CloudStore.jl",
)

deploydocs(;
    repo="github.com/JuliaServices/CloudStore.jl",
    devbranch = "main",
    push_preview = true,
)
