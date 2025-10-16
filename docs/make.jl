using Pkg
Pkg.instantiate()

using Documenter

Pkg.activate(dirname(@__DIR__))
using PSRDatabase

makedocs(;
    modules = [PSRDatabase],
    doctest = true,
    clean = true,
    format = Documenter.HTML(; mathengine = Documenter.MathJax2()),
    sitename = "PSRDatabase.jl",
    authors = "psrenergy",
    pages = [
        "Home" => "index.md",
        "PSRDatabase Overview" => String[
            "psrdatabase/introduction.md",
            "psrdatabase/rules.md",
            "psrdatabase/time_series.md",
        ],
        "PSRDatabase Examples" => String[
            "sqlite_examples/migrations.md",
        ],
        "API Reference" => "api_reference.md",
    ],
)

deploydocs(;
    repo = "github.com/psrenergy/PSRDatabase.jl.git",
    push_preview = true,
)
