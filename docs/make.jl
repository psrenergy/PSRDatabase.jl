using Documenter

using PSRDatabase
const PSRI = PSRDatabase

makedocs(;
    modules = [PSRDatabase],
    doctest = true,
    clean = true,
    format = Documenter.HTML(; mathengine = Documenter.MathJax2()),
    sitename = "PSRDatabase.jl",
    authors = "psrenergy",
    pages = [
        "Home" => "index.md",
        "manual.md",
        "PSRDatabase Overview" => String[
            "psrdatabase/introduction.md",
            "psrdatabase/rules.md",
            "psrdatabase/time_series.md",
        ],
        "PSRDatabase Examples" => String[
            "sqlite_examples/migrations.md",
        ],
    ],
)

deploydocs(;
    repo = "github.com/psrenergy/PSRDatabase.jl.git",
    push_preview = true,
)
