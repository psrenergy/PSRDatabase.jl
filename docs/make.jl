using Documenter
using PSRDatabase

DocMeta.setdocmeta!(PSRDatabase, :DocTestSetup, :(using PSRDatabase); recursive = true)

Documenter.makedocs(;
    sitename = "PSRDatabase",
    modules = [PSRDatabase],
    repo = "https://github.com/psrenergy/PSRDatabase.jl/blob/{commit}{path}#{line}",
    doctest = true,
    clean = true,
    checkdocs = :none,
    format = Documenter.HTML(;
        prettyurls = get(ENV, "CI", "false") == "true",
        canonical = "https://psrenergy.github.io/PSRDatabase.jl",
        edit_link = "master",
        # assets = [
        #     "assets/favicon.ico",
        # ],
    ),
    pages = [
        "Home" => "index.md",
        "manual.md",
        "PSRDatabase Overview" => [
            "psrdatabase/introduction.md",
            "psrdatabase/rules.md",
            "psrdatabase/time_series.md",
        ],
        "PSRDatabase Examples" => [
            "sqlite_examples/migrations.md",
        ],
    ],
)

deploydocs(;
    repo = "github.com/psrenergy/PSRDatabase.jl.git",
    devbranch = "master",
    push_preview = true,
)
