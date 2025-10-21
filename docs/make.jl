using Documenter
<<<<<<< HEAD

using Pkg
Pkg.activate(dirname(@__DIR__))
=======
>>>>>>> 23ff8153db433c0ff2bbd6ebb9747389c0169302
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
    ),
    pages = [
        "Home" => "index.md",
<<<<<<< HEAD
        "PSRDatabase Overview" => String[
            "psrdatabase/introduction.md",
            "psrdatabase/rules.md",
            "psrdatabase/time_series.md",
        ],
        "PSRDatabase Examples" => String[
            "sqlite_examples/migrations.md",
        ],
        "API Reference" => "api_reference.md",
=======
>>>>>>> 23ff8153db433c0ff2bbd6ebb9747389c0169302
    ],
)

deploydocs(;
    repo = "github.com/psrenergy/PSRDatabase.jl.git",
    devbranch = "master",
    push_preview = true,
)
