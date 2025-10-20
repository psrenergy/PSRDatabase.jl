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
    ),
    pages = [
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo = "github.com/psrenergy/PSRDatabase.jl.git",
    devbranch = "master",
    push_preview = true,
)
