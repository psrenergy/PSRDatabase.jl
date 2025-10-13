module TestReadOnly

using PSRDatabase
using SQLite
using Dates
using Test

function test_read_only()
    path_schema = joinpath(@__DIR__, "test_read_only.sql")
    db_path = joinpath(@__DIR__, "test_read_only.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(
        db,
        "Configuration";
        label = "Toy Case",
        date_initial = DateTime(2020, 1, 1),
    )
    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
    )
    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 2",
    )

    PSRDatabase.close!(db)

    db = PSRDatabase.load_db(db_path; read_only = true)

    @test PSRDatabase.read_scalar_parameters(db, "Configuration", "label") ==
          ["Toy Case"]
    @test PSRDatabase.read_scalar_parameters(db, "Resource", "label") ==
          ["Resource 1", "Resource 2"]
    @test PSRDatabase.read_scalar_parameter(db, "Resource", "label", "Resource 1") ==
          "Resource 1"

    @test_throws SQLite.SQLiteException PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 3",
    )

    PSRDatabase.close!(db)
    return rm(db_path)
end

function runtests()
    Base.GC.gc()
    Base.GC.gc()
    for name in names(@__MODULE__; all = true)
        if startswith("$name", "test_")
            @testset "$(name)" begin
                getfield(@__MODULE__, name)()
            end
        end
    end
end

TestReadOnly.runtests()

end
