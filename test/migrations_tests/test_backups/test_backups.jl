module TestBackups

using PSRDatabase
using SQLite
using Test

function test_backups()
    path_migrations_directory = joinpath(@__DIR__, "migrations")
    @test PSRDatabase.test_migrations(path_migrations_directory)
    db_path = joinpath(@__DIR__, "test_backups.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(
        db_path,
        joinpath(path_migrations_directory, "1", "up.sql");
        force = true,
    )

    PSRDatabase.create_element!(
        db,
        "Configuration";
        label = "conf",
    )

    PSRDatabase.create_element!(
        db,
        "TestOne";
        id = 1,
        label = "some_label",
    )

    PSRDatabase.create_element!(
        db,
        "TestOne";
        id = 2,
        label = "some_label_2",
    )

    PSRDatabase.create_element!(
        db,
        "TestTwo";
        id = 1,
        label = "some_other_label",
    )

    PSRDatabase.apply_migrations!(
        db.sqlite_db,
        path_migrations_directory,
        1,
        2,
        :up,
    )

    backup_file_path = readdir(joinpath(@__DIR__, "_backups"))[1]

    db_copy = PSRDatabase.load_db(joinpath(@__DIR__, "_backups", backup_file_path))

    @test PSRDatabase.read_scalar_parameters(db_copy, "TestOne", "label") ==
          ["some_label", "some_label_2"]
    @test PSRDatabase.read_scalar_parameters(db_copy, "TestTwo", "label") ==
          ["some_other_label"]

    PSRDatabase.close!(db)
    PSRDatabase.close!(db_copy)

    rm((joinpath(@__DIR__, "_backups")); recursive = true)
    rm(db_path; force = true)

    return nothing
end

function runtests()
    for name in names(@__MODULE__; all = true)
        if startswith("$name", "test_")
            @testset "$(name)" begin
                getfield(@__MODULE__, name)()
            end
        end
    end
end

TestBackups.runtests()

end
