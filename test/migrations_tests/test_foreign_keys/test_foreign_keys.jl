module TestForeignKeys

using PSRDatabase
using SQLite
using Test

function test_foreign_keys()
    migrations_dir = joinpath(@__DIR__, "migrations")
    path_schema = joinpath(migrations_dir, "1", "up.sql")
    db_path = joinpath(@__DIR__, "test.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(
        db_path,
        path_schema;
        force = true,
    )

    PSRDatabase.create_element!(
        db,
        "Process";
        label = "Sugar Mill",
        capex = 52000.0,
        opex = 0.0,
        base_capacity = 100.0,
    )

    PSRDatabase.create_element!(
        db,
        "Product";
        label = "Sugar",
        sell_price = 5.0,
        unit = "kg",
    )

    PSRDatabase.create_element!(
        db,
        "Product";
        label = "Sugarcane",
        unit = "ton",
        initial_availability = 100.0,
    )

    PSRDatabase.create_element!(
        db,
        "Input";
        id = 1,
        process_id = 1,
        product_id = 1,
        factor = 1.0,
    )

    PSRDatabase.create_element!(
        db,
        "Output";
        id = 1,
        process_id = 1,
        product_id = 2,
        factor = 0.75,
    )

    PSRDatabase.apply_migrations!(
        db.sqlite_db,
        migrations_dir,
        1,
        3,
        :up,
    )

    PSRDatabase.close!(db)

    db = PSRDatabase.load_db(db_path)

    process_input = PSRDatabase.read_vector_relation(
        db,
        "Process",
        "Product",
        "Sugar Mill",
        "input",
    )
    process_output = PSRDatabase.read_vector_relation(
        db,
        "Process",
        "Product",
        "Sugar Mill",
        "output",
    )

    PSRDatabase.close!(db)
    rm((joinpath(@__DIR__, "_backups")); recursive = true)
    return rm(db_path; force = true)
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

TestForeignKeys.runtests()

end
