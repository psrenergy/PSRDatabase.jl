module TestCreate

using PSRDatabase
using SQLite
using Dates
using DataFrames
using Test

function test_create_parameters()
    path_schema = joinpath(@__DIR__, "test_create_parameters.sql")
    db_path = joinpath(@__DIR__, "test_create_parameters.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    @test_throws PSRDatabase.DatabaseException PSRDatabase.create_element!(
        db,
        "Configuration";
        label = "Toy Case",
        value1 = "wrong",
    )
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)
    PSRDatabase.create_element!(db, "Resource"; label = "Resource 2")
    PSRDatabase.create_element!(db, "Resource"; label = "Resource 1", type = "E")
    @test_throws PSRDatabase.DatabaseException PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 4",
        type3 = "E",
    )
    PSRDatabase.close!(db)
    rm(db_path)
    @test true
    return nothing
end

function test_create_empty_parameters()
    path_schema = joinpath(@__DIR__, "test_create_parameters.sql")
    db_path = joinpath(@__DIR__, "test_create_empty_parameters.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case")
    @test_throws PSRDatabase.DatabaseException PSRDatabase.create_element!(
        db,
        "Resource",
    )
    PSRDatabase.close!(db)
    rm(db_path)
    @test true
    return nothing
end

function test_create_parameters_and_vectors()
    path_schema = joinpath(@__DIR__, "test_create_parameters_and_vectors.sql")
    db_path = joinpath(@__DIR__, "test_create_parameters_and_vectors.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)
    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        type = "E",
        some_value = [1.0, 2.0, 3.0],
    )
    PSRDatabase.create_element!(db, "Cost"; label = "Cost 1", value = 30.0)
    PSRDatabase.create_element!(db, "Cost"; label = "Cost 2", value = 20.0)
    PSRDatabase.create_element!(
        db,
        "Plant";
        label = "Plant 1",
        capacity = 50.0,
        some_factor = [0.1, 0.3],
    )
    PSRDatabase.create_element!(
        db,
        "Plant";
        label = "Plant 2",
        capacity = 50.0,
        some_factor = [0.1, 0.3, 0.5],
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.create_element!(
        db,
        "Plant";
        label = "Plant 3",
        generation = "some_file.txt",
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.create_element!(
        db,
        "Plant";
        label = "Plant 2",
        capacity = 50.0,
        some_factor = [],
    )
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 3", resource_id = 1)
    @test_throws PSRDatabase.DatabaseException PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        type = "E",
        some_value = 1.0,
    )
    PSRDatabase.close!(db)
    rm(db_path)
    @test true
    return nothing
end

function test_create_with_transaction()
    path_schema = joinpath(@__DIR__, "test_create_parameters_and_vectors.sql")
    db_path = joinpath(@__DIR__, "test_create_parameters_and_vectors.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)
    PSRDatabase.SQLite.transaction(db.sqlite_db) do
        for i in 1:10
            PSRDatabase.create_element!(
                db,
                "Plant";
                label = "Plant $i",
                capacity = 5.0 * i,
            )
        end
    end
    PSRDatabase.close!(db)
    rm(db_path)
    @test true
end

function test_create_vectors_with_different_sizes_in_same_group()
    path_schema =
        joinpath(@__DIR__, "test_create_vectors_with_different_sizes_in_same_group.sql")
    db_path =
        joinpath(@__DIR__, "test_create_vectors_with_different_sizes_in_same_group.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)
    @test_throws PSRDatabase.DatabaseException PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        type = "E",
        some_vector1 = [1.0],
        some_vector2 = [1.0, 2.0],
    )
    PSRDatabase.close!(db)
    rm(db_path)
    @test true
    return nothing
end

function test_create_scalar_parameter_date()
    path_schema = joinpath(@__DIR__, "test_create_scalar_parameter_date.sql")
    db_path = joinpath(@__DIR__, "test_create_scalar_parameter_date.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(
        db,
        "Configuration";
        label = "Toy Case",
        date_initial = DateTime(2000),
        date_final = DateTime(2001, 10, 12, 23, 45, 12),
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.create_element!(
        db,
        "Configuration";
        label = "Toy Case",
        date_initial = Date(2000),
        date_final = DateTime(2001, 10, 12, 23, 45, 12),
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        date_initial_1 = "2000-01",
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 2",
        date_initial_1 = "20001334",
    )
    PSRDatabase.close!(db)
    rm(db_path)
    @test true
    return nothing
end

function test_create_small_time_series_as_vectors()
    path_schema = joinpath(@__DIR__, "test_create_small_time_series_as_vectors.sql")
    db_path = joinpath(@__DIR__, "test_create_small_time_series_as_vectors.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)
    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        type = "E",
        date_of_modification = [DateTime(2000), DateTime(2001)],
        some_value = [1.0, 2.0],
    )
    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 2",
        type = "E",
        date_of_modification = [DateTime(2002), DateTime(2001)],
        some_value = [1.0, 2.0],
    )
    PSRDatabase.close!(db)
    return rm(db_path)
end

function test_create_vectors_with_relations()
    path_schema = joinpath(@__DIR__, "test_create_vectors_with_relations.sql")
    db_path = joinpath(@__DIR__, "test_create_vectors_with_relations.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(
        db,
        "Configuration";
        label = "Toy Case",
        some_value = 1.0,
    )
    PSRDatabase.create_element!(db, "Product"; label = "Sugar", unit = "Kg")
    PSRDatabase.create_element!(db, "Product"; label = "Sugarcane", unit = "ton")
    PSRDatabase.create_element!(db, "Product"; label = "Molasse", unit = "ton")
    PSRDatabase.create_element!(db, "Product"; label = "Bagasse", unit = "ton")
    @test_throws PSRDatabase.DatabaseException PSRDatabase.create_element!(
        db,
        "Product",
        label = "Bagasse 2",
        unit = 30,
    )
    PSRDatabase.create_element!(db, "Process";
        label = "Sugar Mill",
        product_input = ["Sugarcane"],
        factor_input = [1.0],
        product_output = ["Sugar", "Molasse", "Bagasse"],
        factor_output = [0.3, 0.3, 0.4],
    )

    @test_throws PSRDatabase.DatabaseException PSRDatabase.create_element!(db,
        "Process";
        label = "Sugar Mill 2",
        product_input = ["Sugar"],
        factor_input = ["wrong"],
        product_output = ["Sugarcane"],
        factor_output = [1.0],
    )

    @test_throws PSRDatabase.DatabaseException PSRDatabase.create_element!(db,
        "Process";
        label = "Sugar Mill 3",
        product_input = ["Some Sugar"],
        factor_input = [1.0],
        product_output = ["Sugarcane"],
        factor_output = [1.0],
    )

    @test_throws PSRDatabase.DatabaseException PSRDatabase.create_element!(db,
        "Process";
        label = "Sugar Mill 3",
        product_input = ["Some Sugar"],
        factor_input = [],
        product_output = ["Sugarcane"],
        factor_output = [1.0],
    )

    PSRDatabase.close!(db)
    GC.gc()
    GC.gc()
    rm(db_path)
    @test true
    return nothing
end

function test_create_time_series()
    path_schema = joinpath(@__DIR__, "test_create_time_series.sql")
    db_path = joinpath(@__DIR__, "test_create_time_series.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)

    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)

    for i in 1:3
        df_time_series_group1 = DataFrame(;
            date_time = [DateTime(2000), DateTime(2001)],
            some_vector1 = [1.0, 2.0] .* i,
            some_vector2 = [2.0, 3.0] .* i,
        )
        df_time_series_group2 = DataFrame(;
            date_time = [DateTime(2000), DateTime(2000), DateTime(2001), DateTime(2001)],
            block = [1, 2, 1, 2],
            some_vector3 = [1.0, missing, 3.0, 4.0] .* i,
        )
        df_time_series_group3 = DataFrame(;
            date_time = [
                DateTime(2000),
                DateTime(2000),
                DateTime(2000),
                DateTime(2000),
                DateTime(2001),
                DateTime(2001),
                DateTime(2001),
                DateTime(2009),
            ],
            block = [1, 1, 1, 1, 2, 2, 2, 2],
            segment = [1, 2, 3, 4, 1, 2, 3, 4],
            some_vector5 = [1.0, 2.0, 3.0, 4.0, 1, 2, 3, 4] .* i,
            some_vector6 = [1.0, 2.0, 3.0, 4.0, 1, 2, 3, 4] .* i,
        )
        PSRDatabase.create_element!(
            db,
            "Resource";
            label = "Resource $i",
            group1 = df_time_series_group1,
            group2 = df_time_series_group2,
            group3 = df_time_series_group3,
        )
    end

    df_time_series_group5 = DataFrame(;
        date_time = [DateTime(2000), DateTime(2001)],
        some_vector1 = [1.0, 2.0],
        some_vector2 = [2.0, 3.0],
    )

    @test_throws PSRDatabase.DatabaseException PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 4",
        group5 = df_time_series_group5,
    )

    PSRDatabase.close!(db)
    GC.gc()
    GC.gc()
    rm(db_path)
    @test true
    return nothing
end

function test_create_vector_with_empty_relations_id()
    path_schema = joinpath(@__DIR__, "test_create_vectors_with_empty_relations.sql")
    db_path = joinpath(@__DIR__, "test_create_vectors_with_empty_relations.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)

    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case")

    PSRDatabase.create_element!(db, "Process"; label = "Blast Furnace")
    PSRDatabase.create_element!(db, "Process"; label = "Basic Oxygen Furnace")
    PSRDatabase.create_element!(db, "Process"; label = "Electric Arc Furnace")

    PSRDatabase.create_element!(
        db,
        "Plant";
        label = "Steel Plant",
        process_id = ["Blast Furnace", "Basic Oxygen Furnace", "Electric Arc Furnace"],
        process_capacity = [100.0, 200.0, 300.0],
        process_is_candidate = [0, 0, 1],
        process_substitute = [typemin(Int), typemin(Int), 2],
    )

    PSRDatabase.close!(db)
    GC.gc()
    GC.gc()
    rm(db_path)
    @test true
    return nothing
end

function test_create_vector_with_empty_relations_string()
    path_schema = joinpath(@__DIR__, "test_create_vectors_with_empty_relations.sql")
    db_path = joinpath(@__DIR__, "test_create_vectors_with_empty_relations.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)

    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case")

    PSRDatabase.create_element!(db, "Process"; label = "Blast Furnace")
    PSRDatabase.create_element!(db, "Process"; label = "Basic Oxygen Furnace")
    PSRDatabase.create_element!(db, "Process"; label = "Electric Arc Furnace")

    PSRDatabase.create_element!(
        db,
        "Plant";
        label = "Steel Plant",
        process_id = ["Blast Furnace", "Basic Oxygen Furnace", "Electric Arc Furnace"],
        process_capacity = [100.0, 200.0, 300.0],
        process_is_candidate = [0, 0, 1],
        process_substitute = ["", "", "Basic Oxygen Furnace"],
    )

    PSRDatabase.close!(db)
    GC.gc()
    GC.gc()
    rm(db_path)
    @test true
    return nothing
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

TestCreate.runtests()

end
