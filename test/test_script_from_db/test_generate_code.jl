module TestGenerateCode

using PSRDatabase
using SQLite
using Dates
using DataFrames
using Test

function test_generate_code_from_simple_parameters()
    path_schema = joinpath(@__DIR__, "..", "test_create", "test_create_parameters.sql")
    db_path = joinpath(@__DIR__, "test_generate_code_simple_parameters.sqlite")
    db_reconstructed_path = joinpath(@__DIR__, "test_generate_code_simple_parameters_reconstructed.sqlite")
    code_path = joinpath(@__DIR__, "test_generate_code_simple_parameters_code.jl")

    # Create and populate original database
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)
    PSRDatabase.create_element!(db, "Resource"; label = "Resource 1", type = "E")
    PSRDatabase.create_element!(db, "Resource"; label = "Resource 2", type = "F")

    # Generate code to file
    PSRDatabase.generate_julia_script_from_database(
        db,
        code_path,
        db_reconstructed_path;
        path_schema = path_schema,
    )

    include(code_path)

    # Reload both databases
    db1 = PSRDatabase.load_db(db_path; read_only = true)
    db2 = PSRDatabase.load_db(db_reconstructed_path; read_only = true)

    # Compare databases
    @test isempty(PSRDatabase.compare_databases(db1, db2))

    # Cleanup
    PSRDatabase.close!(db)
    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(db_path)
    rm(db_reconstructed_path)
    rm(code_path)

    return nothing
end

function test_generate_code_from_parameters_and_vectors()
    path_schema = joinpath(@__DIR__, "..", "test_create", "test_create_parameters_and_vectors.sql")
    db_path = joinpath(@__DIR__, "test_generate_code_parameters_and_vectors.sqlite")
    db_reconstructed_path = joinpath(@__DIR__, "test_generate_code_parameters_and_vectors_reconstructed.sqlite")
    code_path = joinpath(@__DIR__, "test_generate_code_parameters_and_vectors_code.jl")

    # Create and populate original database
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

    # Generate code to file
    PSRDatabase.generate_julia_script_from_database(
        db,
        code_path,
        db_reconstructed_path;
        path_schema = path_schema,
    )

    include(code_path)

    # Reload both databases
    db1 = PSRDatabase.load_db(db_path; read_only = true)
    db2 = PSRDatabase.load_db(db_reconstructed_path; read_only = true)

    # Compare databases
    @test isempty(PSRDatabase.compare_databases(db1, db2))

    # Cleanup
    PSRDatabase.close!(db)
    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(db_path)
    rm(db_reconstructed_path)
    rm(code_path)

    return nothing
end

function test_generate_code_from_vectors_with_relations()
    path_schema = joinpath(@__DIR__, "..", "test_create", "test_create_vectors_with_relations.sql")
    db_path = joinpath(@__DIR__, "test_generate_code_vectors_with_relations.sqlite")
    db_reconstructed_path = joinpath(@__DIR__, "test_generate_code_vectors_with_relations_reconstructed.sqlite")
    code_path = joinpath(@__DIR__, "test_generate_code_vectors_with_relations_code.jl")

    # Create and populate original database
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
    PSRDatabase.create_element!(
        db,
        "Process";
        label = "Sugar Mill",
        product_input = ["Sugarcane"],
        factor_input = [1.0],
        product_output = ["Sugar", "Molasse", "Bagasse"],
        factor_output = [0.3, 0.3, 0.4],
    )

    # Generate code to file
    PSRDatabase.generate_julia_script_from_database(
        db,
        code_path,
        db_reconstructed_path;
        path_schema = path_schema,
    )

    include(code_path)

    # Reload both databases
    db1 = PSRDatabase.load_db(db_path; read_only = true)
    db2 = PSRDatabase.load_db(db_reconstructed_path; read_only = true)

    # Compare databases
    @test isempty(PSRDatabase.compare_databases(db1, db2))

    # Cleanup
    PSRDatabase.close!(db)
    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(db_path)
    rm(db_reconstructed_path)
    rm(code_path)

    return nothing
end

function test_generate_code_from_time_series()
    path_schema = joinpath(@__DIR__, "..", "test_create", "test_create_time_series.sql")
    db_path = joinpath(@__DIR__, "test_generate_code_time_series.sqlite")
    db_reconstructed_path = joinpath(@__DIR__, "test_generate_code_time_series_reconstructed.sqlite")
    code_path = joinpath(@__DIR__, "test_generate_code_time_series_code.jl")

    # Create and populate original database
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

    # Generate code to file
    PSRDatabase.generate_julia_script_from_database(
        db,
        code_path,
        db_reconstructed_path;
        path_schema = path_schema,
    )

    include(code_path)

    # Reload both databases
    db1 = PSRDatabase.load_db(db_path; read_only = true)
    db2 = PSRDatabase.load_db(db_reconstructed_path; read_only = true)

    # Compare databases
    @test isempty(PSRDatabase.compare_databases(db1, db2))

    # Cleanup
    PSRDatabase.close!(db)
    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(db_path)
    rm(db_reconstructed_path)
    rm(code_path)

    return nothing
end

function test_generate_code_from_date_parameters()
    path_schema = joinpath(@__DIR__, "..", "test_create", "test_create_scalar_parameter_date.sql")
    db_path = joinpath(@__DIR__, "test_generate_code_date_parameters.sqlite")
    db_reconstructed_path = joinpath(@__DIR__, "test_generate_code_date_parameters_reconstructed.sqlite")
    code_path = joinpath(@__DIR__, "test_generate_code_date_parameters_code.jl")

    # Create and populate original database
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(
        db,
        "Configuration";
        label = "Toy Case",
        date_initial = DateTime(2000),
        date_final = DateTime(2001, 10, 12, 23, 45, 12),
    )
    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        date_initial_1 = DateTime(2020, 5, 15),
    )

    # Generate code to file
    PSRDatabase.generate_julia_script_from_database(
        db,
        code_path,
        db_reconstructed_path;
        path_schema = path_schema,
    )

    include(code_path)

    # Reload both databases
    db1 = PSRDatabase.load_db(db_path; read_only = true)
    db2 = PSRDatabase.load_db(db_reconstructed_path; read_only = true)

    # Compare databases
    @test isempty(PSRDatabase.compare_databases(db1, db2))

    # Cleanup
    PSRDatabase.close!(db)
    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(db_path)
    rm(db_reconstructed_path)
    rm(code_path)

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

TestGenerateCode.runtests()

end
