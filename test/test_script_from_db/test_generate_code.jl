module TestGenerateCode

using PSRDatabase
using SQLite
using Dates
using DataFrames
using Test

"""
Helper function to compare scalar parameters between two databases for a collection.
"""
function compare_scalar_parameters(
    db1::PSRDatabase.DatabaseSQLite,
    db2::PSRDatabase.DatabaseSQLite,
    collection_id::String,
)
    collection = PSRDatabase._get_collection(db1, collection_id)

    for (attr_id, attr) in collection.scalar_parameters
        if attr_id == "id"
            continue
        end

        values1 = PSRDatabase.read_scalar_parameters(db1, collection_id, attr_id)
        values2 = PSRDatabase.read_scalar_parameters(db2, collection_id, attr_id)

        @test length(values1) == length(values2)

        for (v1, v2) in zip(values1, values2)
            if ismissing(v1) && ismissing(v2)
                continue
            elseif attr.type <: Float64 && (isnan(v1) || isnan(v2))
                @test isnan(v1) == isnan(v2)
            else
                @test v1 == v2
            end
        end
    end

    return nothing
end

"""
Helper function to compare vector parameters between two databases for a collection.
"""
function compare_vector_parameters(
    db1::PSRDatabase.DatabaseSQLite,
    db2::PSRDatabase.DatabaseSQLite,
    collection_id::String,
)
    collection = PSRDatabase._get_collection(db1, collection_id)

    for (attr_id, attr) in collection.vector_parameters
        vectors1 = PSRDatabase.read_vector_parameters(db1, collection_id, attr_id)
        vectors2 = PSRDatabase.read_vector_parameters(db2, collection_id, attr_id)

        @test length(vectors1) == length(vectors2)

        for (vec1, vec2) in zip(vectors1, vectors2)
            @test length(vec1) == length(vec2)

            for (v1, v2) in zip(vec1, vec2)
                if ismissing(v1) && ismissing(v2)
                    continue
                elseif attr.type <: Float64 && (isnan(v1) || isnan(v2))
                    @test isnan(v1) == isnan(v2)
                else
                    @test v1 == v2
                end
            end
        end
    end

    return nothing
end

"""
Helper function to compare scalar relations between two databases for a collection.
"""
function compare_scalar_relations(
    db1::PSRDatabase.DatabaseSQLite,
    db2::PSRDatabase.DatabaseSQLite,
    collection_id::String,
)
    collection = PSRDatabase._get_collection(db1, collection_id)

    for (attr_id, attr) in collection.scalar_relations
        relations1 = PSRDatabase.read_scalar_relations(
            db1,
            collection_id,
            attr.relation_collection,
            attr.relation_type,
        )
        relations2 = PSRDatabase.read_scalar_relations(
            db2,
            collection_id,
            attr.relation_collection,
            attr.relation_type,
        )

        @test length(relations1) == length(relations2)

        for (r1, r2) in zip(relations1, relations2)
            @test r1 == r2
        end
    end

    return nothing
end

"""
Helper function to compare vector relations between two databases for a collection.
"""
function compare_vector_relations(
    db1::PSRDatabase.DatabaseSQLite,
    db2::PSRDatabase.DatabaseSQLite,
    collection_id::String,
)
    collection = PSRDatabase._get_collection(db1, collection_id)

    for (attr_id, attr) in collection.vector_relations
        relations1 = PSRDatabase.read_vector_relations(
            db1,
            collection_id,
            attr.relation_collection,
            attr.relation_type,
        )
        relations2 = PSRDatabase.read_vector_relations(
            db2,
            collection_id,
            attr.relation_collection,
            attr.relation_type,
        )

        @test length(relations1) == length(relations2)

        for (rel_vec1, rel_vec2) in zip(relations1, relations2)
            @test length(rel_vec1) == length(rel_vec2)

            for (r1, r2) in zip(rel_vec1, rel_vec2)
                @test r1 == r2
            end
        end
    end

    return nothing
end

"""
Helper function to compare time series between two databases for a collection.
"""
function compare_time_series(
    db1::PSRDatabase.DatabaseSQLite,
    db2::PSRDatabase.DatabaseSQLite,
    collection_id::String,
)
    collection = PSRDatabase._get_collection(db1, collection_id)

    # Get all element labels
    num_elements = PSRDatabase.number_of_elements(db1, collection_id)
    labels = PSRDatabase.read_scalar_parameters(db1, collection_id, "label")

    # Group time series by group_id
    time_series_groups = Dict{String, Vector{String}}()
    for (attr_id, attr) in collection.time_series
        group_id = attr.group_id
        if !haskey(time_series_groups, group_id)
            time_series_groups[group_id] = String[]
        end
        push!(time_series_groups[group_id], attr_id)
    end

    for label in labels
        for (group_id, attr_ids) in time_series_groups
            for attr_id in attr_ids
                df1 = PSRDatabase.read_time_series_table(db1, collection_id, attr_id, label)
                df2 = PSRDatabase.read_time_series_table(db2, collection_id, attr_id, label)

                @test size(df1) == size(df2)
                @test names(df1) == names(df2)

                # Compare each column
                for col_name in names(df1)
                    col1 = df1[!, col_name]
                    col2 = df2[!, col_name]

                    for (v1, v2) in zip(col1, col2)
                        if ismissing(v1) && ismissing(v2)
                            continue
                        elseif typeof(v1) <: Float64 && (isnan(v1) || isnan(v2))
                            @test isnan(v1) == isnan(v2)
                        else
                            @test v1 == v2
                        end
                    end
                end
            end
        end
    end

    return nothing
end

"""
Helper function to compare time series files between two databases for a collection.
"""
function compare_time_series_files(
    db1::PSRDatabase.DatabaseSQLite,
    db2::PSRDatabase.DatabaseSQLite,
    collection_id::String,
)
    collection = PSRDatabase._get_collection(db1, collection_id)

    if isempty(collection.time_series_files)
        return nothing
    end

    labels = PSRDatabase.read_scalar_parameters(db1, collection_id, "label")

    for label in labels
        for (attr_id, attr) in collection.time_series_files
            # Read time series file paths
            id1 = PSRDatabase._get_id(db1, collection_id, label)
            id2 = PSRDatabase._get_id(db2, collection_id, label)

            file_path1 = PSRDatabase._read_time_series_file_path(db1, collection_id, attr_id)
            file_path2 = PSRDatabase._read_time_series_file_path(db2, collection_id, attr_id)

            @show file_path1
            @show file_path2
            @test file_path1 == file_path2
        end
    end

    return nothing
end

"""
Compare two databases to ensure they have the same data.
"""
function compare_databases(
    db1::PSRDatabase.DatabaseSQLite,
    db2::PSRDatabase.DatabaseSQLite,
)
    collection_ids = PSRDatabase._get_collection_ids(db1)

    for collection_id in collection_ids
        # Compare number of elements
        num_elements1 = PSRDatabase.number_of_elements(db1, collection_id)
        num_elements2 = PSRDatabase.number_of_elements(db2, collection_id)
        @test num_elements1 == num_elements2

        # Compare scalar parameters
        compare_scalar_parameters(db1, db2, collection_id)

        # Compare vector parameters
        compare_vector_parameters(db1, db2, collection_id)

        # Compare scalar relations
        compare_scalar_relations(db1, db2, collection_id)

        # Compare vector relations
        compare_vector_relations(db1, db2, collection_id)

        # Compare time series
        if !isempty(PSRDatabase._get_collection(db1, collection_id).time_series)
            compare_time_series(db1, db2, collection_id)
        end
    end

    return nothing
end

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
    PSRDatabase.generate_code_from_database(
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
    compare_databases(db1, db2)

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
    PSRDatabase.generate_code_from_database(
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
    compare_databases(db1, db2)

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
    PSRDatabase.generate_code_from_database(
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
    compare_databases(db1, db2)

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
    PSRDatabase.generate_code_from_database(
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
    compare_databases(db1, db2)

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
    PSRDatabase.generate_code_from_database(
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
    compare_databases(db1, db2)

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
