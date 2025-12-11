module TestCompareDbs

using PSRDatabase
using DataFrames
using Dates
using Test

const PATH_SCHEMA = joinpath(@__DIR__, "test_compare_dbs_schema.sql")

function test_compare_identical_databases()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create identical data in both databases
    PSRDatabase.create_element!(db1, "Configuration"; label = "Config1", value1 = 100.0)
    PSRDatabase.create_element!(db2, "Configuration"; label = "Config1", value1 = 100.0)

    PSRDatabase.create_element!(
        db1,
        "Resource";
        label = "Resource1",
        type = "D",
        some_value1 = [1.0, 2.0, 3.0],
    )
    PSRDatabase.create_element!(
        db2,
        "Resource";
        label = "Resource1",
        type = "D",
        some_value1 = [1.0, 2.0, 3.0],
    )

    differences = PSRDatabase.compare_databases(db1, db2)
    @test isempty(differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_scalar_parameters_different_values()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create elements with different scalar values
    PSRDatabase.create_element!(db1, "Configuration"; label = "Config1", value1 = 100.0)
    PSRDatabase.create_element!(db2, "Configuration"; label = "Config1", value1 = 200.0)

    differences = PSRDatabase.compare_scalar_parameters(db1, db2, "Configuration")
    @test !isempty(differences)
    @test any(occursin("value1", diff) for diff in differences)
    @test any(occursin("100.0", diff) && occursin("200.0", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_scalar_parameters_different_count()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create different number of elements
    PSRDatabase.create_element!(db1, "Configuration"; label = "Config1", value1 = 100.0)
    PSRDatabase.create_element!(db1, "Configuration"; label = "Config2", value1 = 200.0)
    PSRDatabase.create_element!(db2, "Configuration"; label = "Config1", value1 = 100.0)

    differences = PSRDatabase.compare_scalar_parameters(db1, db2, "Configuration")
    @test !isempty(differences)
    @test any(occursin("different number of values", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_scalar_parameters_null_mismatch()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create elements with non-null vs null values
    PSRDatabase.create_element!(
        db1,
        "Product";
        label = "Product1",
        unit = "kg",
        initial_availability = 10.0,
    )
    PSRDatabase.create_element!(
        db2,
        "Product";
        label = "Product1",
        unit = "kg",
    )

    differences = PSRDatabase.compare_scalar_parameters(db1, db2, "Product")
    @test !isempty(differences)
    @test any(occursin("initial_availability", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_vector_parameters_different_values()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create elements with different vector values
    PSRDatabase.create_element!(
        db1,
        "Resource";
        label = "Resource1",
        type = "D",
        some_value1 = [1.0, 2.0, 3.0],
    )
    PSRDatabase.create_element!(
        db2,
        "Resource";
        label = "Resource1",
        type = "D",
        some_value1 = [1.0, 5.0, 3.0],
    )

    differences = PSRDatabase.compare_vector_parameters(db1, db2, "Resource")
    @test !isempty(differences)
    @test any(occursin("some_value1", diff) for diff in differences)
    @test any(occursin("index 2", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_vector_parameters_different_lengths()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create elements with different vector lengths
    PSRDatabase.create_element!(
        db1,
        "Resource";
        label = "Resource1",
        type = "D",
        some_value1 = [1.0, 2.0, 3.0],
    )
    PSRDatabase.create_element!(
        db2,
        "Resource";
        label = "Resource1",
        type = "D",
        some_value1 = [1.0, 2.0],
    )

    differences = PSRDatabase.compare_vector_parameters(db1, db2, "Resource")
    @test !isempty(differences)
    @test any(occursin("different vector lengths", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_vector_parameters_different_element_count()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create different number of elements
    PSRDatabase.create_element!(
        db1,
        "Resource";
        label = "Resource1",
        type = "D",
        some_value1 = [1.0, 2.0],
    )
    PSRDatabase.create_element!(
        db1,
        "Resource";
        label = "Resource2",
        type = "E",
        some_value1 = [3.0, 4.0],
    )
    PSRDatabase.create_element!(
        db2,
        "Resource";
        label = "Resource1",
        type = "D",
        some_value1 = [1.0, 2.0],
    )

    differences = PSRDatabase.compare_vector_parameters(db1, db2, "Resource")
    @test !isempty(differences)
    @test any(occursin("different number of elements", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_vector_parameters_null_mismatch()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create different number of elements with null vs non-null vector values
    PSRDatabase.create_element!(
        db1,
        "Resource";
        label = "Resource1",
        type = "D",
        some_value1 = [1.0, 2.0],
        some_value2 = [1.0, 2.0],
    )
    PSRDatabase.create_element!(
        db2,
        "Resource";
        label = "Resource1",
        type = "D",
        some_value1 = [1.0, 2.0],
    )

    differences = PSRDatabase.compare_vector_parameters(db1, db2, "Resource")
    @test !isempty(differences)
    @test any(occursin("null mismatch", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_scalar_relations_different_relations()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create resources and plants with different relations
    PSRDatabase.create_element!(db1, "Resource"; label = "Resource1", type = "D")
    PSRDatabase.create_element!(db1, "Resource"; label = "Resource2", type = "E")
    PSRDatabase.create_element!(db2, "Resource"; label = "Resource1", type = "D")
    PSRDatabase.create_element!(db2, "Resource"; label = "Resource2", type = "E")

    PSRDatabase.create_element!(db1, "Plant"; label = "Plant1", capacity = 100.0)
    PSRDatabase.create_element!(db2, "Plant"; label = "Plant1", capacity = 100.0)

    # Set different scalar relations
    PSRDatabase.set_scalar_relation!(
        db1,
        "Plant",
        "Resource",
        "Plant1",
        "Resource1",
        "id",
    )
    PSRDatabase.set_scalar_relation!(
        db2,
        "Plant",
        "Resource",
        "Plant1",
        "Resource2",
        "id",
    )

    differences = PSRDatabase.compare_scalar_relations(db1, db2, "Plant")
    @test !isempty(differences)
    @test any(occursin("resource_id", diff) for diff in differences)
    @test any(occursin("Resource1", diff) && occursin("Resource2", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_scalar_relations_identical()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create identical relations
    PSRDatabase.create_element!(db1, "Resource"; label = "Resource1", type = "D")
    PSRDatabase.create_element!(db2, "Resource"; label = "Resource1", type = "D")

    PSRDatabase.create_element!(db1, "Plant"; label = "Plant1", capacity = 100.0)
    PSRDatabase.create_element!(db2, "Plant"; label = "Plant1", capacity = 100.0)

    PSRDatabase.set_scalar_relation!(
        db1,
        "Plant",
        "Resource",
        "Plant1",
        "Resource1",
        "id",
    )
    PSRDatabase.set_scalar_relation!(
        db2,
        "Plant",
        "Resource",
        "Plant1",
        "Resource1",
        "id",
    )

    differences = PSRDatabase.compare_scalar_relations(db1, db2, "Plant")
    @test isempty(differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_vector_relations_different_relations()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create costs and plants with different vector relations
    PSRDatabase.create_element!(db1, "Cost"; label = "Cost1", value = 10.0)
    PSRDatabase.create_element!(db1, "Cost"; label = "Cost2", value = 20.0)
    PSRDatabase.create_element!(db1, "Cost"; label = "Cost3", value = 30.0)
    PSRDatabase.create_element!(db2, "Cost"; label = "Cost1", value = 10.0)
    PSRDatabase.create_element!(db2, "Cost"; label = "Cost2", value = 20.0)
    PSRDatabase.create_element!(db2, "Cost"; label = "Cost3", value = 30.0)

    PSRDatabase.create_element!(
        db1,
        "Plant";
        label = "Plant1",
        capacity = 100.0,
        some_factor = [1.0, 2.0],
    )
    PSRDatabase.create_element!(
        db2,
        "Plant";
        label = "Plant1",
        capacity = 100.0,
        some_factor = [1.0, 2.0],
    )

    # Set different vector relations
    PSRDatabase.set_vector_relation!(
        db1,
        "Plant",
        "Cost",
        "Plant1",
        ["Cost1", "Cost2"],
        "id",
    )
    PSRDatabase.set_vector_relation!(
        db2,
        "Plant",
        "Cost",
        "Plant1",
        ["Cost1", "Cost3"],
        "id",
    )

    differences = PSRDatabase.compare_vector_relations(db1, db2, "Plant")
    @test !isempty(differences)
    @test any(occursin("cost_id", diff) for diff in differences)
    @test any(occursin("Cost2", diff) && occursin("Cost3", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_vector_relations_different_lengths()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create costs and plants with different vector relation lengths
    PSRDatabase.create_element!(db1, "Cost"; label = "Cost1", value = 10.0)
    PSRDatabase.create_element!(db1, "Cost"; label = "Cost2", value = 20.0)
    PSRDatabase.create_element!(db2, "Cost"; label = "Cost1", value = 10.0)
    PSRDatabase.create_element!(db2, "Cost"; label = "Cost2", value = 20.0)

    PSRDatabase.create_element!(
        db1,
        "Plant";
        label = "Plant1",
        capacity = 100.0,
        some_factor = [1.0, 2.0, 3.0],
    )
    PSRDatabase.create_element!(
        db2,
        "Plant";
        label = "Plant1",
        capacity = 100.0,
        some_factor = [1.0, 2.0],
    )

    PSRDatabase.set_vector_relation!(
        db1,
        "Plant",
        "Cost",
        "Plant1",
        ["Cost1", "Cost2", "Cost1"],
        "id",
    )
    PSRDatabase.set_vector_relation!(
        db2,
        "Plant",
        "Cost",
        "Plant1",
        ["Cost1", "Cost2"],
        "id",
    )

    differences = PSRDatabase.compare_vector_relations(db1, db2, "Plant")
    @test !isempty(differences)
    @test any(occursin("different vector lengths", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_time_series_different_values()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create elements with different time series values
    df1 = DataFrame(;
        date_time = [DateTime(2020), DateTime(2021), DateTime(2022)],
        some_vector1 = [1.0, 2.0, 3.0],
        some_vector2 = [10.0, 20.0, 30.0],
    )
    df2 = DataFrame(;
        date_time = [DateTime(2020), DateTime(2021), DateTime(2022)],
        some_vector1 = [1.0, 5.0, 3.0],
        some_vector2 = [10.0, 20.0, 30.0],
    )

    PSRDatabase.create_element!(db1, "Resource"; label = "Resource1", type = "D", group1 = df1)
    PSRDatabase.create_element!(db2, "Resource"; label = "Resource1", type = "D", group1 = df2)

    differences = PSRDatabase.compare_time_series(db1, db2, "Resource")
    @test !isempty(differences)
    @test any(occursin("some_vector1", diff) for diff in differences)
    @test any(occursin("row 2", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_time_series_different_sizes()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create elements with different time series sizes
    df1 = DataFrame(;
        date_time = [DateTime(2020), DateTime(2021), DateTime(2022)],
        some_vector1 = [1.0, 2.0, 3.0],
    )
    df2 = DataFrame(; date_time = [DateTime(2020), DateTime(2021)], some_vector1 = [1.0, 2.0])

    PSRDatabase.create_element!(db1, "Resource"; label = "Resource1", type = "D", group1 = df1)
    PSRDatabase.create_element!(db2, "Resource"; label = "Resource1", type = "D", group1 = df2)

    differences = PSRDatabase.compare_time_series(db1, db2, "Resource")
    @test !isempty(differences)
    @test any(occursin("different table sizes", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_time_series_different_columns()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create elements with time series having different columns
    df1 = DataFrame(;
        date_time = [DateTime(2020), DateTime(2021)],
        some_vector1 = [1.0, 2.0],
        some_vector2 = [10.0, 20.0],
    )
    df2 = DataFrame(; date_time = [DateTime(2020), DateTime(2021)], some_vector1 = [1.0, 2.0])

    PSRDatabase.create_element!(db1, "Resource"; label = "Resource1", type = "D", group1 = df1)
    PSRDatabase.create_element!(db2, "Resource"; label = "Resource1", type = "D", group1 = df2)

    differences = PSRDatabase.compare_time_series(db1, db2, "Resource")
    @test !isempty(differences)
    @test any(occursin("missing mismatch", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_time_series_identical()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create identical time series
    df1 = DataFrame(;
        date_time = [DateTime(2020), DateTime(2021)],
        some_vector1 = [1.0, 2.0],
        some_vector2 = [10.0, 20.0],
    )
    df2 = DataFrame(;
        date_time = [DateTime(2020), DateTime(2021)],
        some_vector1 = [1.0, 2.0],
        some_vector2 = [10.0, 20.0],
    )

    PSRDatabase.create_element!(db1, "Resource"; label = "Resource1", type = "D", group1 = df1)
    PSRDatabase.create_element!(db2, "Resource"; label = "Resource1", type = "D", group1 = df2)

    differences = PSRDatabase.compare_time_series(db1, db2, "Resource")
    @test isempty(differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_time_series_missing_values()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create elements with time series having missing values vs non-missing values
    df1 = DataFrame(;
        date_time = [DateTime(2020), DateTime(2021)],
        some_vector1 = [1.0, NaN],
        some_vector2 = [missing, 20.0],
    )
    df2 = DataFrame(;
        date_time = [DateTime(2020), DateTime(2021)],
        some_vector1 = [1.0, 2.0],
        some_vector2 = [10.0, 20.0],
    )

    PSRDatabase.create_element!(db1, "Resource"; label = "Resource1", type = "D", group1 = df1)
    PSRDatabase.create_element!(db2, "Resource"; label = "Resource1", type = "D", group1 = df2)

    differences = PSRDatabase.compare_time_series(db1, db2, "Resource")
    @test !isempty(differences)
    @test all(occursin("missing mismatch", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_time_series_files_different_paths()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create plants and set different time series file paths
    PSRDatabase.create_element!(db1, "Plant"; label = "Plant1", capacity = 100.0)
    PSRDatabase.create_element!(db2, "Plant"; label = "Plant1", capacity = 100.0)

    PSRDatabase.set_time_series_file!(db1, "Plant"; generation = "generation1.csv")
    PSRDatabase.set_time_series_file!(db2, "Plant"; generation = "generation2.csv")

    differences = PSRDatabase.compare_time_series_files(db1, db2, "Plant")
    @test !isempty(differences)
    @test any(occursin("generation", diff) for diff in differences)
    @test any(
        occursin("generation1.csv", diff) && occursin("generation2.csv", diff) for
        diff in differences
    )

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_time_series_files_missing_in_one()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create plants with file path in one database only
    PSRDatabase.create_element!(db1, "Plant"; label = "Plant1", capacity = 100.0)
    PSRDatabase.create_element!(db2, "Plant"; label = "Plant1", capacity = 100.0)

    PSRDatabase.set_time_series_file!(db1, "Plant"; generation = "generation.csv")

    differences = PSRDatabase.compare_time_series_files(db1, db2, "Plant")
    @test !isempty(differences)
    @test any(occursin("file path present in db1 but missing in db2", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_databases_comprehensive()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create comprehensive test data with multiple differences
    PSRDatabase.create_element!(db1, "Configuration"; label = "Config1", value1 = 100.0)
    PSRDatabase.create_element!(db2, "Configuration"; label = "Config1", value1 = 200.0)

    PSRDatabase.create_element!(
        db1,
        "Resource";
        label = "Resource1",
        type = "D",
        some_value1 = [1.0, 2.0],
    )
    PSRDatabase.create_element!(
        db2,
        "Resource";
        label = "Resource1",
        type = "D",
        some_value1 = [1.0, 3.0],
    )

    PSRDatabase.create_element!(db1, "Plant"; label = "Plant1", capacity = 100.0)
    PSRDatabase.create_element!(db2, "Plant"; label = "Plant1", capacity = 100.0)

    PSRDatabase.set_scalar_relation!(
        db1,
        "Plant",
        "Resource",
        "Plant1",
        "Resource1",
        "id",
    )
    # No relation set in db2 - this will show as different

    differences = PSRDatabase.compare_databases(db1, db2)
    @test !isempty(differences)
    @test any(occursin("Configuration", diff) && occursin("value1", diff) for diff in differences)
    @test any(occursin("Resource", diff) && occursin("some_value1", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_databases_empty()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Compare empty databases
    differences = PSRDatabase.compare_databases(db1, db2)
    @test isempty(differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_databases_different_element_counts()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create different number of elements in collections
    PSRDatabase.create_element!(db1, "Configuration"; label = "Config1", value1 = 100.0)
    PSRDatabase.create_element!(db1, "Configuration"; label = "Config2", value1 = 200.0)
    PSRDatabase.create_element!(db2, "Configuration"; label = "Config1", value1 = 100.0)

    differences = PSRDatabase.compare_databases(db1, db2)
    @test !isempty(differences)
    @test any(occursin("different number of elements", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_set_parameters_different_sets()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create elements with different set parameter values
    PSRDatabase.create_element!(
        db1,
        "Resource";
        label = "Resource1",
        type = "D",
        some_set_value1 = [1.0, 2.0],
        some_set_value2 = [3.0, 4.0],
    )
    PSRDatabase.create_element!(
        db2,
        "Resource";
        label = "Resource1",
        type = "D",
        some_set_value1 = [1.0, 2.0],
        some_set_value2 = [5.0, 6.0],
    )

    differences = PSRDatabase.compare_set_parameters(db1, db2, "Resource")
    @test !isempty(differences)
    @test any(occursin("some_set_value2", diff) for diff in differences)
    @test any(occursin("sets differ", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_set_parameters_different_lengths()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create elements with different set parameter lengths
    PSRDatabase.create_element!(
        db1,
        "Resource";
        label = "Resource1",
        type = "D",
        some_set_value1 = [1.0, 2.0, 3.0],
    )
    PSRDatabase.create_element!(
        db2,
        "Resource";
        label = "Resource1",
        type = "D",
        some_set_value1 = [1.0, 2.0],
    )

    differences = PSRDatabase.compare_set_parameters(db1, db2, "Resource")
    @test !isempty(differences)
    @test any(occursin("different set lengths", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_set_parameters_identical()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create elements with identical set parameter values
    PSRDatabase.create_element!(
        db1,
        "Resource";
        label = "Resource1",
        type = "D",
        some_set_value1 = [1.0, 2.0],
        some_set_value2 = [3.0, 4.0],
    )
    PSRDatabase.create_element!(
        db2,
        "Resource";
        label = "Resource1",
        type = "D",
        some_set_value1 = [1.0, 2.0],
        some_set_value2 = [3.0, 4.0],
    )

    differences = PSRDatabase.compare_set_parameters(db1, db2, "Resource")
    @test isempty(differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_set_parameters_null_mismatch()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create elements with null vs non-null set parameter values
    PSRDatabase.create_element!(
        db1,
        "Resource";
        label = "Resource1",
        type = "D",
        some_set_value1 = [1.0, 2.0],
        some_set_value2 = [3.0, 4.0],
    )
    PSRDatabase.create_element!(
        db2,
        "Resource";
        label = "Resource1",
        type = "D",
        some_set_value1 = [1.0, 2.0],
    )

    differences = PSRDatabase.compare_set_parameters(db1, db2, "Resource")
    @test !isempty(differences)
    @test any(occursin("null mismatch", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_set_relations_different_relations()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create costs
    PSRDatabase.create_element!(db1, "Cost"; label = "Cost1", value = 10.0)
    PSRDatabase.create_element!(db1, "Cost"; label = "Cost2", value = 20.0)
    PSRDatabase.create_element!(db1, "Cost"; label = "Cost3", value = 30.0)
    PSRDatabase.create_element!(db2, "Cost"; label = "Cost1", value = 10.0)
    PSRDatabase.create_element!(db2, "Cost"; label = "Cost2", value = 20.0)
    PSRDatabase.create_element!(db2, "Cost"; label = "Cost3", value = 30.0)

    # Create resources with different set relations
    PSRDatabase.create_element!(
        db1,
        "Resource";
        label = "Resource1",
        some_set_factor = [1.0, 2.0],
        cost_id = ["Cost1", "Cost2"],
    )
    PSRDatabase.create_element!(
        db2,
        "Resource";
        label = "Resource1",
        some_set_factor = [1.0, 3.0],
        cost_id = ["Cost1", "Cost3"],
    )

    differences = PSRDatabase.compare_set_relations(db1, db2, "Resource")
    @test !isempty(differences)
    @test any(occursin("relation", diff) for diff in differences)
    @test any(occursin("relation sets differ", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_set_relations_different_lengths()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create costs
    PSRDatabase.create_element!(db1, "Cost"; label = "Cost1", value = 10.0)
    PSRDatabase.create_element!(db1, "Cost"; label = "Cost2", value = 20.0)
    PSRDatabase.create_element!(db2, "Cost"; label = "Cost1", value = 10.0)
    PSRDatabase.create_element!(db2, "Cost"; label = "Cost2", value = 20.0)

    # Create resources with different set relation lengths
    PSRDatabase.create_element!(
        db1,
        "Resource";
        label = "Resource1",
        some_set_factor = [1.0, 2.0, 3.0],
        cost_id = ["Cost1", "Cost2", "Cost1"],
    )
    PSRDatabase.create_element!(
        db2,
        "Resource";
        label = "Resource1",
        some_set_factor = [1.0, 2.0],
        cost_id = ["Cost1", "Cost2"],
    )

    differences = PSRDatabase.compare_set_relations(db1, db2, "Resource")
    @test !isempty(differences)
    @test any(occursin("different set lengths", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_set_relations_identical()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create costs
    PSRDatabase.create_element!(db1, "Cost"; label = "Cost1", value = 10.0)
    PSRDatabase.create_element!(db1, "Cost"; label = "Cost2", value = 20.0)
    PSRDatabase.create_element!(db2, "Cost"; label = "Cost1", value = 10.0)
    PSRDatabase.create_element!(db2, "Cost"; label = "Cost2", value = 20.0)

    # Create resources with identical set relations
    PSRDatabase.create_element!(
        db1,
        "Resource";
        label = "Resource1",
        some_set_factor = [1.0, 2.0],
        cost_id = ["Cost1", "Cost2"],
    )
    PSRDatabase.create_element!(
        db2,
        "Resource";
        label = "Resource1",
        some_set_factor = [1.0, 2.0],
        cost_id = ["Cost1", "Cost2"],
    )

    differences = PSRDatabase.compare_set_relations(db1, db2, "Resource")
    @test isempty(differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function test_compare_set_relations_null_mismatch()
    db1 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db1.sqlite"),
        PATH_SCHEMA;
        force = true,
    )
    db2 = PSRDatabase.create_empty_db_from_schema(
        joinpath(@__DIR__, "test_db2.sqlite"),
        PATH_SCHEMA;
        force = true,
    )

    # Create costs
    PSRDatabase.create_element!(db1, "Cost"; label = "Cost1", value = 10.0)
    PSRDatabase.create_element!(db1, "Cost"; label = "Cost2", value = 20.0)
    PSRDatabase.create_element!(db2, "Cost"; label = "Cost1", value = 10.0)
    PSRDatabase.create_element!(db2, "Cost"; label = "Cost2", value = 20.0)

    # Create resources with null vs non-null set relations
    PSRDatabase.create_element!(
        db1,
        "Resource";
        label = "Resource1",
        some_set_factor = [1.0, 2.0],
        cost_id = ["Cost1", "Cost2"],
    )
    PSRDatabase.create_element!(
        db2,
        "Resource";
        label = "Resource1",
        some_set_factor = [1.0, 2.0],
    )

    differences = PSRDatabase.compare_set_relations(db1, db2, "Resource")
    @test !isempty(differences)
    @test any(occursin("null mismatch", diff) for diff in differences)

    PSRDatabase.close!(db1)
    PSRDatabase.close!(db2)
    rm(joinpath(@__DIR__, "test_db1.sqlite"); force = true)
    rm(joinpath(@__DIR__, "test_db2.sqlite"); force = true)
    return nothing
end

function runtests()
    for name in names(@__MODULE__; all = true)
        if startswith("$(name)", "test_")
            @testset "$(name)" begin
                getfield(@__MODULE__, name)()
            end
        end
    end
    return nothing
end

end # module TestCompareDbs

TestCompareDbs.runtests()
