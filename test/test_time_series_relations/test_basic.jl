module TestTimeSeriesRelationsBasic

using PSRDatabase
using SQLite
using Dates
using DataFrames
using Test

function test_create_and_read_time_series_relations()
    path_schema = joinpath(@__DIR__, "test_schema.sql")
    db_path = joinpath(@__DIR__, "test_basic.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)

    # Create configuration
    PSRDatabase.create_element!(db, "Configuration"; value1 = 1.0)

    # Create plants
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 1")
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 2")
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 3")

    # Create resource with time series data including relations
    df_generation = DataFrame(;
        date_time = [DateTime(2000), DateTime(2001), DateTime(2002)],
        power = [100.0, 200.0, 300.0],
        plant_id = ["Plant 1", "Plant 2", "Plant 3"],
    )

    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        generation = df_generation,
    )

    # Test reading time series relation table
    df_read = PSRDatabase.read_time_series_relation_table(
        db,
        "Resource",
        "plant_id",
        "Resource 1",
    )

    @test nrow(df_read) == 3
    @test df_read[1, :plant_id] == "Plant 1"
    @test df_read[2, :plant_id] == "Plant 2"
    @test df_read[3, :plant_id] == "Plant 3"

    PSRDatabase.close!(db)
    return rm(db_path)
end

function test_add_and_update_time_series_relations()
    path_schema = joinpath(@__DIR__, "test_schema.sql")
    db_path = joinpath(@__DIR__, "test_update.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)

    # Create configuration
    PSRDatabase.create_element!(db, "Configuration"; value1 = 1.0)

    # Create plants
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 1")
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 2")
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 3")

    # Create resource without time series data
    PSRDatabase.create_element!(db, "Resource"; label = "Resource 1")

    # Test add_time_series_relation_row!
    PSRDatabase.add_time_series_relation_row!(
        db,
        "Resource",
        "plant_id",
        "Resource 1",
        "Plant 1";
        date_time = DateTime(2000),
    )

    PSRDatabase.add_time_series_relation_row!(
        db,
        "Resource",
        "plant_id",
        "Resource 1",
        "Plant 2";
        date_time = DateTime(2001),
    )

    # Verify the data was added
    df_read = PSRDatabase.read_time_series_relation_table(
        db,
        "Resource",
        "plant_id",
        "Resource 1",
    )

    @test nrow(df_read) == 2
    @test df_read[1, :plant_id] == "Plant 1"
    @test df_read[2, :plant_id] == "Plant 2"

    # Test update_time_series_relation_row!
    PSRDatabase.update_time_series_relation_row!(
        db,
        "Resource",
        "plant_id",
        "Resource 1",
        "Plant 3";
        date_time = DateTime(2000),
    )

    # Verify the update
    df_updated = PSRDatabase.read_time_series_relation_table(
        db,
        "Resource",
        "plant_id",
        "Resource 1",
    )

    @test nrow(df_updated) == 2
    @test df_updated[1, :plant_id] == "Plant 3"  # Updated from Plant 1
    @test df_updated[2, :plant_id] == "Plant 2"  # Unchanged

    # Test that update throws error for non-existent dimension
    @test_throws PSRDatabase.DatabaseException PSRDatabase.update_time_series_relation_row!(
        db,
        "Resource",
        "plant_id",
        "Resource 1",
        "Plant 1";
        date_time = DateTime(2010),  # This date doesn't exist
    )

    PSRDatabase.close!(db)
    return rm(db_path)
end

function test_update_time_series_relations_with_multiple_dimensions()
    path_schema = joinpath(@__DIR__, "test_schema.sql")
    db_path = joinpath(@__DIR__, "test_multidim.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)

    # Create configuration
    PSRDatabase.create_element!(db, "Configuration"; value1 = 1.0)

    # Create plants
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 1")
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 2")
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 3")

    # Create resource
    PSRDatabase.create_element!(db, "Resource"; label = "Resource 1")

    # Test add_time_series_relation_row! with multiple dimensions
    # Add data for different date_time, block, and scenario combinations
    PSRDatabase.add_time_series_relation_row!(
        db,
        "Resource",
        "plant_dispatch_id",
        "Resource 1",
        "Plant 1";
        date_time = DateTime(2000),
        block = 1,
        scenario = 1,
    )

    PSRDatabase.add_time_series_relation_row!(
        db,
        "Resource",
        "plant_dispatch_id",
        "Resource 1",
        "Plant 2";
        date_time = DateTime(2000),
        block = 1,
        scenario = 2,
    )

    PSRDatabase.add_time_series_relation_row!(
        db,
        "Resource",
        "plant_dispatch_id",
        "Resource 1",
        "Plant 1";
        date_time = DateTime(2000),
        block = 2,
        scenario = 1,
    )

    PSRDatabase.add_time_series_relation_row!(
        db,
        "Resource",
        "plant_dispatch_id",
        "Resource 1",
        "Plant 2";
        date_time = DateTime(2001),
        block = 1,
        scenario = 1,
    )

    # Verify the data was added
    df_read = PSRDatabase.read_time_series_relation_table(
        db,
        "Resource",
        "plant_dispatch_id",
        "Resource 1",
    )

    @test nrow(df_read) == 4
    @test df_read[1, :plant_dispatch_id] == "Plant 1"
    @test df_read[1, :date_time] == "2000-01-01T00:00:00"
    @test df_read[1, :block] == 1
    @test df_read[1, :scenario] == 1

    @test df_read[2, :plant_dispatch_id] == "Plant 2"
    @test df_read[2, :scenario] == 2

    @test df_read[3, :plant_dispatch_id] == "Plant 1"
    @test df_read[3, :block] == 2

    @test df_read[4, :plant_dispatch_id] == "Plant 2"
    @test df_read[4, :date_time] == "2001-01-01T00:00:00"

    # Test update_time_series_relation_row! with multiple dimensions
    # Update the first entry to point to Plant 3
    PSRDatabase.update_time_series_relation_row!(
        db,
        "Resource",
        "plant_dispatch_id",
        "Resource 1",
        "Plant 3";
        date_time = DateTime(2000),
        block = 1,
        scenario = 1,
    )

    # Update entry with block = 2
    PSRDatabase.update_time_series_relation_row!(
        db,
        "Resource",
        "plant_dispatch_id",
        "Resource 1",
        "Plant 3";
        date_time = DateTime(2000),
        block = 2,
        scenario = 1,
    )

    # Verify the updates
    df_updated = PSRDatabase.read_time_series_relation_table(
        db,
        "Resource",
        "plant_dispatch_id",
        "Resource 1",
    )

    @test nrow(df_updated) == 4
    @test df_updated[1, :plant_dispatch_id] == "Plant 3"  # Updated from Plant 1
    @test df_updated[1, :block] == 1
    @test df_updated[1, :scenario] == 1

    @test df_updated[2, :plant_dispatch_id] == "Plant 2"  # Unchanged
    @test df_updated[2, :scenario] == 2

    @test df_updated[3, :plant_dispatch_id] == "Plant 3"  # Updated from Plant 1
    @test df_updated[3, :block] == 2

    @test df_updated[4, :plant_dispatch_id] == "Plant 2"  # Unchanged
    @test df_updated[4, :date_time] == "2001-01-01T00:00:00"

    # Test that update throws error for non-existent dimension combination
    @test_throws PSRDatabase.DatabaseException PSRDatabase.update_time_series_relation_row!(
        db,
        "Resource",
        "plant_dispatch_id",
        "Resource 1",
        "Plant 1";
        date_time = DateTime(2010),  # This date doesn't exist
        block = 1,
        scenario = 1,
    )

    # Test error when missing a dimension
    @test_throws PSRDatabase.DatabaseException PSRDatabase.update_time_series_relation_row!(
        db,
        "Resource",
        "plant_dispatch_id",
        "Resource 1",
        "Plant 1";
        date_time = DateTime(2000),
        block = 1,
        # Missing scenario dimension
    )

    # Test error when providing wrong dimension name
    @test_throws PSRDatabase.DatabaseException PSRDatabase.update_time_series_relation_row!(
        db,
        "Resource",
        "plant_dispatch_id",
        "Resource 1",
        "Plant 1";
        date_time = DateTime(2000),
        block = 1,
        stage = 1,  # Wrong dimension name (should be scenario)
    )

    PSRDatabase.close!(db)
    return rm(db_path)
end

function runtests()
    for name in names(@__MODULE__; all = true)
        if startswith("$(name)", "test_")
            @testset "$(name)" begin
                getfield(@__MODULE__, name)()
            end
        end
    end
end

end # module

TestTimeSeriesRelationsBasic.runtests()
