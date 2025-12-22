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
    # return rm(db_path)
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
