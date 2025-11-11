module TestUpdate

using PSRDatabase
using SQLite
using Test

function test_update_scalar_relations()
    path_schema = joinpath(@__DIR__, "test_update_scalar_relations.sql")
    db_path = joinpath(@__DIR__, "test_update_scalar_relations.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)
    PSRDatabase.create_element!(db, "Resource"; label = "Resource 1", type = "E")
    PSRDatabase.create_element!(db, "Resource"; label = "Resource 2", type = "E")
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 1", capacity = 50.0)
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 2", capacity = 50.0)
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 3", capacity = 50.0)

    # Valid relations
    PSRDatabase.set_scalar_relation!(
        db,
        "Plant",
        "Resource",
        "Plant 1",
        "Resource 1",
        "id",
    )
    PSRDatabase.set_scalar_relation!(
        db,
        "Plant",
        "Resource",
        "Plant 1",
        "Resource 2",
        "id",
    )
    PSRDatabase.set_scalar_relation!(
        db,
        "Plant",
        "Resource",
        "Plant 2",
        "Resource 1",
        "id",
    )
    PSRDatabase.set_scalar_relation!(
        db,
        "Plant",
        "Resource",
        "Plant 3",
        "Resource 2",
        "id",
    )
    PSRDatabase.set_scalar_relation!(
        db,
        "Plant",
        "Plant",
        "Plant 3",
        "Plant 1",
        "turbine_to",
    )
    PSRDatabase.set_scalar_relation!(
        db,
        "Plant",
        "Plant",
        "Plant 1",
        "Plant 2",
        "spill_to",
    )

    # invalid relations
    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_scalar_relation!(
        db,
        "Plant",
        "Resource",
        "Plant 1",
        "Resource 1",
        "wrong",
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_scalar_relation!(
        db,
        "Plant",
        "Resource",
        "Plant 1",
        "Resource 4",
        "id",
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_scalar_relation!(
        db,
        "Plant",
        "Resource",
        "Plant 5",
        "Resource 1",
        "id",
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_scalar_relation!(
        db,
        "Resource",
        "Resource",
        "Resource 1",
        "Resource 2",
        "wrong",
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_scalar_relation!(
        db,
        "Plant",
        "Plant",
        "Plant 1",
        "Plant 2",
        "wrong",
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_scalar_relation!(
        db,
        "Plant",
        "Plant",
        "Plant 1",
        "Plant 1",
        "turbine_to",
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_scalar_relation!(
        db,
        "Plant",
        "Plant",
        "Plant 1",
        "Plant 2",
        "id",
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_scalar_relation!(
        db,
        "Plant",
        "Plant",
        "Plant",
        "Plant",
        "id",
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_scalar_relation!(
        db,
        "Plant",
        "Plant 1",
        "Plant",
        "Plant 2",
        "id",
    )

    PSRDatabase.close!(db)
    return rm(db_path)
end

function test_update_vector_relations()
    path_schema = joinpath(@__DIR__, "test_update_vector_relations.sql")
    db_path = joinpath(@__DIR__, "test_update_vector_relations.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case")
    PSRDatabase.create_element!(db, "Cost"; label = "Cost 1")
    PSRDatabase.create_element!(db, "Cost"; label = "Cost 2")
    PSRDatabase.create_element!(db, "Cost"; label = "Cost 3")
    PSRDatabase.create_element!(db, "Cost"; label = "Cost 4")
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 1", capacity = 49.0)
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 2", capacity = 50.0)
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 3", capacity = 51.0)
    PSRDatabase.create_element!(
        db,
        "Plant";
        label = "Plant 4",
        capacity = 51.0,
        some_factor = [0.1, 0.3],
    )

    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_scalar_relation!(
        db,
        "Plant",
        "Cost",
        "Plant 1",
        ["Cost 1"],
        "some_relation_type",
    )
    PSRDatabase.set_vector_relation!(
        db,
        "Plant",
        "Cost",
        "Plant 1",
        ["Cost 1"],
        "some_relation_type",
    )
    PSRDatabase.set_vector_relation!(
        db,
        "Plant",
        "Cost",
        "Plant 2",
        ["Cost 1", "Cost 2", "Cost 3"],
        "some_relation_type",
    )
    PSRDatabase.set_vector_relation!(
        db,
        "Plant",
        "Cost",
        "Plant 4",
        ["Cost 1", "Cost 3"],
        "id",
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_vector_relation!(
        db,
        "Plant",
        "Cost",
        "Plant 2",
        ["Cost 10", "Cost 2"],
        "some_relation_type",
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_vector_relation!(
        db,
        "Plant",
        "Cost",
        "Plant 2",
        ["Cost 1", "Cost 2", "Cost 3"],
        "wrong",
    )

    PSRDatabase.close!(db)
    return rm(db_path)
end

function test_update_set_relations()
    path_schema = joinpath(@__DIR__, "test_update_set_relations.sql")
    db_path = joinpath(@__DIR__, "test_update_set_relations.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case")
    PSRDatabase.create_element!(db, "Cost"; label = "Cost 1")
    PSRDatabase.create_element!(db, "Cost"; label = "Cost 2")
    PSRDatabase.create_element!(db, "Cost"; label = "Cost 3")
    PSRDatabase.create_element!(db, "Cost"; label = "Cost 4")
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 1", capacity = 49.0)
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 2", capacity = 50.0)
    PSRDatabase.create_element!(db, "Plant"; label = "Plant 3", capacity = 51.0)
    PSRDatabase.create_element!(
        db,
        "Plant";
        label = "Plant 4",
        capacity = 51.0,
        some_factor = [0.1, 0.3],
    )

    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_scalar_relation!(
        db,
        "Plant",
        "Cost",
        "Plant 1",
        ["Cost 1"],
        "some_relation_type",
    )
    PSRDatabase.set_set_relation!(
        db,
        "Plant",
        "Cost",
        "Plant 1",
        ["Cost 1"],
        "some_relation_type",
    )
    PSRDatabase.set_set_relation!(
        db,
        "Plant",
        "Cost",
        "Plant 2",
        ["Cost 1", "Cost 2", "Cost 3"],
        "some_relation_type",
    )
    PSRDatabase.set_set_relation!(
        db,
        "Plant",
        "Cost",
        "Plant 4",
        ["Cost 1", "Cost 3"],
        "id",
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_set_relation!(
        db,
        "Plant",
        "Cost",
        "Plant 2",
        ["Cost 10", "Cost 2"],
        "some_relation_type",
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_set_relation!(
        db,
        "Plant",
        "Cost",
        "Plant 2",
        ["Cost 1", "Cost 2", "Cost 3"],
        "wrong",
    )

    PSRDatabase.close!(db)
    return rm(db_path)
end

function test_update_scalar_parameters()
    path_schema = joinpath(@__DIR__, "test_update_scalar_parameters.sql")
    db_path = joinpath(@__DIR__, "test_update_scalar_parameters.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)
    PSRDatabase.create_element!(db, "Resource"; label = "Resource 1", type = "E")
    PSRDatabase.create_element!(db, "Resource"; label = "Resource 2", type = "E")
    PSRDatabase.create_element!(db, "Cost"; label = "Cost 1")
    PSRDatabase.create_element!(db, "Cost"; label = "Cost 2")

    PSRDatabase.update_scalar_parameter!(db, "Resource", "type", "Resource 1", "D")
    @test_throws PSRDatabase.DatabaseException PSRDatabase.update_scalar_parameter!(
        db,
        "Resource",
        "some_value",
        "Resource 4",
        1.0,
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.update_scalar_parameter!(
        db,
        "Resource",
        "invented_attribute",
        "Resource 4",
        1.0,
    )
    PSRDatabase.update_scalar_parameter!(
        db,
        "Resource",
        "some_value_1",
        "Resource 1",
        1.0,
    )
    PSRDatabase.update_parameter!(
        db,
        "Resource",
        "Resource 1";
        some_value_1 = 1.0,
    )
    PSRDatabase.update_scalar_parameter!(
        db,
        "Resource",
        "some_value_1",
        "Resource 1",
        1.0,
    )
    PSRDatabase.update_scalar_parameter!(
        db,
        "Resource",
        "some_value_2",
        "Resource 1",
        99.0,
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.update_scalar_parameter!(
        db,
        "Resource",
        "some_value_2",
        "Resource 1",
        "wrong!",
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.update_scalar_parameter!(
        db,
        "Resource",
        "cost_id",
        "Resource 1",
        "something",
    )
    PSRDatabase.close!(db)
    return rm(db_path)
end

function test_update_vector_parameters()
    path_schema = joinpath(@__DIR__, "test_update_vector_parameters.sql")
    db_path = joinpath(@__DIR__, "test_update_vector_parameters.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)
    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        type = "E",
        some_value_1 = [1.0, 2.0, 3.0],
    )
    PSRDatabase.create_element!(db, "Resource"; label = "Resource 2", type = "E")

    PSRDatabase.update_vector_parameters!(
        db,
        "Resource",
        "some_value_1",
        "Resource 1",
        [4.0, 5.0, 6.0],
    )
    PSRDatabase.update_vector_parameters!(
        db,
        "Resource",
        "some_value_2",
        "Resource 1",
        [4.0, 5.0, 6.0],
    )
    PSRDatabase.update_parameter!(
        db,
        "Resource",
        "Resource 1";
        some_value_1 = [7.0, 8.0, 9.0],
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.update_vector_parameters!(
        db,
        "Resource",
        "some_value_3",
        "Resource 1",
        [4.0, 5.0, 6.0],
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.update_vector_parameters!(
        db,
        "Resource",
        "some_value_1",
        "Resource 1",
        [1, 2, 3],
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.update_vector_parameters!(
        db,
        "Resource",
        "some_value_1",
        "Resource 1",
        [4.0, 5.0, 6.0, 7.0],
    )
    PSRDatabase.close!(db)
    return rm(db_path)
end

function test_update_set_parameters()
    path_schema = joinpath(@__DIR__, "test_update_set_parameters.sql")
    db_path = joinpath(@__DIR__, "test_update_set_parameters.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)
    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        type = "E",
        some_value_1 = [1.0, 2.0, 3.0],
    )
    PSRDatabase.create_element!(db, "Resource"; label = "Resource 2", type = "E")

    PSRDatabase.update_set_parameters!(
        db,
        "Resource",
        "some_value_1",
        "Resource 1",
        [4.0, 5.0, 6.0],
    )
    PSRDatabase.update_set_parameters!(
        db,
        "Resource",
        "some_value_2",
        "Resource 1",
        [4.0, 5.0, 6.0],
    )
    PSRDatabase.update_parameter!(
        db,
        "Resource",
        "Resource 1";
        some_value_1 = [7.0, 8.0, 9.0],
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.update_set_parameters!(
        db,
        "Resource",
        "some_value_3",
        "Resource 1",
        [4.0, 5.0, 6.0],
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.update_set_parameters!(
        db,
        "Resource",
        "some_value_1",
        "Resource 1",
        [1, 2, 3],
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.update_set_parameters!(
        db,
        "Resource",
        "some_value_1",
        "Resource 1",
        [4.0, 5.0, 6.0, 7.0],
    )
    PSRDatabase.close!(db)
    return rm(db_path)
end

function test_update_time_series_files()
    path_schema = joinpath(@__DIR__, "test_update_time_series_files.sql")
    db_path = joinpath(@__DIR__, "test_update_time_series_files.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)
    PSRDatabase.create_element!(db, "Resource"; label = "Resource 1")
    PSRDatabase.set_time_series_file!(db, "Resource"; wind_speed = "some_file.txt")
    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_time_series_file!(
        db,
        "Resource";
        wind_speed = ["some_file.txt"],
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_time_series_file!(
        db,
        "Resource";
        label = "RS",
    )
    PSRDatabase.set_time_series_file!(
        db,
        "Resource";
        wind_speed = "some_other_file.txt",
    )
    PSRDatabase.set_time_series_file!(
        db,
        "Resource";
        wind_speed = "speed.txt",
        wind_direction = "direction.txt",
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_time_series_file!(
        db,
        "Resource";
        wind_speed = "C:\\Users\\some_user\\some_file.txt",
    )
    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_time_series_file!(
        db,
        "Resource";
        wind_speed = "~/some_user/some_file.txt",
    )
    PSRDatabase.close!(db)
    return rm(db_path)
end

function test_update_time_series()
    path_schema = joinpath(@__DIR__, "test_update_time_series.sql")
    db_path = joinpath(@__DIR__, "test_update_time_series.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Plant"; label = "Solar")
    @test PSRDatabase.read_time_series_file(db, "Plant", "generation") == ""
    PSRDatabase.set_time_series_file!(db, "Plant"; generation = "hrrnew.csv")
    @test PSRDatabase.read_time_series_file(db, "Plant", "generation") == "hrrnew.csv"
    PSRDatabase.set_time_series_file!(db, "Plant"; generation = "hrrnew2.csv")
    @test PSRDatabase.read_time_series_file(db, "Plant", "generation") ==
          "hrrnew2.csv"
    PSRDatabase.close!(db)

    db = PSRDatabase.load_db(db_path)
    @test PSRDatabase.read_time_series_file(db, "Plant", "generation") ==
          "hrrnew2.csv"
    PSRDatabase.set_time_series_file!(db, "Plant"; generation = "hrrnew3.csv")
    @test PSRDatabase.read_time_series_file(db, "Plant", "generation") ==
          "hrrnew3.csv"

    PSRDatabase.create_element!(db, "Resource"; label = "Resource 1")
    @test_throws PSRDatabase.DatabaseException PSRDatabase.set_time_series_file!(
        db,
        "Resource";
        wind_speed = "some_file.txt",
    )
    PSRDatabase.set_time_series_file!(db, "Resource"; generation = "gen.txt")
    @test PSRDatabase.read_time_series_file(db, "Resource", "generation") == "gen.txt"
    @test PSRDatabase.read_time_series_file(db, "Resource", "other_generation") == ""

    PSRDatabase.set_time_series_file!(
        db,
        "Resource";
        generation = "gen.txt",
        other_generation = "other_gen.txt",
    )
    @test PSRDatabase.read_time_series_file(db, "Resource", "generation") == "gen.txt"
    @test PSRDatabase.read_time_series_file(db, "Resource", "other_generation") ==
          "other_gen.txt"

    PSRDatabase.close!(db)
    return rm(db_path)
end

function test_update_vector_with_empty_relations_id()
    path_schema = joinpath(dirname(@__DIR__), "test_create", "test_create_vectors_with_empty_relations.sql")
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
    )

    PSRDatabase.set_vector_relation!(
        db,
        "Plant",
        "Process",
        1,
        [typemin(Int), typemin(Int), 2],
        "substitute",
    )

    PSRDatabase.close!(db)
    GC.gc()
    GC.gc()
    rm(db_path)
    @test true
    return nothing
end

function test_update_vector_with_empty_relations_string()
    path_schema = joinpath(dirname(@__DIR__), "test_create", "test_create_vectors_with_empty_relations.sql")
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
    )

    PSRDatabase.set_vector_relation!(
        db,
        "Plant",
        "Process",
        "Steel Plant",
        ["", "", "Basic Oxygen Furnace"],
        "substitute",
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

TestUpdate.runtests()

end
