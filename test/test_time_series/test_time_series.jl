module TestTimeController

using PSRDatabase
using SQLite
using Dates
using DataFrames
using Test

function _test_row(cached_data, answer)
    @test length(cached_data) == length(answer)
    for i in eachindex(cached_data)
        if isnan(answer[i])
            @test isnan(cached_data[i])
        else
            @test cached_data[i] == answer[i]
        end
    end
end

function _test_table(table, answer)
    for (i, row) in enumerate(eachrow(table))
        for col in names(table)
            if col == "date_time"
                @test DateTime(row[col]) == answer[i, col]
                continue
            end
            if ismissing(answer[i, col])
                @test ismissing(row[col])
            else
                @test row[col] == answer[i, col]
            end
        end
    end
end

#####################
# Time Series Table #
#####################

function test_read_time_series_single()
    path_schema = joinpath(@__DIR__, "test_read_time_series.sql")
    db_path = joinpath(@__DIR__, "test_read_time_series.sqlite")
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

    # return single dataframe

    for i in 1:3
        df_group1_answer = DataFrame(;
            date_time = [DateTime(2000), DateTime(2001)],
            some_vector1 = [1.0, 2.0] .* i,
            some_vector2 = [2.0, 3.0] .* i,
        )
        df_group2_answer = DataFrame(;
            date_time = [DateTime(2000), DateTime(2000), DateTime(2001), DateTime(2001)],
            block = [1, 2, 1, 2],
            some_vector3 = [1.0, missing, 3.0, 4.0] .* i,
        )
        df_group3_answer = DataFrame(;
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

        all_answers = [df_group1_answer, df_group2_answer, df_group3_answer]

        # iterating over the three groups

        for df_answer in all_answers
            for col in names(df_answer)
                if startswith(col, "some_vector")
                    df = PSRDatabase.read_time_series_table(
                        db,
                        "Resource",
                        col,
                        "Resource $i",
                    )
                    _test_table(df, df_answer)
                end
            end
        end
    end

    PSRDatabase.close!(db)
    GC.gc()
    GC.gc()
    rm(db_path)
    @test true
    return nothing
end

# ##################
# # Time Controller#
# ##################

# # For each date, test the returned value with the expected value
function test_time_controller_read()
    path_schema = joinpath(@__DIR__, "test_time_controller.sql")
    db_path = joinpath(@__DIR__, "test_time_controller_read.sqlite")
    GC.gc()
    GC.gc()
    if isfile(db_path)
        rm(db_path)
    end

    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)

    df = DataFrame(;
        date_time = [DateTime(2000), DateTime(2001), DateTime(2002)],
        some_vector1 = [missing, 1.0, 2.0],
        some_vector2 = [1.0, 2.0, 3.0],
        some_vector3 = [3.0, 2.0, 1.0],
        some_vector4 = [1.0, missing, 5.0],
        some_vector5 = [missing, missing, missing],
        some_vector6 = [6.0, missing, missing],
    )
    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        group1 = df,
    )

    PSRDatabase.close!(db)
    db = PSRDatabase.load_db(db_path; read_only = true)

    some_vector1_answer = [[NaN], [1.0], [2.0]]
    some_vector2_answer = [[1.0], [2.0], [3.0]]
    some_vector3_answer = [[3.0], [2.0], [1.0]]
    some_vector4_answer = [[1.0], [1.0], [5.0]]
    some_vector5_answer = [[NaN], [NaN], [NaN]]
    some_vector6_answer = [[6.0], [6.0], [6.0]]

    # test for dates in correct sequence
    for d_i in eachindex(df.date_time)
        cached_1 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector1";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_1, some_vector1_answer[d_i])

        cached_2 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector2";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_2, some_vector2_answer[d_i])

        cached_3 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector3";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_3, some_vector3_answer[d_i])

        cached_4 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector4";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_4, some_vector4_answer[d_i])

        cached_5 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector5";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_5, some_vector5_answer[d_i])

        cached_6 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector6";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_6, some_vector6_answer[d_i])
    end

    # test for dates in reverse sequence
    for d_i in reverse(eachindex(df.date_time))
        cached_1 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector1";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_1, some_vector1_answer[d_i])

        cached_2 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector2";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_2, some_vector2_answer[d_i])

        cached_3 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector3";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_3, some_vector3_answer[d_i])

        cached_4 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector4";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_4, some_vector4_answer[d_i])

        cached_5 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector5";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_5, some_vector5_answer[d_i])

        cached_6 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector6";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_6, some_vector6_answer[d_i])
    end

    # test for dates in random sequence
    for d_i in [2, 1, 3]
        cached_1 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector1";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_1, some_vector1_answer[d_i])

        cached_2 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector2";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_2, some_vector2_answer[d_i])

        cached_3 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector3";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_3, some_vector3_answer[d_i])

        cached_4 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector4";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_4, some_vector4_answer[d_i])

        cached_5 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector5";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_5, some_vector5_answer[d_i])

        cached_6 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector6";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_6, some_vector6_answer[d_i])
    end

    PSRDatabase.close!(db)
    return rm(db_path)
end

function test_time_controller_read_more_agents()
    path_schema = joinpath(@__DIR__, "test_time_controller.sql")
    db_path = joinpath(@__DIR__, "test_time_controller_read_multiple.sqlite")
    GC.gc()
    GC.gc()
    if isfile(db_path)
        rm(db_path)
    end

    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)

    df = DataFrame(;
        date_time = [DateTime(2000), DateTime(2001), DateTime(2002)],
        some_vector1 = [missing, 1.0, 2.0],
        some_vector2 = [1.0, missing, 5.0],
    )
    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        group1 = df,
    )

    df2 = DataFrame(;
        date_time = [DateTime(2000), DateTime(2001), DateTime(2002)],
        some_vector1 = [missing, 10.0, 20.0],
        some_vector2 = [10.0, missing, 50.0],
    )
    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 2",
        group1 = df2,
    )

    PSRDatabase.close!(db)
    db = PSRDatabase.load_db(db_path; read_only = true)

    some_vector1_answer = [[NaN, NaN], [1.0, 10.0], [2.0, 20.0]]
    some_vector2_answer = [[1.0, 10.0], [1.0, 10.0], [5.0, 50.0]]

    # test for dates in correct sequence
    for d_i in eachindex(df.date_time)
        cached_1 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector1";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_1, some_vector1_answer[d_i])

        cached_2 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector2";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_2, some_vector2_answer[d_i])
    end

    PSRDatabase.close!(db)
    return rm(db_path)
end

function test_time_controller_read_more_agents_2()
    path_schema = joinpath(@__DIR__, "test_time_controller.sql")
    db_path = joinpath(@__DIR__, "test_time_controller_read_multiple_2.sqlite")
    GC.gc()
    GC.gc()
    if isfile(db_path)
        rm(db_path)
    end

    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)

    df = DataFrame(;
        date_time = [DateTime(2000)],
        some_vector1 = [missing],
        some_vector2 = [1.0],
    )
    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        group1 = df,
    )

    df2 = DataFrame(;
        date_time = [DateTime(2000)],
        some_vector1 = [1.0],
        some_vector2 = [10.0],
    )
    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 2",
        group1 = df2,
    )

    PSRDatabase.close!(db)
    db = PSRDatabase.load_db(db_path; read_only = true)

    some_vector1_answer = [[NaN, 1.0]]
    some_vector2_answer = [[1.0, 10.0]]

    # test for dates in correct sequence
    for d_i in eachindex(df.date_time)
        cached_1 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector1";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_1, some_vector1_answer[d_i])

        cached_2 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector2";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_2, some_vector2_answer[d_i])
    end

    PSRDatabase.close!(db)
    return rm(db_path)
end

function test_time_controller_empty()
    path_schema = joinpath(@__DIR__, "test_time_controller.sql")
    db_path = joinpath(@__DIR__, "test_time_controller_read_empty.sqlite")
    GC.gc()
    GC.gc()
    if isfile(db_path)
        rm(db_path)
    end

    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)

    PSRDatabase.close!(db)
    db = PSRDatabase.load_db(db_path; read_only = true)

    empty_cache = PSRDatabase.read_time_series_row(
        db,
        "Resource",
        "some_vector1";
        date_time = DateTime(2000),
    )
    _test_row(empty_cache, [])

    PSRDatabase.close!(db)
    return rm(db_path)
end

function test_time_controller_filled_then_empty()
    path_schema = joinpath(@__DIR__, "test_time_controller.sql")
    db_path = joinpath(@__DIR__, "test_time_controller_read_filled_then_empty.sqlite")
    GC.gc()
    GC.gc()
    if isfile(db_path)
        rm(db_path)
    end

    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)
    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)

    df = DataFrame(;
        date_time = [DateTime(2000), DateTime(2001), DateTime(2002)],
        some_vector1 = [missing, 1.0, 2.0],
        some_vector2 = [1.0, missing, 5.0],
    )
    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        group1 = df,
    )

    df2 = DataFrame(;
        date_time = [DateTime(2000), DateTime(2001), DateTime(2002)],
        some_vector1 = [missing, 10.0, 20.0],
        some_vector2 = [10.0, missing, 50.0],
    )
    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 2",
        group1 = df2,
    )

    PSRDatabase.close!(db)
    db = PSRDatabase.load_db(db_path; read_only = true)

    some_vector1_answer = [[NaN, NaN], [1.0, 10.0], [2.0, 20.0]]
    some_vector2_answer = [[1.0, 10.0], [1.0, 10.0], [5.0, 50.0]]

    # test for dates in correct sequence
    for d_i in eachindex(df.date_time)
        cached_1 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector1";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_1, some_vector1_answer[d_i])

        cached_2 = PSRDatabase.read_time_series_row(
            db,
            "Resource",
            "some_vector2";
            date_time = DateTime(df.date_time[d_i]),
        )
        _test_row(cached_2, some_vector2_answer[d_i])
    end

    PSRDatabase.close!(db)

    db = PSRDatabase.load_db(db_path; read_only = false)

    PSRDatabase.delete_element!(db, "Resource", "Resource 1")
    PSRDatabase.delete_element!(db, "Resource", "Resource 2")

    PSRDatabase.close!(db)

    db = PSRDatabase.load_db(db_path; read_only = true)

    empty_cache = PSRDatabase.read_time_series_row(
        db,
        "Resource",
        "some_vector1";
        date_time = DateTime(2000),
    )
    _test_row(empty_cache, [])

    PSRDatabase.close!(db)

    return rm(db_path)
end

function test_update_time_series()
    path_schema = joinpath(@__DIR__, "test_read_time_series.sql")
    db_path = joinpath(@__DIR__, "test_update_time_series.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)

    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)

    df_time_series_group1 = DataFrame(;
        date_time = [DateTime(2000), DateTime(2001)],
        some_vector1 = [1.0, 2.0],
        some_vector2 = [2.0, 3.0],
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
        some_vector5 = [1.0, 2.0, 3.0, 4.0, 1, 2, 3, 4],
        some_vector6 = [1.0, 2.0, 3.0, 4.0, 1, 2, 3, 4],
    )
    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        group1 = df_time_series_group1,
        group3 = df_time_series_group3,
    )

    PSRDatabase.update_time_series_row!(
        db,
        "Resource",
        "some_vector1",
        "Resource 1",
        10.0;
        date_time = DateTime(2001),
    )

    PSRDatabase.update_time_series_row!(
        db,
        "Resource",
        "some_vector2",
        "Resource 1",
        50.0;
        date_time = DateTime(2001),
    )

    PSRDatabase.update_time_series_row!(
        db,
        "Resource",
        "some_vector5",
        "Resource 1",
        10.0;
        date_time = DateTime(2000),
        block = 1,
        segment = 2,
    )

    PSRDatabase.update_time_series_row!(
        db,
        "Resource",
        "some_vector5",
        "Resource 1",
        3.0;
        date_time = DateTime(2000),
        block = 1,
        segment = 1,
    )

    PSRDatabase.update_time_series_row!(
        db,
        "Resource",
        "some_vector6",
        "Resource 1",
        33.0;
        date_time = DateTime(2000),
        block = 1,
        segment = 3,
    )

    @test_throws PSRDatabase.DatabaseException PSRDatabase.update_time_series_row!(
        db,
        "Resource",
        "some_vector6",
        "Resource 1",
        10.0;
        date_time = DateTime(2000),
        segment = 2,
    )

    @test_throws PSRDatabase.DatabaseException PSRDatabase.update_time_series_row!(
        db,
        "Resource",
        "some_vector5",
        "Resource 1",
        3.0;
        date_time = DateTime(1890),
        block = 999,
        segment = 2,
    )

    df_group1_answer = DataFrame(;
        date_time = [DateTime(2000), DateTime(2001)],
        some_vector1 = [1.0, 10.0],
        some_vector2 = [2.0, 50.0],
    )
    df_group3_answer = DataFrame(;
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
        some_vector5 = [3.0, 10.0, 3.0, 4.0, 1, 2, 3, 4],
        some_vector6 = [1.0, 2.0, 33.0, 4.0, 1, 2, 3, 4],
    )

    all_answers = [df_group1_answer, df_group3_answer]

    # iterating over the three groups

    for df_answer in all_answers
        for col in names(df_answer)
            if startswith(col, "some_vector")
                df = PSRDatabase.read_time_series_table(
                    db,
                    "Resource",
                    col,
                    "Resource 1",
                )
                _test_table(df, df_answer)
            end
        end
    end

    PSRDatabase.close!(db)
    GC.gc()
    GC.gc()
    rm(db_path)
    @test true
    return nothing
end

function test_delete_time_series()
    path_schema = joinpath(@__DIR__, "test_read_time_series.sql")
    db_path = joinpath(@__DIR__, "test_delete_time_series.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)

    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)

    df_time_series_group1 = DataFrame(;
        date_time = [DateTime(2000), DateTime(2001)],
        some_vector1 = [1.0, 2.0],
        some_vector2 = [2.0, 3.0],
    )
    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        group1 = df_time_series_group1,
    )

    PSRDatabase.delete_time_series!(
        db,
        "Resource",
        "group1",
        "Resource 1",
    )

    df = PSRDatabase.read_time_series_table(
        db,
        "Resource",
        "some_vector1",
        "Resource 1",
    )

    @test isempty(df)

    df = PSRDatabase.read_time_series_table(
        db,
        "Resource",
        "some_vector2",
        "Resource 1",
    )

    @test isempty(df)

    PSRDatabase.close!(db)
    GC.gc()
    GC.gc()
    rm(db_path)
    @test true
    return nothing
end

function test_create_wrong_time_series()
    path_schema = joinpath(@__DIR__, "test_read_time_series.sql")
    db_path = joinpath(@__DIR__, "test_create_wrong_time_series.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)

    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)

    df_time_series_group1_wrong = DataFrame(;
        date_time = [DateTime(2000), DateTime(2001)],
        some_vector1 = [1.0, 2.0],
        some_vector20 = [2.0, 3.0],
    )

    df_time_series_group1_wrong2 = DataFrame(;
        date_time = [DateTime(2000), DateTime(2001)],
        block = [1, 2],
        some_vector1 = [1.0, 2.0],
        some_vector2 = [2.0, 3.0],
    )

    df_time_series_group1_wrong3 = DataFrame(;
        date_time = [DateTime(2000), DateTime(2001)],
        something = [1, 2],
        some_vector1 = [1.0, 2.0],
        some_vector2 = [2.0, 3.0],
    )

    df_time_series_group1 = DataFrame(;
        date_time = [DateTime(2000), DateTime(2001)],
        some_vector1 = [1.0, 2.0],
        some_vector2 = [2.0, 3.0],
    )

    @test_throws PSRDatabase.DatabaseException PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        group1 = df_time_series_group1_wrong,
    )

    @test_throws PSRDatabase.DatabaseException PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        group1 = df_time_series_group1_wrong2,
    )

    @test_throws PSRDatabase.DatabaseException PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        group1 = df_time_series_group1_wrong3,
    )

    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
        group1 = df_time_series_group1,
    )

    PSRDatabase.close!(db)
    GC.gc()
    GC.gc()
    rm(db_path)
    @test true
    return nothing
end

function test_add_time_series_row()
    path_schema = joinpath(@__DIR__, "test_read_time_series.sql")
    db_path = joinpath(@__DIR__, "test_add_time_series_row.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)

    PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)

    PSRDatabase.create_element!(
        db,
        "Resource";
        label = "Resource 1",
    )

    PSRDatabase.add_time_series_row!(
        db,
        "Resource",
        "some_vector1",
        "Resource 1",
        1.0;
        date_time = DateTime(2000),
    )

    PSRDatabase.add_time_series_row!(
        db,
        "Resource",
        "some_vector2",
        "Resource 1",
        2.0;
        date_time = DateTime(2000),
    )

    PSRDatabase.add_time_series_row!(
        db,
        "Resource",
        "some_vector3",
        "Resource 1",
        3.0;
        date_time = DateTime(2001),
        block = 1,
    )

    PSRDatabase.add_time_series_row!(
        db,
        "Resource",
        "some_vector4",
        "Resource 1",
        4.0;
        date_time = DateTime(2001),
        block = 1,
    )

    # Attribute is not a time series
    @test_throws PSRDatabase.DatabaseException PSRDatabase.add_time_series_row!(
        db,
        "Resource",
        "label",
        "Resource 1",
        4.0;
        date_time = DateTime(2001),
    )

    # Wrong dimensions
    @test_throws PSRDatabase.DatabaseException PSRDatabase.add_time_series_row!(
        db,
        "Resource",
        "some_vector1",
        "Resource 1",
        4.0;
        date_time = DateTime(2001),
        block = 1,
        segment = 1,
    )

    df_some_vector1 = PSRDatabase.read_time_series_table(
        db,
        "Resource",
        "some_vector1",
        "Resource 1",
    )

    df_some_vector2 = PSRDatabase.read_time_series_table(
        db,
        "Resource",
        "some_vector2",
        "Resource 1",
    )

    df_some_vector3 = PSRDatabase.read_time_series_table(
        db,
        "Resource",
        "some_vector3",
        "Resource 1",
    )

    df_some_vector4 = PSRDatabase.read_time_series_table(
        db,
        "Resource",
        "some_vector4",
        "Resource 1",
    )

    @test df_some_vector1[1, :some_vector1] == 1.0
    @test df_some_vector2[1, :some_vector2] == 2.0
    @test df_some_vector3[1, :some_vector3] == 3.0
    @test df_some_vector4[1, :some_vector4] == 4.0

    PSRDatabase.close!(db)
    GC.gc()
    GC.gc()
    rm(db_path)
    return nothing
end

function test_unordered_time_controller()
    path_schema = joinpath(@__DIR__, "test_unordered_time_controller.sql")
    db_path = joinpath(@__DIR__, "test_unordered_time_controller.sqlite")
    db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)

    PSRDatabase.create_element!(db, "MyTable"; label = "table1")

    PSRDatabase.add_time_series_row!(
        db,
        "MyTable",
        "my_parameter",
        "table1",
        0;
        date_time = DateTime(0),
    )

    PSRDatabase.add_time_series_row!(
        db,
        "MyTable",
        "my_parameter",
        "table1",
        2020;
        date_time = DateTime(2020),
    )

    PSRDatabase.add_time_series_row!(
        db,
        "MyTable",
        "my_parameter",
        "table1",
        1900;
        date_time = DateTime(1900),
    )

    PSRDatabase.add_time_series_row!(
        db,
        "MyTable",
        "my_parameter",
        "table1",
        2018;
        date_time = DateTime(2018),
    )

    PSRDatabase.close!(db)

    db = PSRDatabase.load_db(db_path; read_only = true)

    value = PSRDatabase.read_time_series_row(
        db,
        "MyTable",
        "my_parameter";
        date_time = DateTime(2021),
    )

    @test value[1] == 2020

    PSRDatabase.close!(db)
    GC.gc()
    GC.gc()
    rm(db_path)
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

TestTimeController.runtests()

end
