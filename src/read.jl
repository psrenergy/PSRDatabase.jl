"""
    const READ_METHODS_BY_CLASS_OF_ATTRIBUTE

A dictionary mapping attribute classes to their corresponding read method names in PSRDatabase.
"""
const READ_METHODS_BY_CLASS_OF_ATTRIBUTE = Dict(
    ScalarParameter => "read_scalar_parameters",
    ScalarRelation => "read_scalar_relations",
    VectorParameter => "read_vector_parameters",
    VectorRelation => "read_vector_relations",
    TimeSeries => "read_time_series_row",
    TimeSeriesFile => "read_time_series_file",
)

"""
    number_of_elements(db::DatabaseSQLite, collection_id::String)::Int

Return the total number of elements in the specified collection.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_id::String`: The identifier of the collection to count elements from

# Returns

  - `Int`: The number of elements in the collection
"""
function number_of_elements(db::DatabaseSQLite, collection_id::String)::Int
    query = "SELECT COUNT(*) FROM $collection_id"
    result = DBInterface.execute(db.sqlite_db, query)
    for row in result
        return row[1]
    end
end

"""
    _get_id(db::DatabaseSQLite, collection_id::String, label::String)::Integer

Internal function to retrieve the numeric ID for an element in a collection based on its label.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_id::String`: The identifier of the collection
  - `label::String`: The label of the element to find

# Returns

  - `Integer`: The numeric ID of the element

# Throws

  - Error if the label does not exist in the collection
"""
function _get_id(
    db::DatabaseSQLite,
    collection_id::String,
    label::String,
)::Integer
    query = "SELECT id FROM $collection_id WHERE label = '$label'"
    df = DBInterface.execute(db.sqlite_db, query) |> DataFrame
    if isempty(df)
        psr_database_sqlite_error(
            "label \"$label\" does not exist in collection \"$collection_id\".",
        )
    end
    result = df[!, 1][1]
    return result
end

"""
    read_scalar_parameters(db::DatabaseSQLite, collection_id::String, attribute_id::String; default::Union{Nothing, Any} = nothing)

Read all values of a scalar parameter attribute for all elements in a collection.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_id::String`: The identifier of the collection
  - `attribute_id::String`: The identifier of the scalar parameter attribute to read
  - `default::Union{Nothing, Any}`: Optional default value to use for missing data. If `nothing`, uses type-specific null values (NaN for Float64, typemin(Int64) for Int64, "" for String, typemin(DateTime) for DateTime)

# Returns

  - `Vector`: A vector containing the scalar parameter values for all elements, ordered by ID. The element type matches the attribute type (Float64, Int64, String, or DateTime)

# Examples

```julia
# Read labels (returns Vector{String})
labels = PSRDatabase.read_scalar_parameters(db, "Plant", "label")  # ["Plant 1", "Plant 2", "Plant 3"]

# Read numeric values (returns Vector{Float64})
capacities = PSRDatabase.read_scalar_parameters(db, "Plant", "capacity")  # [2.02, 53.0, 54.0]

# Read dates (returns Vector{DateTime})
dates = PSRDatabase.read_scalar_parameters(db, "Configuration", "date_initial")  # [DateTime(2020, 1, 1)]

# With default value for missing data
values = PSRDatabase.read_scalar_parameters(db, "Cost", "value_without_default"; default = 2.0)  # [2.0, 2.0]
```
"""
function read_scalar_parameters(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String;
    default::Union{Nothing, Any} = nothing,
)
    _throw_if_attribute_is_not_scalar_parameter(
        db,
        collection_id,
        attribute_id,
        :read,
    )

    attribute = _get_attribute(db, collection_id, attribute_id)
    table = _table_where_is_located(attribute)

    query = "SELECT $attribute_id FROM $table ORDER BY id"
    df = DBInterface.execute(db.sqlite_db, query) |> DataFrame
    results = df[!, 1]
    results = _treat_query_result(results, attribute, default)
    return results
end

"""
    read_scalar_parameter(db::DatabaseSQLite, collection_id::String, attribute_id::String, label::String; default::Union{Nothing, Any} = nothing)

Read the value of a scalar parameter attribute for a specific element identified by label.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_id::String`: The identifier of the collection
  - `attribute_id::String`: The identifier of the scalar parameter attribute to read
  - `label::String`: The label of the element to read from
  - `default::Union{Nothing, Any}`: Optional default value to use for missing data. If `nothing`, uses type-specific null values

# Returns

  - The scalar parameter value for the specified element. Type matches the attribute type (Float64, Int64, String, or DateTime)

# Examples

```julia
# Read a string label
name = PSRDatabase.read_scalar_parameter(db, "Resource", "label", "Resource 1")  # "Resource 1"

# Read a numeric value
capacity = PSRDatabase.read_scalar_parameter(db, "Plant", "capacity", "Plant 3")  # 54.0
```

# Throws

  - `DatabaseException` if the label does not exist in the collection
"""
function read_scalar_parameter(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    label::String;
    default::Union{Nothing, Any} = nothing,
)
    _throw_if_attribute_is_not_scalar_parameter(
        db,
        collection_id,
        attribute_id,
        :read,
    )

    id = _get_id(db, collection_id, label)

    return read_scalar_parameter(db, collection_id, attribute_id, id; default)
end

"""
    read_scalar_parameter(db::DatabaseSQLite, collection_id::String, attribute_id::String, id::Integer; default::Union{Nothing, Any} = nothing)

Read the value of a scalar parameter attribute for a specific element identified by numeric ID.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_id::String`: The identifier of the collection
  - `attribute_id::String`: The identifier of the scalar parameter attribute to read
  - `id::Integer`: The numeric ID of the element to read from
  - `default::Union{Nothing, Any}`: Optional default value to use for missing data. If `nothing`, uses type-specific null values

# Returns

  - The scalar parameter value for the specified element. Type matches the attribute type (Float64, Int64, String, or DateTime)

# Example

```julia
capacity = PSRDatabase.read_scalar_parameter(db, "Plant", "capacity", 1)  # 2.02
```
"""
function read_scalar_parameter(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    id::Integer;
    default::Union{Nothing, Any} = nothing,
)
    _throw_if_attribute_is_not_scalar_parameter(
        db,
        collection_id,
        attribute_id,
        :read,
    )
    attribute = _get_attribute(db, collection_id, attribute_id)
    table = _table_where_is_located(attribute)

    query = "SELECT $attribute_id FROM $table WHERE id = '$id'"
    df = DBInterface.execute(db.sqlite_db, query) |> DataFrame
    results = df[!, 1]
    results = _treat_query_result(results, attribute, default)
    return results[1]
end

"""
    read_vector_parameters(db::DatabaseSQLite, collection_id::String, attribute_id::String; default::Union{Nothing, Any} = nothing)

Read all values of a vector parameter attribute for all elements in a collection.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_id::String`: The identifier of the collection
  - `attribute_id::String`: The identifier of the vector parameter attribute to read
  - `default::Union{Nothing, Any}`: Optional default value to use for missing data. If `nothing`, uses type-specific null values

# Returns

  - `Vector{Vector}`: A vector of vectors, where each inner vector contains the parameter values for one element. Inner vector type matches the attribute type (Float64, Int64, String, or DateTime). Empty vectors are returned for elements with no data.

# Examples

```julia
# Read numeric vector parameters
values = PSRDatabase.read_vector_parameters(db, "Resource", "some_value")
# [[1, 2, 3.0], [1, 2, 4.0]]

# Read vector parameters with some empty elements
factors = PSRDatabase.read_vector_parameters(db, "Plant", "some_factor")
# [[1.0], [1.0, 2.0], Float64[], [1.0, 2.0]]

# Read date vectors
dates = PSRDatabase.read_vector_parameters(db, "Plant", "date_some_date")
# [[DateTime(2020, 1, 1)], [DateTime(2020, 1, 1), DateTime(2020, 1, 2)], DateTime[], ...]
```
"""
function read_vector_parameters(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String;
    default::Union{Nothing, Any} = nothing,
)
    _throw_if_attribute_is_not_vector_parameter(
        db,
        collection_id,
        attribute_id,
        :read,
    )
    attribute = _get_attribute(db, collection_id, attribute_id)
    ids_in_table = read_scalar_parameters(db, collection_id, "id")

    results = Vector{attribute.type}[]
    for id in ids_in_table
        push!(results, _query_vector(db, attribute, id; default))
    end

    return results
end

"""
    read_vector_parameter(db::DatabaseSQLite, collection_id::String, attribute_id::String, label::String; default::Union{Nothing, Any} = nothing)

Read the values of a vector parameter attribute for a specific element identified by label.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_id::String`: The identifier of the collection
  - `attribute_id::String`: The identifier of the vector parameter attribute to read
  - `label::String`: The label of the element to read from
  - `default::Union{Nothing, Any}`: Optional default value to use for missing data. If `nothing`, uses type-specific null values

# Returns

  - `Vector`: A vector containing the parameter values for the specified element. Type matches the attribute type (Float64, Int64, String, or DateTime). Returns an empty vector if no data exists.

# Examples

```julia
# Read vector with data
factors = PSRDatabase.read_vector_parameter(db, "Plant", "some_factor", "Plant 1")  # [1.0]
factors = PSRDatabase.read_vector_parameter(db, "Plant", "some_factor", "Plant 2")  # [1.0, 2.0]

# Read empty vector
factors = PSRDatabase.read_vector_parameter(db, "Plant", "some_factor", "Plant 3")  # Float64[]

# Read date vectors
dates = PSRDatabase.read_vector_parameter(db, "Plant", "date_some_date", "Plant 2")
# [DateTime(2020, 1, 1), DateTime(2020, 1, 2)]

# Elements with null dates return typemin(DateTime)
dates = PSRDatabase.read_vector_parameter(db, "Plant", "date_some_date", "Plant 4")
# [typemin(DateTime), typemin(DateTime)]
```

# Throws

  - `DatabaseException` if the label does not exist in the collection
"""
function read_vector_parameter(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    label::String;
    default::Union{Nothing, Any} = nothing,
)
    _throw_if_attribute_is_not_vector_parameter(
        db,
        collection_id,
        attribute_id,
        :read,
    )
    attribute = _get_attribute(db, collection_id, attribute_id)
    id = read_scalar_parameter(db, collection_id, "id", label)
    return _query_vector(db, attribute, id; default)
end

"""
    _query_vector(db::DatabaseSQLite, attribute::VectorParameter, id::Integer; default::Union{Nothing, Any} = nothing)

Internal function to query vector parameter values for a specific element.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `attribute::VectorParameter`: The vector parameter attribute
  - `id::Integer`: The numeric ID of the element
  - `default::Union{Nothing, Any}`: Optional default value for missing data

# Returns

  - `Vector`: The vector of parameter values, ordered by vector_index
"""
function _query_vector(
    db::DatabaseSQLite,
    attribute::VectorParameter,
    id::Integer;
    default::Union{Nothing, Any} = nothing,
)
    query = "SELECT $(attribute.id) FROM $(attribute.table_where_is_located) WHERE id = '$id' ORDER BY vector_index"
    df = DBInterface.execute(db.sqlite_db, query) |> DataFrame
    results = df[!, 1]
    results = _treat_query_result(results, attribute, default)
    return results
end

"""
    end_date_query(db::DatabaseSQLite, attribute::Attribute)

Query the maximum (most recent) date available in a time series attribute table.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `attribute::Attribute`: The time series attribute

# Returns

  - `DateTime`: The most recent date in the time series, or `DateTime(0)` if no data exists
"""
function end_date_query(db::DatabaseSQLite, attribute::Attribute)
    # First checks if the date or dimension value is within the range of the data.
    # Then it queries the closest date before the provided date.
    # If there is no date query the data with date 0 (which will probably return no data.)
    end_date_query = "SELECT MAX(DATE(date_time)) FROM $(attribute.table_where_is_located)"
    end_date = DBInterface.execute(db.sqlite_db, end_date_query) |> DataFrame
    if isempty(end_date)
        return DateTime(0)
    end
    return DateTime(end_date[!, 1][1])
end

"""
    closest_date_query(db::DatabaseSQLite, attribute::Attribute, dim_value::DateTime)

Query the closest date that is less than or equal to the specified date in a time series attribute table.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `attribute::Attribute`: The time series attribute
  - `dim_value::DateTime`: The target date to search for

# Returns

  - `DateTime`: The closest date on or before `dim_value`, or `DateTime(0)` if no such date exists
"""
function closest_date_query(db::DatabaseSQLite, attribute::Attribute, dim_value::DateTime)
    closest_date_query_earlier = "SELECT DISTINCT date_time FROM $(attribute.table_where_is_located) WHERE $(attribute.id) IS NOT NULL AND DATE(date_time) <= DATE('$(dim_value)') ORDER BY DATE(date_time) DESC LIMIT 1"
    closest_date = DBInterface.execute(db.sqlite_db, closest_date_query_earlier) |> DataFrame
    if isempty(closest_date)
        return DateTime(0)
    end
    return DateTime(closest_date[!, 1][1])
end

"""
    read_scalar_relations(db::DatabaseSQLite, collection_from::String, collection_to::String, relation_type::String)

Read all scalar relation mappings from one collection to another.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_from::String`: The identifier of the source collection
  - `collection_to::String`: The identifier of the target collection
  - `relation_type::String`: The type of relation (e.g., "id", "group", "turbine_to")

# Returns

  - `Vector{String}`: A vector of labels from `collection_to` representing the relation for each element in `collection_from`, ordered by ID. Empty strings (`""`) indicate null relations (no connection).

# Examples

```julia
# Get which resource each plant is connected to
resources = PSRDatabase.read_scalar_relations(db, "Plant", "Resource", "id")
# ["Resource 1", "", ""]  # Plant 1 → Resource 1, Plant 2 and 3 → no resource

# Get turbine connections between plants
turbines = PSRDatabase.read_scalar_relations(db, "Plant", "Plant", "turbine_to")
# ["", "", "Plant 2"]  # Only Plant 3 connects to Plant 2
```

# Throws

  - `DatabaseException` if the relation is not a scalar relation (e.g., trying to read a vector relation)
"""
function read_scalar_relations(
    db::DatabaseSQLite,
    collection_from::String,
    collection_to::String,
    relation_type::String,
)
    map_of_elements = _get_scalar_relation_map(
        db,
        collection_from,
        collection_to,
        relation_type,
    )
    names_in_collection_to = read_scalar_parameters(db, collection_to, "label")
    num_elements = length(names_in_collection_to)
    replace_dict = Dict{Any, String}(zip(collect(1:num_elements), names_in_collection_to))
    push!(replace_dict, _PSRDatabase_null_value(Int) => "")
    return replace(map_of_elements, replace_dict...)
end

"""
    read_scalar_relation(db::DatabaseSQLite, collection_from::String, collection_to::String, relation_type::String, collection_from_label::String)

Read the scalar relation mapping for a specific element from one collection to another.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_from::String`: The identifier of the source collection
  - `collection_to::String`: The identifier of the target collection
  - `relation_type::String`: The type of relation (e.g., "id", "group", "turbine_to")
  - `collection_from_label::String`: The label of the element in the source collection

# Returns

  - `String`: The label from `collection_to` that the specified element relates to. Empty string (`""`) indicates a null relation (no connection).

# Examples

```julia
# Get which resource "Plant 1" is connected to
resource = PSRDatabase.read_scalar_relation(db, "Plant", "Resource", "id", "Plant 1")  # "Resource 1"

# Get which plant "Plant 3" connects to via turbine
turbine = PSRDatabase.read_scalar_relation(db, "Plant", "Plant", "turbine_to", "Plant 3")  # "Plant 2"
```
"""
function read_scalar_relation(
    db::DatabaseSQLite,
    collection_from::String,
    collection_to::String,
    relation_type::String,
    collection_from_label::String,
)
    relations = read_scalar_relations(
        db,
        collection_from,
        collection_to,
        relation_type,
    )
    labels_in_collection_from = read_scalar_parameters(db, collection_from, "label")
    index_of_label = findfirst(isequal(collection_from_label), labels_in_collection_from)
    return relations[index_of_label]
end

"""
    _get_scalar_relation_map(db::DatabaseSQLite, collection_from::String, collection_to::String, relation_type::String)

Internal function to retrieve the scalar relation mapping as a vector of indices.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_from::String`: The identifier of the source collection
  - `collection_to::String`: The identifier of the target collection
  - `relation_type::String`: The type of relation

# Returns

  - `Vector{Int}`: A vector of indices mapping each element in `collection_from` to elements in `collection_to`
"""
function _get_scalar_relation_map(
    db::DatabaseSQLite,
    collection_from::String,
    collection_to::String,
    relation_type::String,
)
    attribute_on_collection_from = lowercase(collection_to) * "_" * relation_type
    _throw_if_attribute_is_not_scalar_relation(
        db,
        collection_from,
        attribute_on_collection_from,
        :read,
    )
    attribute = _get_attribute(db, collection_from, attribute_on_collection_from)

    query = "SELECT $(attribute.id) FROM $(attribute.table_where_is_located)"
    df = DBInterface.execute(db.sqlite_db, query) |> DataFrame
    results = df[!, 1]
    num_results = length(results)
    map_of_indexes = -1 * ones(Int, num_results)
    ids_in_collection_to = read_scalar_parameters(db, collection_to, "id")
    for i in 1:num_results
        if ismissing(results[i])
            map_of_indexes[i] = _PSRDatabase_null_value(Int)
        else
            map_of_indexes[i] = findfirst(isequal(results[i]), ids_in_collection_to)
        end
    end
    return map_of_indexes
end

"""
    read_vector_relations(db::DatabaseSQLite, collection_from::String, collection_to::String, relation_type::String)

Read all vector relation mappings from one collection to another.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_from::String`: The identifier of the source collection
  - `collection_to::String`: The identifier of the target collection
  - `relation_type::String`: The type of relation (e.g., "id", "group")

# Returns

  - `Vector{Vector{String}}`: A vector of vectors, where each inner vector contains labels from `collection_to` representing the relations for one element in `collection_from`, ordered by ID. Empty vectors indicate no relations. Empty strings within vectors indicate null relations.

# Examples

```julia
# Get which costs each plant is associated with
costs = PSRDatabase.read_vector_relations(db, "Plant", "Cost", "id")
# [["Cost 1"], ["Cost 1", "Cost 2"], String[]]
# Plant 1 → Cost 1, Plant 2 → Cost 1 and Cost 2, Plant 3 → no costs

# After updating Plant 1's costs
PSRDatabase.set_vector_relation!(db, "Plant", "Cost", "Plant 1", ["Cost 2"], "id")
costs = PSRDatabase.read_vector_relations(db, "Plant", "Cost", "id")
# [["Cost 2"], ["Cost 1", "Cost 2"], String[]]
```

# Throws

  - `DatabaseException` if the relation is not a vector relation (e.g., trying to read a scalar relation)
"""
function read_vector_relations(
    db::DatabaseSQLite,
    collection_from::String,
    collection_to::String,
    relation_type::String,
)
    map_of_vector_with_indexes = _get_vector_relation_map(
        db,
        collection_from,
        collection_to,
        relation_type,
    )

    names_in_collection_to = read_scalar_parameters(db, collection_to, "label")
    num_elements = length(names_in_collection_to)
    replace_dict = Dict{Any, String}(zip(collect(1:num_elements), names_in_collection_to))
    push!(replace_dict, _PSRDatabase_null_value(Int) => "")

    map_with_labels = Vector{Vector{String}}(undef, length(map_of_vector_with_indexes))

    for (i, vector_with_indexes) in enumerate(map_of_vector_with_indexes)
        map_with_labels[i] = replace(vector_with_indexes, replace_dict...)
    end

    return map_with_labels
end

"""
    read_vector_relation(db::DatabaseSQLite, collection_from::String, collection_to::String, collection_from_label::String, relation_type::String)

Read the vector relation mapping for a specific element from one collection to another.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_from::String`: The identifier of the source collection
  - `collection_to::String`: The identifier of the target collection
  - `collection_from_label::String`: The label of the element in the source collection
  - `relation_type::String`: The type of relation (e.g., "id", "group")

# Returns

  - `Vector{String}`: A vector of labels from `collection_to` that the specified element relates to. Returns an empty vector if no relations exist. Empty strings within the vector indicate null relations.

# Examples

```julia
# Get which costs "Plant 1" is associated with
costs = PSRDatabase.read_vector_relation(db, "Plant", "Cost", "Plant 1", "id")  # ["Cost 2"]

# Get multiple related elements
costs = PSRDatabase.read_vector_relation(db, "Plant", "Cost", "Plant 2", "id")  # ["Cost 1", "Cost 2"]

# Element with no relations
costs = PSRDatabase.read_vector_relation(db, "Plant", "Cost", "Plant 3", "id")  # String[]
```
"""
function read_vector_relation(
    db::DatabaseSQLite,
    collection_from::String,
    collection_to::String,
    collection_from_label::String,
    relation_type::String,
)
    relations = read_vector_relations(
        db,
        collection_from,
        collection_to,
        relation_type,
    )
    labels_in_collection_from = read_scalar_parameters(db, collection_from, "label")
    index_of_label = findfirst(isequal(collection_from_label), labels_in_collection_from)
    return relations[index_of_label]
end

"""
    _get_vector_relation_map(db::DatabaseSQLite, collection_from::String, collection_to::String, relation_type::String)

Internal function to retrieve the vector relation mapping as a vector of index vectors.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_from::String`: The identifier of the source collection
  - `collection_to::String`: The identifier of the target collection
  - `relation_type::String`: The type of relation

# Returns

  - `Vector{Vector{Int}}`: A vector of vectors of indices mapping each element in `collection_from` to elements in `collection_to`
"""
function _get_vector_relation_map(
    db::DatabaseSQLite,
    collection_from::String,
    collection_to::String,
    relation_type::String,
)
    attribute_on_collection_from = lowercase(collection_to) * "_" * relation_type
    _throw_if_attribute_is_not_vector_relation(
        db,
        collection_from,
        attribute_on_collection_from,
        :read,
    )
    attribute = _get_attribute(db, collection_from, attribute_on_collection_from)

    query = "SELECT id, vector_index, $(attribute.id) FROM $(attribute.table_where_is_located) ORDER BY id, vector_index"
    df = DBInterface.execute(db.sqlite_db, query) |> DataFrame
    id = df[!, 1]
    results = df[!, 3]

    ids_in_collection_from = read_scalar_parameters(db, collection_from, "id")
    ids_in_collection_to = read_scalar_parameters(db, collection_to, "id")
    num_ids = length(ids_in_collection_from)
    map_of_vector_with_indexes = Vector{Vector{Int}}(undef, num_ids)
    for i in 1:num_ids
        map_of_vector_with_indexes[i] = Vector{Int}(undef, 0)
    end

    num_rows = size(df, 1)
    for i in 1:num_rows
        index_of_id = findfirst(isequal(id[i]), ids_in_collection_from)
        index_of_id_collection_to = findfirst(isequal(results[i]), ids_in_collection_to)
        if isnothing(index_of_id)
            continue
        end
        if isnothing(index_of_id_collection_to)
            push!(
                map_of_vector_with_indexes[index_of_id],
                _PSRDatabase_null_value(Int),
            )
        else
            push!(map_of_vector_with_indexes[index_of_id], index_of_id_collection_to)
        end
    end

    return map_of_vector_with_indexes
end

"""
    read_time_series_file(db::DatabaseSQLite, collection_id::String, attribute_id::String)::String

Read the file path stored in a time series file attribute.

Time series file attributes store references to external files containing time series data.
This function retrieves the file path string.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_id::String`: The identifier of the collection
  - `attribute_id::String`: The identifier of the time series file attribute

# Returns

  - `String`: The file path stored in the attribute, or an empty string (`""`) if not set

# Examples

```julia
# Read time series file paths
wind_file = PSRDatabase.read_time_series_file(db, "Plant", "wind_speed")  # "some_file.txt"
direction_file = PSRDatabase.read_time_series_file(db, "Plant", "wind_direction")  # "some_file2"

# After updating
PSRDatabase.set_time_series_file!(db, "Plant"; wind_speed = "some_file3.txt")
wind_file = PSRDatabase.read_time_series_file(db, "Plant", "wind_speed")  # "some_file3.txt"
```

# Throws

  - `DatabaseException` if the attribute is not a time series file attribute
  - `DatabaseException` if the table has more than one row (should only have one row for time series file attributes)
"""
function read_time_series_file(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
)::String
    _throw_if_attribute_is_not_time_series_file(
        db,
        collection_id,
        attribute_id,
        :read,
    )
    attribute = _get_attribute(db, collection_id, attribute_id)
    table = attribute.table_where_is_located

    query = "SELECT $(attribute.id) FROM $table"
    df = DBInterface.execute(db.sqlite_db, query) |> DataFrame
    result = df[!, 1]
    if isempty(result)
        return ""
    elseif size(df, 1) > 1
        psr_database_sqlite_error(
            "Table $table has more than one row. As a time series file, it should have only one row.",
        )
    elseif ismissing(result[1])
        return ""
    else
        return result[1]
    end
end

"""
    read_time_series_row(db::DatabaseSQLite, collection_id::String, attribute_id::String; date_time::DateTime)

Read a row of time series data for all elements in a collection at a specific date/time.

This function is optimized for read-only databases and uses caching for efficient access to time series data.

# Arguments

  - `db::DatabaseSQLite`: The database connection (must be read-only)
  - `collection_id::String`: The identifier of the collection
  - `attribute_id::String`: The identifier of the time series attribute
  - `date_time::DateTime`: The date/time to query data for

# Returns

  - `Vector`: A vector containing the time series values for all elements at the specified date/time

# Note

This function only works with read-only databases and will throw an error if called on a writable database.

# Example

```julia
generation = PSRDatabase.read_time_series_row(db, "Thermal", "generation"; date_time = DateTime(2025, 1, 1))
```
"""
function read_time_series_row(
    db,
    collection_id::String,
    attribute_id::String;
    date_time::DateTime,
)
    _throw_if_attribute_is_not_time_series(
        db,
        collection_id,
        attribute_id,
        :read,
    )
    @assert _is_read_only(db) "Time series mapping only works in read only databases"

    collection_attribute = _collection_attribute(collection_id, attribute_id)
    attribute = _get_attribute(db, collection_id, attribute_id)

    T = attribute.type

    if _time_controller_collection_is_empty(db, collection_id)
        return Vector{T}(undef, 0)
    end
    if !haskey(db._time_controller.cache, collection_attribute)
        db._time_controller.cache[collection_attribute] = _start_time_controller_cache(db, attribute, date_time, T)
    end
    cache = db._time_controller.cache[collection_attribute]
    data = query_data_in_time_controller(cache, date_time)
    return data
end

"""
    _read_time_series_table(db::DatabaseSQLite, attribute::Attribute, id::Int)

Internal function to read the complete time series table for a specific element.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `attribute::Attribute`: The time series attribute
  - `id::Int`: The numeric ID of the element

# Returns

  - `DataFrame`: A DataFrame containing all time series data for the element
"""
function _read_time_series_table(
    db::DatabaseSQLite,
    attribute::Attribute,
    id::Int,
)
    query = string("SELECT ", join(attribute.dimension_names, ",", ", "), ", ", attribute.id)
    query *= " FROM $(attribute.table_where_is_located) WHERE id = '$id'"
    return DBInterface.execute(db.sqlite_db, query) |> DataFrame
end

"""
    read_time_series_table(db::DatabaseSQLite, collection_id::String, attribute_id::String, label::String)

Read the complete time series table for a specific element identified by label.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_id::String`: The identifier of the collection
  - `attribute_id::String`: The identifier of the time series attribute
  - `label::String`: The label of the element to read data for

# Returns

  - `DataFrame`: A DataFrame containing all time series data (dimensions and values) for the specified element

# Example

```julia
generation_table = PSRDatabase.read_time_series_table(db, "Thermal", "generation", "Plant1")
```
"""
function read_time_series_table(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    label::String,
)
    _throw_if_attribute_is_not_time_series(
        db,
        collection_id,
        attribute_id,
        :read,
    )
    attribute = _get_attribute(db, collection_id, attribute_id)
    id = _get_id(db, collection_id, label)

    return _read_time_series_table(
        db,
        attribute,
        id,
    )
end

"""
    _treat_query_result(query_results::Vector{Missing}, attribute::Attribute, default::Union{Nothing, Any})

Internal function to process query results that are all missing values, replacing them with appropriate defaults.

# Arguments

  - `query_results::Vector{Missing}`: The query results containing only missing values
  - `attribute::Attribute`: The attribute being queried
  - `default::Union{Nothing, Any}`: The default value to use for missing data

# Returns

  - A vector filled with the appropriate default values
"""
function _treat_query_result(
    query_results::Vector{Missing},
    attribute::Attribute,
    default::Union{Nothing, Any},
)
    type_of_attribute = _type(attribute)
    default = if isnothing(default)
        _PSRDatabase_null_value(type_of_attribute)
    else
        default
    end
    final_results = fill(default, length(query_results))
    return final_results
end

"""
    _treat_query_result(query_results::Vector{Union{Missing, T}}, attribute::Attribute, default::Union{Nothing, Any}) where {T <: Union{Int64, Float64}}

Internal function to process numeric query results, replacing missing values with appropriate defaults.

# Arguments

  - `query_results::Vector{Union{Missing, T}}`: The query results that may contain missing values
  - `attribute::Attribute`: The attribute being queried
  - `default::Union{Nothing, Any}`: The default value to use for missing data

# Returns

  - A vector with missing values replaced by the specified default
"""
function _treat_query_result(
    query_results::Vector{Union{Missing, T}},
    attribute::Attribute,
    default::Union{Nothing, Any},
) where {T <: Union{Int64, Float64}}
    type_of_attribute = _type(attribute)
    default = if isnothing(default)
        _PSRDatabase_null_value(type_of_attribute)
    else
        if isa(default, type_of_attribute)
            default
        else
            psr_database_sqlite_error(
                "default value must be of the same type as attribute \"$(attribute.id)\": $(type_of_attribute). User inputed $(typeof(default)): default.",
            )
        end
    end
    final_results = fill(default, length(query_results))
    for i in eachindex(final_results)
        if !ismissing(query_results[i])
            final_results[i] = query_results[i]
        end
    end
    return final_results
end

"""
    _treat_query_result(query_results::Vector{<:Union{Missing, String}}, attribute::Attribute, default::Union{Nothing, Any})

Internal function to process string query results, replacing missing values with appropriate defaults.
Handles both String and DateTime types (DateTime values are stored as strings in the database).

# Arguments

  - `query_results::Vector{<:Union{Missing, String}}`: The query results that may contain missing values
  - `attribute::Attribute`: The attribute being queried
  - `default::Union{Nothing, Any}`: The default value to use for missing data

# Returns

  - A vector with missing values replaced by the specified default, with DateTime conversion if applicable
"""
function _treat_query_result(
    query_results::Vector{<:Union{Missing, String}},
    attribute::Attribute,
    default::Union{Nothing, Any},
)
    type_of_attribute = _type(attribute)
    default = if isnothing(default)
        _PSRDatabase_null_value(type_of_attribute)
    else
        if isa(default, type_of_attribute)
            default
        else
            psr_database_sqlite_error(
                "default value must be of the same type as attribute \"$(attribute.id)\": $(type_of_attribute). User inputed $(typeof(default)): default.",
            )
        end
    end
    final_results = fill(default, length(query_results))
    for i in eachindex(final_results)
        if !ismissing(query_results[i])
            if isa(default, String)
                final_results[i] = query_results[i]
            else
                final_results[i] = DateTime(query_results[i])
            end
        end
    end
    return final_results
end

"""
    _treat_query_result(results::Vector{T}, ::Attribute, ::Union{Nothing, Any}) where {T <: Union{Int64, Float64}}

Internal function to process numeric query results that contain no missing values.
Returns the results unchanged.

# Arguments

  - `results::Vector{T}`: The query results with no missing values
  - `::Attribute`: The attribute being queried (unused)
  - `::Union{Nothing, Any}`: The default value (unused)

# Returns

  - The original results vector unchanged
"""
_treat_query_result(
    results::Vector{T},
    ::Attribute,
    ::Union{Nothing, Any},
) where {T <: Union{Int64, Float64}} = results

"""
    _PSRDatabase_null_value(::Type{Float64})
    _PSRDatabase_null_value(::Type{Int64})
    _PSRDatabase_null_value(::Type{String})
    _PSRDatabase_null_value(::Type{DateTime})

Get the null/missing value representation for a specific type in PSRDatabase.

# Arguments

  - Type parameter: The data type to get the null value for

# Returns

  - For `Float64`: `NaN`
  - For `Int64`: `typemin(Int64)`
  - For `String`: `""`
  - For `DateTime`: `typemin(DateTime)`
"""
_PSRDatabase_null_value(::Type{Float64}) = NaN
_PSRDatabase_null_value(::Type{Int64}) = typemin(Int64)
_PSRDatabase_null_value(::Type{String}) = ""
_PSRDatabase_null_value(::Type{DateTime}) = typemin(DateTime)

"""
    _is_null_in_db(value::Float64)
    _is_null_in_db(value::Int64)
    _is_null_in_db(value::String)
    _is_null_in_db(value::DateTime)

Check if a value represents a null/missing value in PSRDatabase.

# Arguments

  - `value`: The value to check

# Returns

  - `Bool`: `true` if the value is null, `false` otherwise

# Details

  - For `Float64`: checks if `isnan(value)`
  - For `Int64`: checks if `value == typemin(Int64)`
  - For `String`: checks if `isempty(value)`
  - For `DateTime`: checks if `value == typemin(DateTime)`
"""
function _is_null_in_db(value::Float64)
    return isnan(value)
end
function _is_null_in_db(value::Int64)
    return value == typemin(Int64)
end
function _is_null_in_db(value::String)
    return isempty(value)
end
function _is_null_in_db(value::DateTime)
    return value == typemin(DateTime)
end
