function _create_scalar_attributes!(
    db::DatabaseSQLite,
    collection_id::String,
    scalar_attributes,
)
    attributes = string.(keys(scalar_attributes))
    _throw_if_collection_or_attribute_do_not_exist(db, collection_id, attributes)

    _replace_scalar_relation_labels_with_id!(db, collection_id, scalar_attributes)

    table = _get_collection_scalar_attribute_tables(db.sqlite_db, collection_id)

    if isempty(scalar_attributes)
        DBInterface.execute(db.sqlite_db, "INSERT INTO $table DEFAULT VALUES")
    else
        cols = join(keys(scalar_attributes), ", ")
        vals = join(values(scalar_attributes), "', '")

        DBInterface.execute(db.sqlite_db, "INSERT INTO $table ($cols) VALUES ('$vals')")
    end
    return nothing
end

function _insert_vectors_from_df(
    db::DatabaseSQLite,
    df::DataFrame,
    table_name::String,
)
    # Code to insert rows without using a transaction
    cols = join(string.(names(df)), ", ")
    num_cols = size(df, 2)
    for row in eachrow(df)
        query = "INSERT INTO $table_name ($cols) VALUES ("
        for (i, value) in enumerate(row)
            if ismissing(value) || _is_null_in_db(value)
                query *= "NULL, "
            else
                query *= "\'$value\', "
            end
            if i == num_cols
                query = query[1:end-2]
                query *= ")"
            end
        end
        DBInterface.execute(db.sqlite_db, query)
    end
    return nothing
end

function _create_vector_group!(
    db::DatabaseSQLite,
    collection_id::String,
    group::String,
    id::Integer,
    vector_attributes::Vector{String},
    group_vector_attributes,
)
    vectors_group_table_name = _vectors_group_table_name(collection_id, group)
    _throw_if_collection_or_attribute_do_not_exist(
        db,
        collection_id,
        vector_attributes,
    )
    _replace_vector_relation_labels_with_ids!(
        db,
        collection_id,
        group_vector_attributes,
    )
    _all_vector_of_group_must_have_same_size!(
        group_vector_attributes,
        vector_attributes,
        vectors_group_table_name,
    )
    df = DataFrame(group_vector_attributes)
    num_values = size(df, 1)
    ids = fill(id, num_values)
    vector_index = collect(1:num_values)
    DataFrames.insertcols!(df, 1, :vector_index => vector_index)
    DataFrames.insertcols!(df, 1, :id => ids)
    _insert_vectors_from_df(db, df, vectors_group_table_name)
    return nothing
end

function _create_set_group!(
    db::DatabaseSQLite,
    collection_id::String,
    group::String,
    id::Integer,
    set_attributes::Vector{String},
    group_set_attributes,
)
    set_group_table_name = _set_group_table_name(collection_id, group)
    _throw_if_collection_or_attribute_do_not_exist(
        db,
        collection_id,
        set_attributes,
    )
    _replace_set_relation_labels_with_ids!(
        db,
        collection_id,
        group_set_attributes,
    )
    # No need to check sizes as each set attribute is independent
    df = DataFrame(group_set_attributes)
    num_values = size(df, 1)
    ids = fill(id, num_values)
    DataFrames.insertcols!(df, 1, :id => ids)
    _insert_vectors_from_df(db, df, set_group_table_name)
    return nothing
end

function _create_vectors!(
    db::DatabaseSQLite,
    collection_id::String,
    id::Integer,
    dict_vector_attributes,
)
    # separate vectors by groups
    map_of_groups_to_vector_attributes =
        _map_of_groups_to_vector_attributes(db, collection_id)
    for (group, vector_attributes) in map_of_groups_to_vector_attributes
        group_vector_attributes = Dict()
        for vector_attribute in Symbol.(vector_attributes)
            if haskey(dict_vector_attributes, vector_attribute)
                group_vector_attributes[vector_attribute] =
                    dict_vector_attributes[vector_attribute]
            end
        end
        if isempty(group_vector_attributes)
            continue
        end
        _create_vector_group!(
            db,
            collection_id,
            group,
            id,
            vector_attributes,
            group_vector_attributes,
        )
    end
    return nothing
end

function _create_set_attributes!(
    db::DatabaseSQLite,
    collection_id::String,
    id::Integer,
    dict_set_attributes,
)
    # separate sets by groups
    map_of_groups_to_set_attributes =
        _map_of_groups_to_set_attributes(db, collection_id)
    for (group, set_attributes) in map_of_groups_to_set_attributes
        group_set_attributes = Dict()
        for set_attribute in Symbol.(set_attributes)
            if haskey(dict_set_attributes, set_attribute)
                group_set_attributes[set_attribute] =
                    dict_set_attributes[set_attribute]
            end
        end
        if isempty(group_set_attributes)
            continue
        end
        _create_set_group!(
            db,
            collection_id,
            group,
            id,
            set_attributes,
            group_set_attributes,
        )
    end
    return nothing
end

function _create_time_series!(
    db::DatabaseSQLite,
    collection_id::String,
    id::Integer,
    dict_time_series_attributes,
)
    for (group, df) in dict_time_series_attributes
        time_series_group_table_name = _time_series_group_table_name(collection_id, string(group))
        ids = fill(id, nrow(df))
        DataFrames.insertcols!(df, 1, :id => ids)
        # Convert datetime column to string
        df[!, :date_time] = string.(df[!, :date_time])
        # Add missing columns
        missing_names_in_df = setdiff(_attributes_in_time_series_group(db, collection_id, string(group)), string.(names(df)))
        for missing_attribute in missing_names_in_df
            df[!, Symbol(missing_attribute)] = fill(missing, nrow(df))
        end
        _insert_vectors_from_df(db, df, time_series_group_table_name)
    end
end

function _create_element!(
    db::DatabaseSQLite,
    collection_id::String;
    kwargs...,
)
    _throw_if_collection_does_not_exist(db, collection_id)
    dict_scalar_attributes = Dict{Symbol, Any}()
    dict_vector_attributes = Dict{Symbol, Any}()
    dict_set_attributes = Dict{Symbol, Any}()
    dict_time_series_attributes = Dict{Symbol, Any}()

    # Validate that the arguments will be valid
    for (key, value) in kwargs
        if isa(value, AbstractVector)
            # Check if it is a vector or a set
            if _is_set_attribute(db, collection_id, string(key))
                if isempty(value)
                    psr_database_sqlite_error(
                        "Cannot create the set attribute \"$key\" with an empty vector.",
                    )
                end
                dict_set_attributes[key] = value
            elseif _is_vector_attribute(db, collection_id, string(key))
                if isempty(value)
                    psr_database_sqlite_error(
                        "Cannot create the attribute \"$key\" with an empty vector.",
                    )
                end
                dict_vector_attributes[key] = value
            end
        elseif isa(value, DataFrame)
            _throw_if_not_time_series_group(db, collection_id, string(key))
            _throw_if_data_does_not_match_group(db, collection_id, string(key), value)
            if isempty(value)
                psr_database_sqlite_error(
                    "Cannot create the time series group \"$key\" with an empty DataFrame.",
                )
            end
            dict_time_series_attributes[key] = value
        else
            _throw_if_is_time_series_file(db, collection_id, string(key))
            _throw_if_not_scalar_attribute(db, collection_id, string(key))
            dict_scalar_attributes[key] = value
        end
    end

    _validate_attribute_types_on_creation!(
        db,
        collection_id,
        dict_scalar_attributes,
        dict_vector_attributes,
        dict_set_attributes,
    )

    _create_scalar_attributes!(db, collection_id, dict_scalar_attributes)

    if !isempty(dict_vector_attributes)
        id = get(
            dict_scalar_attributes,
            :id,
            _get_id(db, collection_id, dict_scalar_attributes[:label]),
        )
        _create_vectors!(db, collection_id, id, dict_vector_attributes)
    end

    if !isempty(dict_set_attributes)
        id = get(
            dict_scalar_attributes,
            :id,
            _get_id(db, collection_id, dict_scalar_attributes[:label]),
        )
        _create_set_attributes!(db, collection_id, id, dict_set_attributes)
    end

    if !isempty(dict_time_series_attributes)
        id = get(
            dict_scalar_attributes,
            :id,
            _get_id(db, collection_id, dict_scalar_attributes[:label]),
        )
        _create_time_series!(db, collection_id, id, dict_time_series_attributes)
    end

    return nothing
end

"""
    create_element!(db::DatabaseSQLite, collection_id::String; kwargs...)

Create a new element in the specified collection with the given attributes.

# Arguments

  - `db::DatabaseSQLite`: The database connection

  - `collection_id::String`: The identifier of the collection to add the element to
  - `kwargs...`: Named arguments specifying the attribute values for the new element

      + Scalar parameters: Single values (Int, Float64, String, DateTime)
      + Vector parameters: Arrays of values that must all have the same length within a group
      + Scalar relations: String labels of related elements or integer IDs
      + Vector relations: Arrays of string labels of related elements
      + Time series: File paths as strings

# Returns

  - `nothing`

# Throws

  - `DatabaseException` if the collection doesn't exist
  - `DatabaseException` if an attribute doesn't exist or has invalid type
  - `DatabaseException` if vector parameters in the same group have different lengths
  - `DatabaseException` if a required attribute is missing (e.g., label)
  - `SQLiteException` if a label already exists (violates unique constraint)

# Examples

```julia
# Create element with scalar parameters
PSRDatabase.create_element!(db, "Configuration"; label = "Toy Case", value1 = 1.0)

# Create element with vector parameters
PSRDatabase.create_element!(
    db,
    "Resource";
    label = "Resource 1",
    type = "E",
    some_value = [1.0, 2.0, 3.0],
)

# Create element with scalar relation (using label)
PSRDatabase.create_element!(db, "Plant"; label = "Plant 1", capacity = 50.0, resource_id = "Resource 1")

# Create element with vector relations
PSRDatabase.create_element!(
    db,
    "Process";
    label = "Sugar Mill",
    product_input = ["Sugarcane"],
    factor_input = [1.0],
    product_output = ["Sugar", "Molasse", "Bagasse"],
    factor_output = [0.3, 0.3, 0.4],
)
```
"""
function create_element!(
    db::DatabaseSQLite,
    collection_id::String;
    kwargs...,
)
    try
        _create_element!(db, collection_id; kwargs...)
    catch e
        @error """
               Error creating element in collection \"$collection_id\"
               """
        rethrow(e)
    end
    return nothing
end

function _all_vector_of_group_must_have_same_size!(
    group_vector_attributes,
    vector_attributes::Vector{String},
    table_name::String,
)
    vector_attributes = Symbol.(vector_attributes)
    if isempty(group_vector_attributes)
        return nothing
    end
    dict_of_lengths = Dict{String, Int}()
    for (k, v) in group_vector_attributes
        dict_of_lengths[string(k)] = length(v)
    end
    unique_lengths = unique(values(dict_of_lengths))
    if length(unique_lengths) > 1
        psr_database_sqlite_error(
            "All vectors of table \"$table_name\" must have the same length. These are the current lengths: $(_show_sizes_of_vectors_in_string(dict_of_lengths)) ",
        )
    end
    length_first_vector = unique_lengths[1]
    # fill missing vectors with missing values
    for vector_attribute in vector_attributes
        if !haskey(group_vector_attributes, vector_attribute)
            group_vector_attributes[vector_attribute] = fill(missing, length_first_vector)
        end
    end
    return nothing
end

function _show_sizes_of_vectors_in_string(dict_of_lengths::Dict{String, Int})
    string_sizes = ""
    for (k, v) in dict_of_lengths
        string_sizes *= "\n - $k: $v"
    end
    return string_sizes
end

function _get_label_or_id(
    collection_id::String,
    dict_scalar_attributes,
)
    if collection_id == "Configuration"
        return 1
    elseif haskey(dict_scalar_attributes, :label)
        return dict_scalar_attributes[:label]
    elseif haskey(dict_scalar_attributes, :id)
        return dict_scalar_attributes[:id]
    else
        psr_database_sqlite_error(
            "No label or id was provided for collection $collection_id.",
        )
    end
end

function _replace_scalar_relation_labels_with_id!(
    db::DatabaseSQLite,
    collection_id::String,
    scalar_attributes,
)
    for (key, value) in scalar_attributes
        if _is_scalar_relation(db, collection_id, string(key)) &&
           isa(value, String)
            scalar_relation = _get_attribute(db, collection_id, string(key))
            collection_to = scalar_relation.relation_collection
            scalar_attributes[key] = _get_id(db, collection_to, value)
        end
    end
    return nothing
end

function _replace_vector_relation_labels_with_ids!(
    db::DatabaseSQLite,
    collection_id::String,
    vector_attributes,
)
    for (key, value) in vector_attributes
        if _is_vector_relation(db, collection_id, string(key)) &&
           isa(value, Vector{String})
            vector_relation = _get_attribute(db, collection_id, string(key))
            collection_to = vector_relation.relation_collection
            vec_of_ids = fill(_PSRDatabase_null_value(Int), length(value))
            for (i, v) in enumerate(value)
                if !_is_null_in_db(v)
                    vec_of_ids[i] = _get_id(db, collection_to, v)
                end
            end
            vector_attributes[key] = vec_of_ids
        end
    end
    return nothing
end

function _replace_set_relation_labels_with_ids!(
    db::DatabaseSQLite,
    collection_id::String,
    set_attributes,
)
    for (key, value) in set_attributes
        if _is_set_relation(db, collection_id, string(key)) &&
           isa(value, Vector{String})
            set_relation = _get_attribute(db, collection_id, string(key))
            collection_to = set_relation.relation_collection
            vec_of_ids = fill(_PSRDatabase_null_value(Int), length(value))
            for (i, v) in enumerate(value)
                if !_is_null_in_db(v)
                    vec_of_ids[i] = _get_id(db, collection_to, v)
                end
            end
            set_attributes[key] = vec_of_ids
        end
    end
    return nothing
end

function _validate_attribute_types_on_creation!(
    db::DatabaseSQLite,
    collection_id::String,
    dict_scalar_attributes::AbstractDict,
    dict_vector_attributes::AbstractDict,
    dict_set_attributes::AbstractDict,
)
    label_or_id = _get_label_or_id(collection_id, dict_scalar_attributes)
    _validate_attribute_types!(
        db,
        collection_id,
        label_or_id,
        dict_scalar_attributes,
        dict_vector_attributes,
        dict_set_attributes,
    )
    return nothing
end

function _add_time_series_row!(
    db::DatabaseSQLite,
    attribute::Attribute,
    id::Integer,
    val,
    dimensions,
)
    # Adding a time series element column by column as it is implemented on this function 
    # is not the most efficient way to do it. In any case if the user wants to add a time
    # series column by column, this function can only be implemented as an upsert statements
    # for each column. This is because the user can add a value in a primary key that already
    # exists in the time series. In that case the column should be updated instead of inserted.
    dimensions_string = join(keys(dimensions), ", ")
    values_string = "$id, "
    for dim in dimensions
        values_string *= "'$(dim[2])', "
    end
    values_string *= "'$val'"
    query = """ 
        INSERT INTO $(attribute.table_where_is_located) (id, $dimensions_string, $(attribute.id)) 
        VALUES ($values_string)
        ON CONFLICT(id, $dimensions_string) DO UPDATE SET $(attribute.id) = '$val'
    """
    DBInterface.execute(db.sqlite_db, query)
    return nothing
end

"""
    add_time_series_row!(db::DatabaseSQLite, collection_id::String, attribute_id::String, label::String, val; dimensions...)

Add or update a value in a time series attribute for a specific element and dimension combination.

This function performs an "upsert" operation - if a row with the specified dimensions already exists,
it updates the value; otherwise, it inserts a new row.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_id::String`: The identifier of the collection containing the element
  - `attribute_id::String`: The identifier of the time series attribute
  - `label::String`: The label of the element to add/update the time series value for
  - `val`: The value to set for the time series at the specified dimensions
  - `dimensions...`: Named arguments specifying the dimension values (e.g., `date_time = DateTime(2020, 1, 1)`, `stage = 1`)

# Returns

  - `nothing`

# Throws

  - `DatabaseException` if the attribute is not a time series
  - `DatabaseException` if the number of dimensions doesn't match the attribute definition
  - `DatabaseException` if dimension names don't match the attribute definition

# Examples

```julia
# Add time series value with date_time dimension
PSRDatabase.add_time_series_row!(
    db,
    "Plant",
    "generation",
    "Plant 1",
    100.5;
    date_time = DateTime(2020, 1, 1),
)

# Add time series value with multiple dimensions
PSRDatabase.add_time_series_row!(
    db,
    "Plant",
    "cost",
    "Plant 1",
    50.0;
    date_time = DateTime(2020, 1, 1),
    stage = 1,
)

# Update existing time series value (same dimensions)
PSRDatabase.add_time_series_row!(
    db,
    "Plant",
    "generation",
    "Plant 1",
    120.0;
    date_time = DateTime(2020, 1, 1),  # This will update the existing value
)
```
"""
function add_time_series_row!(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    label::String,
    val;
    dimensions...,
)
    if !_is_time_series(db, collection_id, attribute_id)
        psr_database_sqlite_error(
            "The attribute $attribute_id is not a time series.",
        )
    end
    attribute = _get_attribute(db, collection_id, attribute_id)
    id = _get_id(db, collection_id, label)
    _validate_time_series_dimensions(collection_id, attribute, dimensions)

    if length(dimensions) != length(attribute.dimension_names)
        psr_database_sqlite_error(
            "The number of dimensions in the time series does not match the number of dimensions in the attribute. " *
            "The attribute has $(attribute.num_dimensions) dimensions: $(join(attribute.dimension_names, ", ")).",
        )
    end

    return _add_time_series_row!(db, attribute, id, val, dimensions)
end
