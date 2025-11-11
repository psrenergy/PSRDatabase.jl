"""
    const UPDATE_METHODS_BY_CLASS_OF_ATTRIBUTE

A dictionary mapping attribute classes to their corresponding update method names in PSRDatabase.
"""
const UPDATE_METHODS_BY_CLASS_OF_ATTRIBUTE = Dict(
    ScalarParameter => "update_scalar_parameter!",
    ScalarRelation => "set_scalar_relation!",
    VectorParameter => "update_vector_parameters!",
    VectorRelation => "set_vector_relation!",
    SetParameter => "update_set_parameters!",
    SetRelation => "set_set_relation!",
    TimeSeries => "update_time_series_row!",
    TimeSeriesFile => "set_time_series_file!",
)

"""
    update_parameter!(db::DatabaseSQLite, collection_id::String, label::String; kwargs...)

Update multiple parameter attributes for a specific element in a collection.
The function can update multiple types of parameters

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_id::String`: The identifier of the collection containing the element
  - `label::String`: The label of the element to update
  - `kwargs...`: Named arguments where keys are attribute names and values are the new values for those attributes

# Returns

    - `nothing`

"""
function update_parameter!(
    db::DatabaseSQLite,
    collection_id::String,
    label::String;
    kwargs...
)
    for attribute in keys(kwargs)
        attr = String(attribute)
        # Validate that the attribute is a parameter
        _throw_if_attribute_is_not_parameter(
            db,
            collection_id,
            attr,
            :update,
        )
        value = kwargs[attribute]
        if _is_vector_parameter(db, collection_id, attr)
            update_vector_parameters!(db, collection_id, attr, label, value)
        elseif _is_set_parameter(db, collection_id, attr)
            update_set_parameters!(db, collection_id, attr, label, value)
        elseif _is_scalar_parameter(db, collection_id, attr)
            update_scalar_parameter!(db, collection_id, attr, label, value)
        else
            psr_database_sqlite_error(
                "Attribute $attr in collection $collection_id is not a recognized parameter type.",
            )
        end
    end
    return nothing
end

"""
    update_scalar_parameter!(db::DatabaseSQLite, collection_id::String, attribute_id::String, label::String, val)

Update the value of a scalar parameter attribute for a specific element in a collection.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_id::String`: The identifier of the collection containing the element
  - `attribute_id::String`: The identifier of the scalar parameter attribute to update
  - `label::String`: The label of the element to update
  - `val`: The new value for the attribute (must match the attribute's type: Float64, Int64, String, or DateTime)

# Returns

  - `nothing`

# Throws

  - `DatabaseException` if the collection or attribute doesn't exist
  - `DatabaseException` if the attribute is not a scalar parameter
  - `DatabaseException` if the value type doesn't match the attribute type
  - `DatabaseException` if the element label doesn't exist

# Examples

```julia
# Update a string parameter
PSRDatabase.update_scalar_parameter!(db, "Resource", "type", "Resource 1", "D")

# Update a numeric parameter
PSRDatabase.update_scalar_parameter!(db, "Resource", "some_value_1", "Resource 1", 1.0)

# Update a date parameter
PSRDatabase.update_scalar_parameter!(db, "Configuration", "date_initial", "Toy Case", DateTime(2021, 1, 1))
```
"""
function update_scalar_parameter!(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    label::String,
    val,
)
    _throw_if_collection_or_attribute_do_not_exist(
        db,
        collection_id,
        attribute_id,
    )
    _throw_if_attribute_is_not_scalar_parameter(
        db,
        collection_id,
        attribute_id,
        :update,
    )
    attribute = _get_attribute(db, collection_id, attribute_id)
    _validate_scalar_parameter_type(attribute, label, val)
    id = _get_id(db, collection_id, label)
    _update_scalar_parameter!(db, collection_id, attribute_id, id, val)
    return nothing
end

function _update_scalar_parameter!(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    id::Integer,
    val,
)
    attribute = _get_attribute(db, collection_id, attribute_id)
    table_name = attribute.table_where_is_located
    DBInterface.execute(
        db.sqlite_db,
        "UPDATE $table_name SET $attribute_id = '$val' WHERE id = '$id'",
    )
    return nothing
end

"""
    update_vector_parameters!(db::DatabaseSQLite, collection_id::String, attribute_id::String, label::String, vals::Vector)

Update all values of a vector parameter attribute for a specific element in a collection.

This function replaces all existing values for the vector with the new values provided.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_id::String`: The identifier of the collection containing the element
  - `attribute_id::String`: The identifier of the vector parameter attribute to update
  - `label::String`: The label of the element to update
  - `vals::Vector`: A vector containing the new values (must match the attribute's type)

# Returns

  - `nothing`

# Throws

  - `DatabaseException` if the collection or attribute doesn't exist
  - `DatabaseException` if the attribute is not a vector parameter
  - `DatabaseException` if the value types don't match the attribute type
  - `DatabaseException` if the element label doesn't exist
  - `DatabaseException` if updating vectors in a group and the new length doesn't match other vectors in the group

# Examples

```julia
# Update a vector of numeric values
PSRDatabase.update_vector_parameters!(
    db,
    "Resource",
    "some_value_1",
    "Resource 1",
    [10.0, 20.0, 30.0],
)

# Update to a different number of values (if not constrained by vector group)
PSRDatabase.update_vector_parameters!(
    db,
    "Resource",
    "some_value_1",
    "Resource 1",
    [5.0, 15.0],
)
```
"""
function update_vector_parameters!(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    label::String,
    vals::Vector,
)
    _throw_if_collection_or_attribute_do_not_exist(
        db,
        collection_id,
        attribute_id,
    )
    _throw_if_attribute_is_not_vector_parameter(
        db,
        collection_id,
        attribute_id,
        :update,
    )
    attribute = _get_attribute(db, collection_id, attribute_id)
    _validate_vector_parameter_type(attribute, label, vals)
    id = _get_id(db, collection_id, label)
    return _update_vector_parameters!(db, collection_id, attribute_id, id, vals)
end

function _update_vector_parameters!(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    id::Integer,
    vals::Vector,
)
    attribute = _get_attribute(db, collection_id, attribute_id)
    group_id = attribute.group_id
    table_name = attribute.table_where_is_located
    num_new_elements = length(vals)
    df_num_rows =
        DBInterface.execute(
            db.sqlite_db,
            "SELECT $(attribute_id) FROM $table_name WHERE id = '$id'",
        ) |> DataFrame
    num_rows_in_query = size(df_num_rows, 1)
    if num_rows_in_query != num_new_elements
        if num_rows_in_query == 0
            # If there are no rows in the table we can create them
            _create_vectors!(
                db,
                collection_id,
                id,
                Dict(Symbol(attribute_id) => vals),
            )
        else
            # If there are rows in the table we must check that the number of rows is the same as the number of new relations
            psr_database_sqlite_error(
                "There is currently a vector of $num_rows_in_query elements in the group $group_id. " *
                "User is trying to set a vector of length $num_new_elements. This is invalid. " *
                "If you want to change the number of elements in the group you might have to delete " *
                "the element and create it again with the new vector.",
            )
        end
    else
        # Update the elements
        for (i, val) in enumerate(vals)
            DBInterface.execute(
                db.sqlite_db,
                "UPDATE $table_name SET $attribute_id = '$val' WHERE id = '$id' AND vector_index = '$i'",
            )
        end
    end
    return nothing
end

function update_set_parameters!(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    label::String,
    vals::Vector,
)
    _throw_if_collection_or_attribute_do_not_exist(
        db,
        collection_id,
        attribute_id,
    )
    _throw_if_attribute_is_not_set_parameter(
        db,
        collection_id,
        attribute_id,
        :update,
    )
    attribute = _get_attribute(db, collection_id, attribute_id)
    _validate_set_parameter_type(attribute, label, vals)
    id = _get_id(db, collection_id, label)
    return _update_set_parameters!(db, collection_id, attribute_id, id, vals)
end

function _update_set_parameters!(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    id::Integer,
    vals::Vector,
)
    attribute = _get_attribute(db, collection_id, attribute_id)
    group_id = attribute.group_id
    table_name = attribute.table_where_is_located
    num_new_elements = length(vals)
    df_num_rows =
        DBInterface.execute(
            db.sqlite_db,
            "SELECT $(attribute_id) FROM $table_name WHERE id = '$id'",
        ) |> DataFrame
    num_rows_in_query = size(df_num_rows, 1)
    if num_rows_in_query != num_new_elements
        if num_rows_in_query == 0
            # If there are no rows in the table we can create them
            _create_set_attributes!(
                db,
                collection_id,
                id,
                Dict(Symbol(attribute_id) => vals),
            )
        else
            # If there are rows in the table we must check that the number of rows is the same as the number of new relations
            psr_database_sqlite_error(
                "There is currently a vector of $num_rows_in_query elements in the group $group_id. " *
                "User is trying to set a vector of length $num_new_elements. This is invalid. " *
                "If you want to change the number of elements in the group you might have to delete " *
                "the element and create it again with the new vector.",
            )
        end
    else
        # Query the rowids to update
        df_rowids =
            DBInterface.execute(
                db.sqlite_db,
                "SELECT rowid FROM $table_name WHERE id = '$id'",
            ) |> DataFrame
        # Update the elements
        for (rowid, val) in zip(df_rowids.rowid, vals)
            DBInterface.execute(
                db.sqlite_db,
                "UPDATE $table_name SET $attribute_id = '$val' WHERE id = '$id' AND rowid = '$rowid'",
            )
        end
    end
    return nothing
end

# Helper to guide user to correct method
function set_scalar_relation!(
    db::DatabaseSQLite,
    collection_from::String,
    collection_to::String,
    label_collection_from::String,
    label_collection_to::Vector{String},
    relation_type::String,
)
    psr_database_sqlite_error(
        "Please use the method `set_vector_relation!` to set a vector relation",
    )
    return nothing
end

"""
    set_scalar_relation!(db::DatabaseSQLite, collection_from::String, collection_to::String, label_collection_from::String, label_collection_to::String, relation_type::String)

Set a scalar relation between two elements, linking an element from one collection to an element in another collection (or the same collection).

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_from::String`: The identifier of the collection containing the element to set the relation from
  - `collection_to::String`: The identifier of the collection containing the element to set the relation to
  - `label_collection_from::String`: The label of the element to set the relation from
  - `label_collection_to::String`: The label of the element to set the relation to
  - `relation_type::String`: The type/name of the relation (e.g., "id", "turbine_to", "spill_to")

# Returns

  - `nothing`

# Throws

  - `DatabaseException` if the attribute is not a scalar relation
  - `DatabaseException` if either element label doesn't exist
  - `DatabaseException` if trying to set a relation between the same element (when both collections are the same)
  - `DatabaseException` if the relation type doesn't exist

# Examples

```julia
# Set a relation to a different collection
PSRDatabase.set_scalar_relation!(
    db,
    "Plant",
    "Resource",
    "Plant 1",
    "Resource 1",
    "id",
)

# Set a relation within the same collection
PSRDatabase.set_scalar_relation!(
    db,
    "Plant",
    "Plant",
    "Plant 3",
    "Plant 1",
    "turbine_to",
)

# Update an existing relation
PSRDatabase.set_scalar_relation!(
    db,
    "Plant",
    "Resource",
    "Plant 1",
    "Resource 2",  # Changes from Resource 1 to Resource 2
    "id",
)
```
"""
function set_scalar_relation!(
    db::DatabaseSQLite,
    collection_from::String,
    collection_to::String,
    label_collection_from::String,
    label_collection_to::String,
    relation_type::String,
)
    attribute_id = lowercase(collection_to) * "_" * relation_type
    _throw_if_attribute_is_not_scalar_relation(
        db,
        collection_from,
        attribute_id,
        :update,
    )
    id_collection_from = _get_id(db, collection_from, label_collection_from)
    id_collection_to = _get_id(db, collection_to, label_collection_to)
    set_scalar_relation!(
        db,
        collection_from,
        collection_to,
        id_collection_from,
        id_collection_to,
        relation_type,
    )
    return nothing
end

function set_scalar_relation!(
    db::DatabaseSQLite,
    collection_from::String,
    collection_to::String,
    id_collection_from::Integer,
    id_collection_to::Integer,
    relation_type::String,
)
    if collection_from == collection_to && id_collection_from == id_collection_to
        psr_database_sqlite_error("Cannot set a relation between the same element.")
    end
    attribute_id = lowercase(collection_to) * "_" * relation_type
    attribute = _get_attribute(db, collection_from, attribute_id)
    table_name = _table_where_is_located(attribute)
    DBInterface.execute(
        db.sqlite_db,
        "UPDATE $table_name SET $attribute_id = '$id_collection_to' WHERE id = '$id_collection_from'",
    )
    return nothing
end

"""
    set_vector_relation!(db::DatabaseSQLite, collection_from::String, collection_to::String, label_collection_from::String, labels_collection_to::Vector{String}, relation_type::String)

Set a vector relation between an element and multiple elements, linking an element from one collection to multiple elements in another collection.

This function replaces all existing relations for the vector with the new relations provided.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_from::String`: The identifier of the collection containing the element to set the relation from
  - `collection_to::String`: The identifier of the collection containing the elements to set the relation to
  - `label_collection_from::String`: The label of the element to set the relation from
  - `labels_collection_to::Vector{String}`: A vector of labels of elements to set the relation to
  - `relation_type::String`: The type/name of the relation

# Returns

  - `nothing`

# Throws

  - `DatabaseException` if the attribute is not a vector relation
  - `DatabaseException` if any element label doesn't exist
  - `DatabaseException` if the relation type doesn't exist
  - `DatabaseException` if the number of relations doesn't match other vectors in the same group

# Examples

```julia
# Set a vector relation
PSRDatabase.set_vector_relation!(
    db,
    "Plant",
    "Cost",
    "Plant 1",
    ["Cost 1", "Cost 2"],
    "some_relation_type",
)

# Update vector relation with different elements
PSRDatabase.set_vector_relation!(
    db,
    "Plant",
    "Cost",
    "Plant 1",
    ["Cost 2", "Cost 3", "Cost 4"],  # Now relates to 3 costs instead of 2
    "some_relation_type",
)

# Clear all relations (empty vector)
PSRDatabase.set_vector_relation!(
    db,
    "Plant",
    "Cost",
    "Plant 1",
    String[],
    "some_relation_type",
)
```
"""
function set_vector_relation!(
    db::DatabaseSQLite,
    collection_from::String,
    collection_to::String,
    label_collection_from::String,
    labels_collection_to::Vector{String},
    relation_type::String,
)
    attribute_id = lowercase(collection_to) * "_" * relation_type
    _throw_if_attribute_is_not_vector_relation(
        db,
        collection_from,
        attribute_id,
        :update,
    )
    id_collection_from = _get_id(db, collection_from, label_collection_from)
    ids_collection_to = fill(_PSRDatabase_null_value(Int), length(labels_collection_to))
    for (i, label) in enumerate(labels_collection_to)
        if !_is_null_in_db(label)
            ids_collection_to[i] = _get_id(db, collection_to, label)
        end
    end
    set_vector_relation!(
        db,
        collection_from,
        collection_to,
        id_collection_from,
        ids_collection_to,
        relation_type,
    )
    return nothing
end

function set_vector_relation!(
    db::DatabaseSQLite,
    collection_from::String,
    collection_to::String,
    id_collection_from::Integer,
    ids_collection_to::Vector{<:Integer},
    relation_type::String,
)
    if collection_from == collection_to && id_collection_from in ids_collection_to
        psr_database_sqlite_error("Cannot set a relation between the same element.")
    end
    attribute_id = lowercase(collection_to) * "_" * relation_type
    attribute = _get_attribute(db, collection_from, attribute_id)
    group_id = attribute.group_id
    table_name = attribute.table_where_is_located
    num_new_relations = length(ids_collection_to)
    df_num_rows =
        DBInterface.execute(
            db.sqlite_db,
            "SELECT $(attribute_id) FROM $table_name WHERE id = '$id_collection_from'",
        ) |> DataFrame
    num_rows_in_query = size(df_num_rows, 1)
    if num_rows_in_query != num_new_relations
        if num_rows_in_query == 0
            # If there are no rows in the table we can create them
            _create_vectors!(
                db,
                collection_from,
                id_collection_from,
                Dict(Symbol(attribute_id) => ids_collection_to),
            )
        else
            # If there are rows in the table we must check that the number of rows is the same as the number of new relations
            psr_database_sqlite_error(
                "There is currently a vector of $num_rows_in_query elements in the group $group_id. " *
                "User is trying to set a vector of $num_new_relations relations. This is invalid. " *
                "If you want to change the number of elements in the group you might have to update " *
                "the vectors in the group before setting this relation. Another option is to delete " *
                "the element and create it again with the new vector.",
            )
        end
    else
        # Update the elements
        for (i, id_collection_to) in enumerate(ids_collection_to)
            if !_is_null_in_db(id_collection_to)
                DBInterface.execute(
                    db.sqlite_db,
                    "UPDATE $table_name SET $attribute_id = '$id_collection_to' WHERE id = '$id_collection_from' AND vector_index = '$i'",
                )
            end
        end
    end
    return nothing
end

function set_set_relation!(
    db::DatabaseSQLite,
    collection_from::String,
    collection_to::String,
    label_collection_from::String,
    labels_collection_to::Vector{String},
    relation_type::String,
)
    attribute_id = lowercase(collection_to) * "_" * relation_type
    _throw_if_attribute_is_not_set_relation(
        db,
        collection_from,
        attribute_id,
        :update,
    )
    id_collection_from = _get_id(db, collection_from, label_collection_from)
    ids_collection_to = fill(_PSRDatabase_null_value(Int), length(labels_collection_to))
    for (i, label) in enumerate(labels_collection_to)
        if !_is_null_in_db(label)
            ids_collection_to[i] = _get_id(db, collection_to, label)
        end
    end
    set_set_relation!(
        db,
        collection_from,
        collection_to,
        id_collection_from,
        ids_collection_to,
        relation_type,
    )
    return nothing
end

function set_set_relation!(
    db::DatabaseSQLite,
    collection_from::String,
    collection_to::String,
    id_collection_from::Integer,
    ids_collection_to::Vector{<:Integer},
    relation_type::String,
)
    if collection_from == collection_to && id_collection_from in ids_collection_to
        psr_database_sqlite_error("Cannot set a relation between the same element.")
    end
    attribute_id = lowercase(collection_to) * "_" * relation_type
    attribute = _get_attribute(db, collection_from, attribute_id)
    group_id = attribute.group_id
    table_name = attribute.table_where_is_located
    num_new_relations = length(ids_collection_to)
    df_num_rows =
        DBInterface.execute(
            db.sqlite_db,
            "SELECT $(attribute_id) FROM $table_name WHERE id = '$id_collection_from'",
        ) |> DataFrame
    num_rows_in_query = size(df_num_rows, 1)
    if num_rows_in_query != num_new_relations
        if num_rows_in_query == 0
            # If there are no rows in the table we can create them
            _create_set_attributes!(
                db,
                collection_from,
                id_collection_from,
                Dict(Symbol(attribute_id) => ids_collection_to),
            )
        else
            # If there are rows in the table we must check that the number of rows is the same as the number of new relations
            psr_database_sqlite_error(
                "There is currently a vector of $num_rows_in_query elements in the group $group_id. " *
                "User is trying to set a vector of $num_new_relations relations. This is invalid. " *
                "If you want to change the number of elements in the group you might have to update " *
                "the vectors in the group before setting this relation. Another option is to delete " *
                "the element and create it again with the new vector.",
            )
        end
    else
        # Query the rowids to update
        df_rowids =
            DBInterface.execute(
                db.sqlite_db,
                "SELECT rowid FROM $table_name WHERE id = '$id_collection_from'",
            ) |> DataFrame
        # Update the elements
        for (rowid, id_collection_to) in zip(df_rowids.rowid, ids_collection_to)
            if !_is_null_in_db(id_collection_to)
                DBInterface.execute(
                    db.sqlite_db,
                    "UPDATE $table_name SET $attribute_id = '$id_collection_to' WHERE id = '$id_collection_from' AND rowid = '$rowid'",
                )
            end
        end
    end
    return nothing
end

"""
    set_time_series_file!(db::DatabaseSQLite, collection_id::String; kwargs...)

Set or update time series file paths for a collection.

This function sets the file paths for time series attributes that store their data in external files.
There can only be one set of time series files per collection.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_id::String`: The identifier of the collection to set time series files for
  - `kwargs...`: Named arguments where keys are time series file attribute names and values are file paths (strings)

# Returns

  - `nothing`

# Throws

  - `DatabaseException` if the collection doesn't exist
  - `DatabaseException` if any attribute is not a time series file attribute
  - `DatabaseException` if any value is not a string
  - `DatabaseException` if there are multiple time series file entries (database corruption)

# Examples

```julia
# Set time series files for a collection
PSRDatabase.set_time_series_file!(
    db,
    "Plant";
    generation = "generation_data.csv",
    cost = "cost_data.csv",
)

# Update a single time series file
PSRDatabase.set_time_series_file!(
    db,
    "Plant";
    generation = "new_generation_data.csv",
)

# Set multiple time series files at once
PSRDatabase.set_time_series_file!(
    db,
    "Resource";
    availability = "availability.txt",
    price = "price.txt",
)
```
"""
function set_time_series_file!(
    db::DatabaseSQLite,
    collection_id::String;
    kwargs...,
)
    _throw_if_collection_does_not_exist(db, collection_id)
    table_name = collection_id * "_time_series_files"
    dict_time_series = Dict()
    for (key, value) in kwargs
        if !isa(value, AbstractString)
            psr_database_sqlite_error(
                "As a time_series_file the value of the attribute $key must be a String. User inputed $(typeof(value)): $value.",
            )
        end
        _throw_if_attribute_is_not_time_series_file(
            db,
            collection_id,
            string(key),
            :update,
        )
        _validate_time_series_attribute_value(value)
        dict_time_series[key] = value
    end
    # Count the number of elements in the time series
    df_count =
        DBInterface.execute(
            db.sqlite_db,
            "SELECT COUNT(*) FROM $table_name",
        ) |> DataFrame
    num_elements = df_count[1, 1]
    if num_elements == 0
        cols = join(keys(dict_time_series), ", ")
        vals = join(values(dict_time_series), "', '")
        DBInterface.execute(
            db.sqlite_db,
            "INSERT INTO $table_name ($cols) VALUES ('$vals')",
        )
    elseif num_elements == 1
        cols_vals = join(
            [string(key, " = '", value, "'") for (key, value) in dict_time_series],
            ", ",
        )
        DBInterface.execute(
            db.sqlite_db,
            """
            WITH TimeSeriesUpdate AS
            (
                SELECT * FROM $table_name
            )
            UPDATE $table_name
                SET $cols_vals
            """,
        )
    else
        psr_database_sqlite_error(
            "There are currently $num_elements time series files in the collection $collection_id. " *
            "This is invalid, there should be only one entry in this table.",
        )
    end
    return nothing
end

function _dimension_value_exists(
    db::DatabaseSQLite,
    attribute::Attribute,
    id::Integer,
    dimensions...,
)
    query = "SELECT $(attribute.id) FROM $(attribute.table_where_is_located) WHERE id = $id AND "
    for (i, (key, value)) in enumerate(dimensions)
        if key == "date_time"
            query *= "$(key) = DATE('$(value)')"
        else
            query *= "$(key) = '$(value)'"
        end
        if i < length(dimensions)
            query *= " AND "
        end
    end
    results = DBInterface.execute(db.sqlite_db, query) |> DataFrame
    if isempty(results)
        return false
    end
    return true
end

function _update_time_series_row!(
    db::DatabaseSQLite,
    attribute::Attribute,
    id::Integer,
    val,
    dimensions,
)
    query = "UPDATE $(attribute.table_where_is_located) SET $(attribute.id) = '$val'"
    query *= " WHERE id = '$id' AND "
    for (i, (key, value)) in enumerate(dimensions)
        if key == "date_time"
            query *= "$(key) = DATE('$(value)')"
        else
            query *= "$(key) = '$(value)'"
        end
        if i < length(dimensions)
            query *= " AND "
        end
    end
    DBInterface.execute(db.sqlite_db, query)
    return nothing
end

"""
    update_time_series_row!(db::DatabaseSQLite, collection_id::String, attribute_id::String, label::String, val; dimensions...)

Update an existing value in a time series attribute for a specific element and dimension combination.

Unlike `add_time_series_row!`, this function only updates existing rows and will throw an error if the
specified dimension combination doesn't exist.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_id::String`: The identifier of the collection containing the element
  - `attribute_id::String`: The identifier of the time series attribute
  - `label::String`: The label of the element to update the time series value for
  - `val`: The new value for the time series at the specified dimensions
  - `dimensions...`: Named arguments specifying the dimension values that identify the row to update

# Returns

  - `nothing`

# Throws

  - `DatabaseException` if the attribute is not a time series
  - `DatabaseException` if the number of dimensions doesn't match the attribute definition
  - `DatabaseException` if dimension names don't match the attribute definition
  - `DatabaseException` if the specified dimension combination doesn't exist

# Examples

```julia
# Update an existing time series value
PSRDatabase.update_time_series_row!(
    db,
    "Plant",
    "generation",
    "Plant 1",
    150.0;
    date_time = DateTime(2020, 1, 1),
)

# Update with multiple dimensions
PSRDatabase.update_time_series_row!(
    db,
    "Plant",
    "cost",
    "Plant 1",
    75.0;
    date_time = DateTime(2020, 1, 1),
    stage = 1,
)
```
"""
function update_time_series_row!(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    label::String,
    val;
    dimensions...,
)
    _throw_if_attribute_is_not_time_series(
        db,
        collection_id,
        attribute_id,
        :update,
    )
    attribute = _get_attribute(db, collection_id, attribute_id)
    id = _get_id(db, collection_id, label)
    _validate_time_series_dimensions(collection_id, attribute, dimensions)

    if !_dimension_value_exists(db, attribute, id, dimensions...)
        psr_database_sqlite_error(
            "The chosen values for dimensions $(join(keys(dimensions), ", ")) do not exist in the time series for element $(label) in collection $(collection_id).",
        )
    end

    if length(dimensions) != length(attribute.dimension_names)
        psr_database_sqlite_error(
            "The number of dimensions in the time series does not match the number of dimensions in the attribute. " *
            "The attribute has $(attribute.num_dimensions) dimensions: $(join(attribute.dimension_names, ", ")).",
        )
    end

    return _update_time_series_row!(db, attribute, id, val, dimensions)
end
