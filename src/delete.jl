"""
    delete_element!(db::DatabaseSQLite, collection_id::String, label::String)

Delete an element from a collection by its label.

This function removes an element and all its associated data from the database. Due to CASCADE DELETE
foreign key constraints, any references to this element from other collections will also be deleted.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_id::String`: The identifier of the collection containing the element
  - `label::String`: The label of the element to delete

# Returns

  - `nothing`

# Throws

  - `DatabaseException` if the collection doesn't exist
  - `DatabaseException` if the element label doesn't exist
  - `SQLiteException` if the deletion violates database constraints (e.g., foreign key without cascade)

# Examples

```julia
# Delete an element by label
PSRDatabase.delete_element!(db, "Plant", "Plant 3")

# After deletion, the same label can be reused
PSRDatabase.create_element!(db, "Plant"; label = "Plant 3", capacity = 100.0)

# Deleting an element that has relations will also delete those relations
PSRDatabase.delete_element!(db, "Resource", "Resource 1")  # Also removes Plant->Resource relations
```

# See Also

  - `delete_element!(db, collection_id, id)`: Delete by numeric ID instead of label
"""
function delete_element!(
    db::DatabaseSQLite,
    collection_id::String,
    label::String,
)
    _throw_if_collection_does_not_exist(db, collection_id)
    id = _get_id(db, collection_id, label)
    delete_element!(db, collection_id, id)
    return nothing
end

"""
    delete_element!(db::DatabaseSQLite, collection_id::String, id::Integer)

Delete an element from a collection by its numeric ID.

This function removes an element and all its associated data from the database. Due to CASCADE DELETE
foreign key constraints, any references to this element from other collections will also be deleted.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_id::String`: The identifier of the collection containing the element
  - `id::Integer`: The numeric ID of the element to delete

# Returns

  - `nothing`

# Examples

```julia
# Delete an element by ID
PSRDatabase.delete_element!(db, "Plant", 3)

# Typically used internally after looking up an ID from a label
id = PSRDatabase._get_id(db, "Plant", "Plant 1")
PSRDatabase.delete_element!(db, "Plant", id)
```

# See Also

  - `delete_element!(db, collection_id, label)`: Delete by label instead of numeric ID
"""
function delete_element!(
    db::DatabaseSQLite,
    collection_id::String,
    id::Integer,
)
    # This assumes that we have on cascade delete for every reference 
    DBInterface.execute(
        db.sqlite_db,
        "DELETE FROM $collection_id WHERE id = '$id'",
    )
    return nothing
end

function _delete_time_series!(
    db::DatabaseSQLite,
    collection_id::String,
    group_id::String,
    id::Integer,
)
    time_series_table_name = "$(collection_id)_time_series_$(group_id)"

    DBInterface.execute(
        db.sqlite_db,
        "DELETE FROM $(time_series_table_name) WHERE id = '$id'",
    )
    return nothing
end

"""
    delete_time_series!(db::DatabaseSQLite, collection_id::String, group_id::String, label::String)

Delete all time series data for a specific element in a time series group.

This function removes all rows from the time series table for a given element, effectively deleting
all time series values across all dimensions for that element and group.

# Arguments

  - `db::DatabaseSQLite`: The database connection
  - `collection_id::String`: The identifier of the collection containing the element
  - `group_id::String`: The identifier of the time series group
  - `label::String`: The label of the element to delete time series data for

# Returns

  - `nothing`

# Throws

  - `DatabaseException` if the collection doesn't exist
  - `DatabaseException` if the element label doesn't exist

# Examples

```julia
# Delete all time series data for an element in a specific group
PSRDatabase.delete_time_series!(db, "Plant", "generation_group", "Plant 1")

# After deletion, you can add new time series data for the same element
PSRDatabase.add_time_series_row!(
    db,
    "Plant",
    "generation",
    "Plant 1",
    100.0;
    date_time = DateTime(2021, 1, 1),
)
```

# Notes

This function only deletes time series data, not the element itself. To delete the entire element,
use `delete_element!` instead.
"""
function delete_time_series!(
    db::DatabaseSQLite,
    collection_id::String,
    group_id::String,
    label::String,
)
    _throw_if_collection_does_not_exist(db, collection_id)
    id = _get_id(db, collection_id, label)

    _delete_time_series!(db, collection_id, group_id, id)

    return nothing
end
