"""
    compare_scalar_parameters(db1::DatabaseSQLite, db2::DatabaseSQLite, collection_id::String)

Compare scalar parameters between two databases for a specific collection.

This function iterates through all scalar parameters (excluding the id field) in the
specified collection and compares their values element-by-element between the two databases.
It checks for differences in the number of elements, null values,
and actual value mismatches.

# Arguments

  - `db1::DatabaseSQLite`: The first database to compare (used as the reference for reading collection structure)
  - `db2::DatabaseSQLite`: The second database to compare against the first
  - `collection_id::String`: The name of the collection (table) to compare scalar parameters for

# Returns

A vector of strings describing differences found in scalar parameters. Each string includes the
collection name, attribute name, element index, and the differing values. Returns an empty vector
if all scalar parameters are identical.
"""
function compare_scalar_parameters(
    db1::DatabaseSQLite,
    db2::DatabaseSQLite,
    collection_id::String,
)
    differences = String[]
    collection = _get_collection(db1, collection_id)

    for (attr_id, attr) in collection.scalar_parameters
        if attr_id == "id"
            continue
        end

        values1 = read_scalar_parameters(db1, collection_id, attr_id)
        values2 = read_scalar_parameters(db2, collection_id, attr_id)

        if length(values1) != length(values2)
            push!(
                differences,
                "Collection '$collection_id', attribute '$attr_id': different number of values (db1: $(length(values1)), db2: $(length(values2)))",
            )
            continue
        end

        for (idx, (v1, v2)) in enumerate(zip(values1, values2))
            if _is_null_in_db(v1) || _is_null_in_db(v2)
                if _is_null_in_db(v1) != _is_null_in_db(v2)
                    push!(
                        differences,
                        "Collection '$collection_id', attribute '$attr_id', element $idx: null mismatch (db1: $(v1), db2: $(v2))",
                    )
                end
            else
                if v1 != v2
                    push!(
                        differences,
                        "Collection '$collection_id', attribute '$attr_id', element $idx: values differ (db1: $(v1), db2: $(v2))",
                    )
                end
            end
        end
    end

    return differences
end

"""
    compare_vector_parameters(db1::DatabaseSQLite, db2::DatabaseSQLite, collection_id::String)

Compare vector parameters between two databases for a specific collection.

This function iterates through all vector parameters in the specified collection and compares
their values element-by-element between the two databases. For each vector attribute, it checks:
- The number of elements in the collection
- The length of each vector
- Individual values within each vector

# Arguments

  - `db1::DatabaseSQLite`: The first database to compare (used as the reference for reading collection structure)
  - `db2::DatabaseSQLite`: The second database to compare against the first
  - `collection_id::String`: The name of the collection (table) to compare vector parameters for

# Returns

A vector of strings describing differences found in vector parameters. Each string includes the
collection name, vector attribute name, element index, vector index, and the differing values.
Returns an empty vector if all vector parameters are identical.
"""
function compare_vector_parameters(
    db1::DatabaseSQLite,
    db2::DatabaseSQLite,
    collection_id::String,
)
    differences = String[]
    collection = _get_collection(db1, collection_id)

    for (attr_id, attr) in collection.vector_parameters
        vectors1 = read_vector_parameters(db1, collection_id, attr_id)
        vectors2 = read_vector_parameters(db2, collection_id, attr_id)

        if length(vectors1) != length(vectors2)
            push!(
                differences,
                "Collection '$collection_id', vector attribute '$attr_id': different number of elements (db1: $(length(vectors1)), db2: $(length(vectors2)))",
            )
            continue
        end

        for (elem_idx, (vec1, vec2)) in enumerate(zip(vectors1, vectors2))
            if length(vec1) != length(vec2)
                push!(
                    differences,
                    "Collection '$collection_id', vector attribute '$attr_id', element $elem_idx: different vector lengths (db1: $(length(vec1)), db2: $(length(vec2)))",
                )
                continue
            end

            for (vec_idx, (v1, v2)) in enumerate(zip(vec1, vec2))
                if _is_null_in_db(v1) || _is_null_in_db(v2)
                    if _is_null_in_db(v1) != _is_null_in_db(v2)
                        push!(
                            differences,
                            "Collection '$collection_id', vector attribute '$attr_id', element $elem_idx, index $vec_idx: null mismatch (db1: $(v1), db2: $(v2))",
                        )
                    end
                else
                    if v1 != v2
                        push!(
                            differences,
                            "Collection '$collection_id', vector attribute '$attr_id', element $elem_idx, index $vec_idx: values differ (db1: $(v1), db2: $(v2))",
                        )
                    end
                end
            end
        end
    end

    return differences
end

"""
    compare_scalar_relations(db1::DatabaseSQLite, db2::DatabaseSQLite, collection_id::String)

Compare scalar relations between two databases for a specific collection.

This function iterates through all scalar relations (foreign key references to other collections)
in the specified collection and compares them element-by-element between the two databases. It
verifies that each element points to the same related element in both databases.

# Arguments

  - `db1::DatabaseSQLite`: The first database to compare (used as the reference for reading collection structure)
  - `db2::DatabaseSQLite`: The second database to compare against the first
  - `collection_id::String`: The name of the collection (table) to compare scalar relations for

# Returns

A vector of strings describing differences found in scalar relations. Each string includes the
collection name, relation attribute name, target collection name, element index, and the labels
of the related elements that differ. Returns an empty vector if all scalar relations are identical.
"""
function compare_scalar_relations(
    db1::DatabaseSQLite,
    db2::DatabaseSQLite,
    collection_id::String,
)
    differences = String[]
    collection = _get_collection(db1, collection_id)

    for (attr_id, attr) in collection.scalar_relations
        relations1 = read_scalar_relations(
            db1,
            collection_id,
            attr.relation_collection,
            attr.relation_type,
        )
        relations2 = read_scalar_relations(
            db2,
            collection_id,
            attr.relation_collection,
            attr.relation_type,
        )

        if length(relations1) != length(relations2)
            push!(
                differences,
                "Collection '$collection_id', scalar relation '$attr_id' to '$(attr.relation_collection)': different number of elements (db1: $(length(relations1)), db2: $(length(relations2)))",
            )
            continue
        end

        for (idx, (r1, r2)) in enumerate(zip(relations1, relations2))
            if r1 != r2
                push!(
                    differences,
                    "Collection '$collection_id', scalar relation '$attr_id' to '$(attr.relation_collection)', element $idx: relations differ (db1: $(r1), db2: $(r2))",
                )
            end
        end
    end

    return differences
end

"""
    compare_vector_relations(db1::DatabaseSQLite, db2::DatabaseSQLite, collection_id::String)

Compare vector relations between two databases for a specific collection.

This function iterates through all vector relations (arrays of foreign key references to other
collections) in the specified collection and compares them element-by-element between the two
databases. For each vector relation, it checks:
- The number of elements in the collection
- The length of each relation vector
- Individual relation references within each vector

# Arguments

  - `db1::DatabaseSQLite`: The first database to compare (used as the reference for reading collection structure)
  - `db2::DatabaseSQLite`: The second database to compare against the first
  - `collection_id::String`: The name of the collection (table) to compare vector relations for

# Returns

A vector of strings describing differences found in vector relations. Each string includes the
collection name, vector relation attribute name, target collection name, element index, vector
index, and the labels of the related elements that differ. Returns an empty vector if all vector
relations are identical.
"""
function compare_vector_relations(
    db1::DatabaseSQLite,
    db2::DatabaseSQLite,
    collection_id::String,
)
    differences = String[]
    collection = _get_collection(db1, collection_id)

    for (attr_id, attr) in collection.vector_relations
        relations1 = read_vector_relations(
            db1,
            collection_id,
            attr.relation_collection,
            attr.relation_type,
        )
        relations2 = read_vector_relations(
            db2,
            collection_id,
            attr.relation_collection,
            attr.relation_type,
        )

        if length(relations1) != length(relations2)
            push!(
                differences,
                "Collection '$collection_id', vector relation '$attr_id' to '$(attr.relation_collection)': different number of elements (db1: $(length(relations1)), db2: $(length(relations2)))",
            )
            continue
        end

        for (elem_idx, (rel_vec1, rel_vec2)) in enumerate(zip(relations1, relations2))
            if length(rel_vec1) != length(rel_vec2)
                push!(
                    differences,
                    "Collection '$collection_id', vector relation '$attr_id' to '$(attr.relation_collection)', element $elem_idx: different vector lengths (db1: $(length(rel_vec1)), db2: $(length(rel_vec2)))",
                )
                continue
            end

            for (vec_idx, (r1, r2)) in enumerate(zip(rel_vec1, rel_vec2))
                if r1 != r2
                    push!(
                        differences,
                        "Collection '$collection_id', vector relation '$attr_id' to '$(attr.relation_collection)', element $elem_idx, index $vec_idx: relations differ (db1: $(r1), db2: $(r2))",
                    )
                end
            end
        end
    end

    return differences
end

"""
    compare_time_series(db1::DatabaseSQLite, db2::DatabaseSQLite, collection_id::String)

Compare time series data between two databases for a specific collection.

This function iterates through all time series attributes in the specified collection, grouped
by their group_id, and compares the data for each element between the two databases. For each
time series, it checks:
- The size of the time series tables (number of rows and columns)
- The column names and their order
- Individual values in each cell of the time series data

The comparison handles missing values and null values appropriately,
ensuring that null states match between databases.

# Arguments

  - `db1::DatabaseSQLite`: The first database to compare (used as the reference for reading collection structure and element labels)
  - `db2::DatabaseSQLite`: The second database to compare against the first
  - `collection_id::String`: The name of the collection (table) to compare time series data for

# Returns

A vector of strings describing differences found in time series data. Each string includes the
collection name, time series attribute name, element label, column name, row index, and the
differing values. Returns an empty vector if all time series data is identical.
"""
function compare_time_series(
    db1::DatabaseSQLite,
    db2::DatabaseSQLite,
    collection_id::String,
)
    differences = String[]
    collection = _get_collection(db1, collection_id)

    # Get all element labels
    num_elements = number_of_elements(db1, collection_id)
    labels = read_scalar_parameters(db1, collection_id, "label")

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
                df1 = read_time_series_table(db1, collection_id, attr_id, label)
                df2 = read_time_series_table(db2, collection_id, attr_id, label)

                if size(df1) != size(df2)
                    push!(
                        differences,
                        "Collection '$collection_id', time series '$attr_id', label '$label': different table sizes (db1: $(size(df1)), db2: $(size(df2)))",
                    )
                    continue
                end

                if names(df1) != names(df2)
                    push!(
                        differences,
                        "Collection '$collection_id', time series '$attr_id', label '$label': different column names (db1: $(names(df1)), db2: $(names(df2)))",
                    )
                    continue
                end

                # Compare each column
                for col_name in names(df1)
                    col1 = df1[!, col_name]
                    col2 = df2[!, col_name]

                    for (row_idx, (v1, v2)) in enumerate(zip(col1, col2))
                        if ismissing(v1) && ismissing(v2)
                            continue
                        elseif ismissing(v1) || ismissing(v2)
                            if ismissing(v1) != ismissing(v2)
                                push!(
                                    differences,
                                    "Collection '$collection_id', time series '$attr_id', label '$label', column '$col_name', row $row_idx: missing mismatch (db1: $(v1), db2: $(v2))",
                                )
                            end
                        else
                            if v1 != v2
                                push!(
                                    differences,
                                    "Collection '$collection_id', time series '$attr_id', label '$label', column '$col_name', row $row_idx: values differ (db1: $(v1), db2: $(v2))",
                                )
                            end
                        end
                    end
                end
            end
        end
    end

    return differences
end

"""
    compare_time_series_files(db1::DatabaseSQLite, db2::DatabaseSQLite, collection_id::String)

Compare time series file paths between two databases for a specific collection.

This function compares the file paths stored in the time_series_files table for each element
in the specified collection. It checks whether file paths are present in one database but not
the other, and whether the file paths match when present in both databases.

Note that this function only compares the file path strings stored in the database, not the
contents of the files themselves.

# Arguments

  - `db1::DatabaseSQLite`: The first database to compare (used as the reference for reading collection structure and element labels)
  - `db2::DatabaseSQLite`: The second database to compare against the first
  - `collection_id::String`: The name of the collection (table) to compare time series file references for

# Returns

A vector of strings describing differences found in time series file paths. Each string includes
the collection name, time series file attribute name, and information about whether
file paths are missing or differ between databases. Returns an empty vector if all time series file
references are identical.
"""
function compare_time_series_files(
    db1::DatabaseSQLite,
    db2::DatabaseSQLite,
    collection_id::String,
)
    differences = String[]
    collection = _get_collection(db1, collection_id)

    if isempty(collection.time_series_files)
        return differences
    end

    for (attr_id, attr) in collection.time_series_files
        # Read time series file paths
        file_path1 = _read_time_series_file_path(db1, collection_id, attr_id)
        file_path2 = _read_time_series_file_path(db2, collection_id, attr_id)

        if ismissing(file_path1) && ismissing(file_path2)
            continue
        elseif ismissing(file_path1) && !ismissing(file_path2)
            push!(
                differences,
                "Collection '$collection_id', time series file '$attr_id': file path missing in db1 but present in db2",
            )
        elseif !ismissing(file_path1) && ismissing(file_path2)
            push!(
                differences,
                "Collection '$collection_id', time series file '$attr_id': file path present in db1 but missing in db2",
            )
        elseif file_path1 != file_path2
            push!(
                differences,
                "Collection '$collection_id', time series file '$attr_id': file paths differ (db1: $(file_path1), db2: $(file_path2))",
            )
        end
    end

    return differences
end

"""
    compare_databases(db1::DatabaseSQLite, db2::DatabaseSQLite)

Compare two databases to ensure they have the same data across all collections.

This function performs a comprehensive comparison of two PSRDatabase databases by iterating
through all collections and comparing their:
- Number of elements
- Scalar parameters
- Vector parameters
- Scalar relations
- Vector relations
- Time series data
- Time series file references

The comparison is thorough and identifies specific differences at the element, attribute, and
value level, making it useful for validating database migrations, testing database operations,
or ensuring data consistency after transformations.

# Arguments

  - `db1::DatabaseSQLite`: The first database to compare
  - `db2::DatabaseSQLite`: The second database to compare

# Returns

A vector of strings describing all differences found. Each string is a human-readable error
message that identifies:
- The collection where the difference was found
- The attribute or data type being compared
- The specific element (by index or label)
- The exact values that differ

If the databases are completely identical, returns an empty vector.

# Example

```julia
db1 = load_db("database1.sqlite")
db2 = load_db("database2.sqlite")

differences = compare_databases(db1, db2)

if isempty(differences)
    println("Databases are identical")
else
    println("Found \$(length(differences)) differences:")
    for diff in differences
        println("  - \$diff")
    end
end
```
"""
function compare_databases(
    db1::DatabaseSQLite,
    db2::DatabaseSQLite,
)
    all_differences = String[]
    collection_ids = _get_collection_ids(db1)

    for collection_id in collection_ids
        # Compare number of elements
        num_elements1 = number_of_elements(db1, collection_id)
        num_elements2 = number_of_elements(db2, collection_id)

        if num_elements1 != num_elements2
            push!(
                all_differences,
                "Collection '$collection_id': different number of elements (db1: $num_elements1, db2: $num_elements2)",
            )
            continue
        end

        # Compare scalar parameters
        append!(all_differences, compare_scalar_parameters(db1, db2, collection_id))

        # Compare vector parameters
        append!(all_differences, compare_vector_parameters(db1, db2, collection_id))

        # Compare scalar relations
        append!(all_differences, compare_scalar_relations(db1, db2, collection_id))

        # Compare vector relations
        append!(all_differences, compare_vector_relations(db1, db2, collection_id))

        # Compare time series
        if !isempty(_get_collection(db1, collection_id).time_series)
            append!(all_differences, compare_time_series(db1, db2, collection_id))
        end

        # Compare time series files
        append!(all_differences, compare_time_series_files(db1, db2, collection_id))
    end

    return all_differences
end
