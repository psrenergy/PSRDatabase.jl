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

# Example

```julia
db1 = create_empty_db_from_schema("db1.sqlite", "schema.sql"; force = true)
db2 = create_empty_db_from_schema("db2.sqlite", "schema.sql"; force = true)

create_element!(db1, "Configuration"; label = "Config1", value1 = 100.0)
create_element!(db2, "Configuration"; label = "Config1", value1 = 200.0)

differences = compare_scalar_parameters(db1, db2, "Configuration")
# Returns: ["Collection 'Configuration', attribute 'value1', element 1: values differ (db1: 100.0, db2: 200.0)"]
```
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

# Example

```julia
db1 = create_empty_db_from_schema("db1.sqlite", "schema.sql"; force = true)
db2 = create_empty_db_from_schema("db2.sqlite", "schema.sql"; force = true)

create_element!(db1, "Resource"; label = "Resource1", type = "D", some_value1 = [1.0, 2.0, 3.0])
create_element!(db2, "Resource"; label = "Resource1", type = "D", some_value1 = [1.0, 5.0, 3.0])

differences = compare_vector_parameters(db1, db2, "Resource")
# Returns: ["Collection 'Resource', vector attribute 'some_value1', element 1, index 2: values differ (db1: 2.0, db2: 5.0)"]
```
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

# Example

```julia
db1 = create_empty_db_from_schema("db1.sqlite", "schema.sql"; force = true)
db2 = create_empty_db_from_schema("db2.sqlite", "schema.sql"; force = true)

# Create resources
create_element!(db1, "Resource"; label = "Resource1", type = "D")
create_element!(db1, "Resource"; label = "Resource2", type = "E")
create_element!(db2, "Resource"; label = "Resource1", type = "D")
create_element!(db2, "Resource"; label = "Resource2", type = "E")

# Create plants with different scalar relations
create_element!(db1, "Plant"; label = "Plant1", capacity = 100.0)
create_element!(db2, "Plant"; label = "Plant1", capacity = 100.0)
set_scalar_relation!(db1, "Plant", "Resource", "Plant1", "Resource1", "id")
set_scalar_relation!(db2, "Plant", "Resource", "Plant1", "Resource2", "id")

differences = compare_scalar_relations(db1, db2, "Plant")
# Returns: ["Collection 'Plant', scalar relation 'resource_id' to 'Resource', element 1: relations differ (db1: Resource1, db2: Resource2)"]
```
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

# Example

```julia
db1 = create_empty_db_from_schema("db1.sqlite", "schema.sql"; force = true)
db2 = create_empty_db_from_schema("db2.sqlite", "schema.sql"; force = true)

# Create costs
for (db, label, value) in [(db1, "Cost1", 10.0), (db1, "Cost2", 20.0), (db1, "Cost3", 30.0),
                            (db2, "Cost1", 10.0), (db2, "Cost2", 20.0), (db2, "Cost3", 30.0)]
    create_element!(db, "Cost"; label = label, value = value)
end

# Create plants with different vector relations
create_element!(db1, "Plant"; label = "Plant1", capacity = 100.0, some_factor = [1.0, 2.0])
create_element!(db2, "Plant"; label = "Plant1", capacity = 100.0, some_factor = [1.0, 2.0])
set_vector_relation!(db1, "Plant", "Cost", "Plant1", ["Cost1", "Cost2"], "id")
set_vector_relation!(db2, "Plant", "Cost", "Plant1", ["Cost1", "Cost3"], "id")

differences = compare_vector_relations(db1, db2, "Plant")
# Returns: ["Collection 'Plant', vector relation 'cost_id' to 'Cost', element 1, index 2: relations differ (db1: Cost2, db2: Cost3)"]
```
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

# Example

```julia
using DataFrames, Dates

db1 = create_empty_db_from_schema("db1.sqlite", "schema.sql"; force = true)
db2 = create_empty_db_from_schema("db2.sqlite", "schema.sql"; force = true)

df1 = DataFrame(
    date_time = [DateTime(2020), DateTime(2021), DateTime(2022)],
    some_vector1 = [1.0, 2.0, 3.0],
    some_vector2 = [10.0, 20.0, 30.0],
)
df2 = DataFrame(
    date_time = [DateTime(2020), DateTime(2021), DateTime(2022)],
    some_vector1 = [1.0, 5.0, 3.0],
    some_vector2 = [10.0, 20.0, 30.0],
)

create_element!(db1, "Resource"; label = "Resource1", type = "D", group1 = df1)
create_element!(db2, "Resource"; label = "Resource1", type = "D", group1 = df2)

differences = compare_time_series(db1, db2, "Resource")
# Returns: ["Collection 'Resource', time series 'some_vector1', label 'Resource1', column 'some_vector1', row 2: values differ (db1: 2.0, db2: 5.0)"]
```
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

# Example

```julia
db1 = create_empty_db_from_schema("db1.sqlite", "schema.sql"; force = true)
db2 = create_empty_db_from_schema("db2.sqlite", "schema.sql"; force = true)

create_element!(db1, "Plant"; label = "Plant1", capacity = 100.0)
create_element!(db2, "Plant"; label = "Plant1", capacity = 100.0)

set_time_series_file!(db1, "Plant"; generation = "generation1.csv")
set_time_series_file!(db2, "Plant"; generation = "generation2.csv")

differences = compare_time_series_files(db1, db2, "Plant")
# Returns: ["Collection 'Plant', time series file 'generation': file paths differ (db1: generation1.csv, db2: generation2.csv)"]
```
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
    compare_set_parameters(db1::DatabaseSQLite, db2::DatabaseSQLite, collection_id::String)

Compare set parameters between two databases for a specific collection.

This function iterates through all set parameters in the specified collection and compares
the sets of values for each element between the two databases. For each set attribute, it checks:
- The number of elements in the collection
- The size of each set
- The values within each set (order-independent comparison)

# Arguments

  - `db1::DatabaseSQLite`: The first database to compare (used as the reference for reading collection structure)
  - `db2::DatabaseSQLite`: The second database to compare against the first
  - `collection_id::String`: The name of the collection (table) to compare set parameters for

# Returns

A vector of strings describing differences found in set parameters. Each string includes the
collection name, set attribute name, element index, and the differing set contents.
Returns an empty vector if all set parameters are identical.

# Example

```julia
db1 = create_empty_db_from_schema("db1.sqlite", "schema.sql"; force = true)
db2 = create_empty_db_from_schema("db2.sqlite", "schema.sql"; force = true)

# Create elements with different set parameter values (provided as arrays)
create_element!(db1, "Resource"; label = "Resource1", type = "D", some_set_value1 = [1.0, 2.0], some_set_value2 = [5.0, 4.0])
create_element!(db2, "Resource"; label = "Resource1", type = "D", some_set_value1 = [1.0, 2.0], some_set_value2 = [5.0, 6.0])

differences = compare_set_parameters(db1, db2, "Resource")
# Returns: ["Collection 'Resource', set attribute 'some_set_value2', element 2: sets differ (db1: 4.0, db2: 6.0)"]
```
"""
function compare_set_parameters(
    db1::DatabaseSQLite,
    db2::DatabaseSQLite,
    collection_id::String,
)
    differences = String[]
    collection = _get_collection(db1, collection_id)

    for (attr_id, attr) in collection.set_parameters
        sets1 = read_set_parameters(db1, collection_id, attr_id)
        sets2 = read_set_parameters(db2, collection_id, attr_id)

        for (elem_idx, (set1, set2)) in enumerate(zip(sets1, sets2))
            if length(set1) != length(set2)
                push!(
                    differences,
                    "Collection '$collection_id', set attribute '$(attr_id)', element $elem_idx: different set lengths (db1: $(length(set1)), db2: $(length(set2)))",
                )
                continue
            end

            for (set_idx, (s1, s2)) in enumerate(zip(set1, set2))
                if _is_null_in_db(s1) || _is_null_in_db(s2)
                    if _is_null_in_db(s1) != _is_null_in_db(s2)
                        push!(
                            differences,
                            "Collection '$collection_id', set attribute '$(attr_id)', element $elem_idx, set index $set_idx: null mismatch (db1: $(s1), db2: $(s2))",
                        )
                    end
                elseif s1 != s2
                    push!(
                        differences,
                        "Collection '$collection_id', set attribute '$(attr_id)', element $elem_idx: sets differ (db1: $(s1), db2: $(s2))",
                    )
                end
            end
        end
    end

    return differences
end

"""
    compare_set_relations(db1::DatabaseSQLite, db2::DatabaseSQLite, collection_id::String)

Compare set relations between two databases for a specific collection.

This function iterates through all set relations (sets of foreign key references to other
collections) in the specified collection and compares them element-by-element between the two
databases. For each set relation, it checks:
- The number of elements in the collection
- The size of each relation set
- Individual relation references within each set (order-independent comparison)

# Arguments

  - `db1::DatabaseSQLite`: The first database to compare (used as the reference for reading collection structure)
  - `db2::DatabaseSQLite`: The second database to compare against the first
  - `collection_id::String`: The name of the collection (table) to compare set relations for

# Returns

A vector of strings describing differences found in set relations. Each string includes the
collection name, set relation attribute name, target collection name, element index, and the
labels of the related elements that differ. Returns an empty vector if all set relations are identical.

# Example

```julia
db1 = create_empty_db_from_schema("db1.sqlite", "schema.sql"; force = true)
db2 = create_empty_db_from_schema("db2.sqlite", "schema.sql"; force = true)

# Create costs
for (db, label, value) in [(db1, "Cost1", 10.0), (db1, "Cost2", 20.0), (db1, "Cost3", 30.0),
                            (db2, "Cost1", 10.0), (db2, "Cost2", 20.0), (db2, "Cost3", 30.0)]
    create_element!(db, "Cost"; label = label, value = value)
end

# Create resources with different set relations (provided as separate arrays)
create_element!(db1, "Resource"; label = "Resource1", some_set_factor = [1.0, 2.0], cost_id = ["Cost1", "Cost2"])
create_element!(db2, "Resource"; label = "Resource1", some_set_factor = [1.0, 2.0], cost_id = ["Cost1", "Cost3"])

differences = compare_set_relations(db1, db2, "Resource")
# Returns: ["Collection 'Resource', set relation 'cost_id' to 'Cost', element 1, set index 2: relation sets differ (db1: Cost2, db2: Cost3)"]
```
"""
function compare_set_relations(
    db1::DatabaseSQLite,
    db2::DatabaseSQLite,
    collection_id::String,
)
    differences = String[]
    collection = _get_collection(db1, collection_id)

    for (attr_id, attr) in collection.set_relations
        relations1 = read_set_relations(
            db1,
            collection_id,
            attr.relation_collection,
            attr.relation_type,
        )
        relations2 = read_set_relations(
            db2,
            collection_id,
            attr.relation_collection,
            attr.relation_type,
        )

        for (elem_idx, (rel_set1, rel_set2)) in enumerate(zip(relations1, relations2))
            if length(rel_set1) != length(rel_set2)
                push!(
                    differences,
                    "Collection '$collection_id', set relation '$attr_id' to '$(attr.relation_collection)', element $elem_idx: different set lengths (db1: $(length(rel_set1)), db2: $(length(rel_set2)))",
                )
                continue
            end

            for (set_idx, (r1, r2)) in enumerate(zip(rel_set1, rel_set2))
                if _is_null_in_db(r1) || _is_null_in_db(r2)
                    if _is_null_in_db(r1) != _is_null_in_db(r2)
                        push!(
                            differences,
                            "Collection '$collection_id', set relation '$attr_id' to '$(attr.relation_collection)', element $elem_idx, set index $set_idx: null mismatch (db1: $(r1), db2: $(r2))",
                        )
                    end
                elseif r1 != r2
                    push!(
                        differences,
                        "Collection '$collection_id', set relation '$attr_id' to '$(attr.relation_collection)', element $elem_idx, set index $set_idx: relation sets differ (db1: $(r1), db2: $(r2))",
                    )
                end
            end
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
- Set parameters
- Set relations
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

# Examples

```julia
# Compare two identical databases
db1 = create_empty_db_from_schema("db1.sqlite", "schema.sql"; force = true)
db2 = create_empty_db_from_schema("db2.sqlite", "schema.sql"; force = true)

create_element!(db1, "Configuration"; label = "Config1", value1 = 100.0)
create_element!(db2, "Configuration"; label = "Config1", value1 = 100.0)

differences = compare_databases(db1, db2)
# Returns: []

# Compare databases with differences
db3 = create_empty_db_from_schema("db3.sqlite", "schema.sql"; force = true)
db4 = create_empty_db_from_schema("db4.sqlite", "schema.sql"; force = true)

create_element!(db3, "Configuration"; label = "Config1", value1 = 100.0)
create_element!(db4, "Configuration"; label = "Config1", value1 = 200.0)

differences = compare_databases(db3, db4)
# Returns: ["Collection 'Configuration', attribute 'value1', element 1: values differ (db1: 100.0, db2: 200.0)"]
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

        # Compare set parameters
        append!(all_differences, compare_set_parameters(db1, db2, collection_id))

        # Compare set relations
        append!(all_differences, compare_set_relations(db1, db2, collection_id))

        # Compare time series
        if !isempty(_get_collection(db1, collection_id).time_series)
            append!(all_differences, compare_time_series(db1, db2, collection_id))
        end

        # Compare time series files
        append!(all_differences, compare_time_series_files(db1, db2, collection_id))
    end

    return all_differences
end
