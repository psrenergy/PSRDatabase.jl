"""
Helper function to compare scalar parameters between two databases for a collection.
Returns a vector of error messages describing any differences found.
"""
function compare_scalar_parameters(
    db1::PSRDatabase.DatabaseSQLite,
    db2::PSRDatabase.DatabaseSQLite,
    collection_id::String,
)
    differences = String[]
    collection = PSRDatabase._get_collection(db1, collection_id)

    for (attr_id, attr) in collection.scalar_parameters
        if attr_id == "id"
            continue
        end

        values1 = PSRDatabase.read_scalar_parameters(db1, collection_id, attr_id)
        values2 = PSRDatabase.read_scalar_parameters(db2, collection_id, attr_id)

        if length(values1) != length(values2)
            push!(
                differences,
                "Collection '$collection_id', attribute '$attr_id': different number of values (db1: $(length(values1)), db2: $(length(values2)))",
            )
            continue
        end

        for (idx, (v1, v2)) in enumerate(zip(values1, values2))
            if ismissing(v1) && ismissing(v2)
                continue
            elseif attr.type <: Float64 && (_is_null_in_db(v1) || _is_null_in_db(v2))
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
Helper function to compare vector parameters between two databases for a collection.
Returns a vector of error messages describing any differences found.
"""
function compare_vector_parameters(
    db1::PSRDatabase.DatabaseSQLite,
    db2::PSRDatabase.DatabaseSQLite,
    collection_id::String,
)
    differences = String[]
    collection = PSRDatabase._get_collection(db1, collection_id)

    for (attr_id, attr) in collection.vector_parameters
        vectors1 = PSRDatabase.read_vector_parameters(db1, collection_id, attr_id)
        vectors2 = PSRDatabase.read_vector_parameters(db2, collection_id, attr_id)

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
                if ismissing(v1) && ismissing(v2)
                    continue
                elseif attr.type <: Float64 && (_is_null_in_db(v1) || _is_null_in_db(v2))
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
Helper function to compare scalar relations between two databases for a collection.
Returns a vector of error messages describing any differences found.
"""
function compare_scalar_relations(
    db1::PSRDatabase.DatabaseSQLite,
    db2::PSRDatabase.DatabaseSQLite,
    collection_id::String,
)
    differences = String[]
    collection = PSRDatabase._get_collection(db1, collection_id)

    for (attr_id, attr) in collection.scalar_relations
        relations1 = PSRDatabase.read_scalar_relations(
            db1,
            collection_id,
            attr.relation_collection,
            attr.relation_type,
        )
        relations2 = PSRDatabase.read_scalar_relations(
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
Helper function to compare vector relations between two databases for a collection.
Returns a vector of error messages describing any differences found.
"""
function compare_vector_relations(
    db1::PSRDatabase.DatabaseSQLite,
    db2::PSRDatabase.DatabaseSQLite,
    collection_id::String,
)
    differences = String[]
    collection = PSRDatabase._get_collection(db1, collection_id)

    for (attr_id, attr) in collection.vector_relations
        relations1 = PSRDatabase.read_vector_relations(
            db1,
            collection_id,
            attr.relation_collection,
            attr.relation_type,
        )
        relations2 = PSRDatabase.read_vector_relations(
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
Helper function to compare time series between two databases for a collection.
Returns a vector of error messages describing any differences found.
"""
function compare_time_series(
    db1::PSRDatabase.DatabaseSQLite,
    db2::PSRDatabase.DatabaseSQLite,
    collection_id::String,
)
    differences = String[]
    collection = PSRDatabase._get_collection(db1, collection_id)

    # Get all element labels
    num_elements = PSRDatabase.number_of_elements(db1, collection_id)
    labels = PSRDatabase.read_scalar_parameters(db1, collection_id, "label")

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
                df1 = PSRDatabase.read_time_series_table(db1, collection_id, attr_id, label)
                df2 = PSRDatabase.read_time_series_table(db2, collection_id, attr_id, label)

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
                        elseif typeof(v1) <: Float64 && (_is_null_in_db(v1) || _is_null_in_db(v2))
                            if _is_null_in_db(v1) != _is_null_in_db(v2)
                                push!(
                                    differences,
                                    "Collection '$collection_id', time series '$attr_id', label '$label', column '$col_name', row $row_idx: null mismatch (db1: $(v1), db2: $(v2))",
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
Helper function to compare time series files between two databases for a collection.
Returns a vector of error messages describing any differences found.
"""
function compare_time_series_files(
    db1::PSRDatabase.DatabaseSQLite,
    db2::PSRDatabase.DatabaseSQLite,
    collection_id::String,
)
    differences = String[]
    collection = PSRDatabase._get_collection(db1, collection_id)

    if isempty(collection.time_series_files)
        return differences
    end

    labels = PSRDatabase.read_scalar_parameters(db1, collection_id, "label")

    for label in labels
        for (attr_id, attr) in collection.time_series_files
            # Read time series file paths
            id1 = PSRDatabase._get_id(db1, collection_id, label)
            id2 = PSRDatabase._get_id(db2, collection_id, label)

            file_path1 = PSRDatabase._read_time_series_file_path(db1, collection_id, attr_id)
            file_path2 = PSRDatabase._read_time_series_file_path(db2, collection_id, attr_id)

            if ismissing(file_path1) || ismissing(file_path2)
                continue
            end

            if ismissing(file_path1) && !ismissing(file_path2)
                push!(
                    differences,
                    "Collection '$collection_id', time series file '$attr_id', label '$label': file path missing in db1 but present in db2",
                )
            end

            if !ismissing(file_path1) && ismissing(file_path2)
                push!(
                    differences,
                    "Collection '$collection_id', time series file '$attr_id', label '$label': file path present in db1 but missing in db2",
                )
            end

            if file_path1 != file_path2
                push!(
                    differences,
                    "Collection '$collection_id', time series file '$attr_id', label '$label': file paths differ (db1: $(file_path1), db2: $(file_path2))",
                )
            end
        end
    end

    return differences
end

"""
Compare two databases to ensure they have the same data.
Returns a vector of strings describing all differences found.
If the databases are identical, returns an empty vector.
"""
function compare_databases(
    db1::PSRDatabase.DatabaseSQLite,
    db2::PSRDatabase.DatabaseSQLite,
)
    all_differences = String[]
    collection_ids = PSRDatabase._get_collection_ids(db1)

    for collection_id in collection_ids
        # Compare number of elements
        num_elements1 = PSRDatabase.number_of_elements(db1, collection_id)
        num_elements2 = PSRDatabase.number_of_elements(db2, collection_id)

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
        if !isempty(PSRDatabase._get_collection(db1, collection_id).time_series)
            append!(all_differences, compare_time_series(db1, db2, collection_id))
        end

        # Compare time series files
        append!(all_differences, compare_time_series_files(db1, db2, collection_id))
    end

    return all_differences
end
