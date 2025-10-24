"""
    execute_statements(db::SQLite.DB, file::String)

Execute all statements in a .sql file against a database.
"""
function execute_statements(db::SQLite.DB, file::String)
    if !isfile(file)
        psr_database_sqlite_error("file not found: $file")
    end
    #! format: off
    # We turn off formatting here because of this discussion
    # https://github.com/domluna/JuliaFormatter.jl/issues/751
    # I agree that open do blocks with return are slighly misleading.
    raw_statements = open(joinpath(file), "r") do io
        read(io, String)
    end
    #! format: on
    statements = split(raw_statements, ";")
    for statement in statements
        trated_statement = _treat_sql_statement(statement)
        if !isempty(trated_statement)
            try
                DBInterface.execute(db, trated_statement)
            catch e
                @error """
                        psr_database_sqlite_error executing command: $trated_statement
                        psr_database_sqlite_error message: $(e.msg)
                        """
                rethrow(e)
            end
        end
    end
    return nothing
end

function _treat_sql_statement(statement::AbstractString)
    stripped_statement = strip(statement)
    return stripped_statement
end

"""
    create_empty_db_from_schema(database_path::String, path_schema::String; force::Bool = false)

Create a new empty database from a SQL schema file.

This function creates a new SQLite database file and executes all SQL statements from the schema file
to set up the database structure (tables, constraints, etc.).

# Arguments

  - `database_path::String`: The file path where the database will be created
  - `path_schema::String`: The path to the SQL schema file containing CREATE TABLE statements
  - `force::Bool`: If `true`, overwrites an existing database file at the same path. If `false` (default), throws an error if the file already exists

# Returns

  - `DatabaseSQLite`: A database connection object to the newly created database

# Throws

  - `DatabaseException` if the database file already exists and `force=false`
  - `DatabaseException` if the schema file is not found
  - `DatabaseException` if the database structure is invalid (e.g., missing Configuration table)
  - `SQLiteException` if SQL statements in the schema are invalid

# Examples

```julia
# Create a new database from a schema file
db = PSRDatabase.create_empty_db_from_schema(
    "my_database.sqlite",
    "schema.sql",
)

# Overwrite existing database
db = PSRDatabase.create_empty_db_from_schema(
    "my_database.sqlite",
    "schema.sql";
    force = true,
)
```

# See Also

  - `create_empty_db_from_migrations`: Create database using migration files
  - `load_db`: Load an existing database
"""
function create_empty_db_from_schema(
    database_path::String,
    path_schema::String;
    force::Bool = false,
)
    _throw_if_file_exists(database_path; force = force)
    db = try
        DatabaseSQLite_from_schema(
            database_path;
            path_schema = path_schema,
        )
    catch e
        rethrow(e)
    end
    return db
end

"""
    create_empty_db_from_migrations(database_path::String, path_migrations::String; force::Bool = false)

Create a new empty database by applying migration files.

This function creates a new SQLite database file and applies all migration files found in the
specified directory to build up the database structure incrementally.

# Arguments

  - `database_path::String`: The file path where the database will be created
  - `path_migrations::String`: The path to the directory containing migration SQL files
  - `force::Bool`: If `true`, overwrites an existing database file at the same path. If `false` (default), throws an error if the file already exists

# Returns

  - `DatabaseSQLite`: A database connection object to the newly created database

# Throws

  - `DatabaseException` if the database file already exists and `force=false`
  - `DatabaseException` if the migrations directory is not found or migrations are invalid
  - `DatabaseException` if the resulting database structure is invalid
  - `SQLiteException` if SQL statements in migrations are invalid

# Examples

```julia
# Create a new database from migrations
db = PSRDatabase.create_empty_db_from_migrations(
    "my_database.sqlite",
    "migrations/",
)

# Overwrite existing database
db = PSRDatabase.create_empty_db_from_migrations(
    "my_database.sqlite",
    "migrations/";
    force = true,
)
```

# See Also

  - `create_empty_db_from_schema`: Create database from a single schema file
  - `load_db`: Load and migrate an existing database
"""
function create_empty_db_from_migrations(
    database_path::String,
    path_migrations::String;
    force::Bool = false,
)
    _throw_if_file_exists(database_path; force = force)
    db = try
        DatabaseSQLite_from_migrations(
            database_path;
            path_migrations = path_migrations,
        )
    catch e
        rethrow(e)
    end
    return db
end

"""
    load_db(database_path::String; read_only::Bool = false)

Load an existing database from a file.

Opens a connection to an existing SQLite database file. The database structure is validated
and metadata is loaded into memory for fast access.

# Arguments

  - `database_path::String`: The file path to the database to load
  - `read_only::Bool`: If `true`, opens the database in read-only mode (immutable). If `false` (default), opens with read-write access

# Returns

  - `DatabaseSQLite`: A database connection object

# Throws

  - `DatabaseException` if the database file doesn't exist
  - `DatabaseException` if the database structure is invalid
  - `SQLiteException` if the file is not a valid SQLite database

# Examples

```julia
# Load a database with read-write access
db = PSRDatabase.load_db("my_database.sqlite")

# Load a database in read-only mode
db = PSRDatabase.load_db("my_database.sqlite"; read_only = true)

# Use the database and close when done
PSRDatabase.close!(db)
```

# See Also

  - `load_db(database_path, path_migrations)`: Load and apply migrations
  - `create_empty_db_from_schema`: Create a new database
"""
function load_db(database_path::String; read_only::Bool = false)
    db = try
        DatabaseSQLite(
            database_path;
            read_only = read_only,
        )
    catch e
        rethrow(e)
    end
    return db
end

"""
    load_db(database_path::String, path_migrations::String)

Load an existing database and apply any pending migrations.

Opens a connection to an existing SQLite database file and applies any migration files
that haven't been applied yet.

# Arguments

  - `database_path::String`: The file path to the database to load
  - `path_migrations::String`: The path to the directory containing migration SQL files

# Returns

  - `DatabaseSQLite`: A database connection object with all migrations applied

# Throws

  - `DatabaseException` if the database file doesn't exist
  - `DatabaseException` if migrations are invalid or cannot be applied
  - `SQLiteException` if the file is not a valid SQLite database or migration SQL is invalid

# Examples

```julia
# Load a database and apply pending migrations
db = PSRDatabase.load_db("my_database.sqlite", "migrations/")

# Work with the database
labels = PSRDatabase.read_scalar_parameters(db, "Plant", "label")

# Close when done
PSRDatabase.close!(db)
```

# See Also

  - `load_db(database_path; read_only)`: Load without applying migrations
  - `create_empty_db_from_migrations`: Create new database with migrations
"""
function load_db(database_path::String, path_migrations::String)
    db = try
        DatabaseSQLite_from_migrations(
            database_path;
            path_migrations = path_migrations,
        )
    catch e
        rethrow(e)
    end
    return db
end

function _throw_if_file_exists(file::String; force::Bool = false)
    if isfile(file)
        if force
            rm(file)
        else
            psr_database_sqlite_error("file already exists: $file")
        end
    end
    return nothing
end

function _open_db_connection(database_path::String)
    db = SQLite.DB(database_path)
    return db
end

function column_names(db::SQLite.DB, table::String)
    cols = SQLite.columns(db, table) |> DataFrame
    return cols.name
end

function index_list(db::SQLite.DB, table::String)
    indices = Vector{String}()
    for index in SQLite.indexes(db, table)
        push!(indices, index.name)
    end
    return indices
end

function table_names(db::SQLite.DB)
    tables = Vector{String}()
    for table in SQLite.tables(db)
        if table.name == "sqlite_sequence"
            continue
        end
        push!(tables, table.name)
    end
    return tables
end
