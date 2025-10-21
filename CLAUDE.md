# PSRDatabase.jl 

## Overview


PSRDatabase is a Julia framework for creating and managing SQLite databases with specific conventions for storing relational data, vectors, and time series. It's designed for applications that need structured data storage with support for migrations, time series, and complex relationships.

## Technology Stack

- **Language**: Julia
- **Database**: SQLite
- **Key Features**: Time series support, migrations, vector attributes, strict schemas

### SQLite Basics

SQLite is a lightweight relational database that doesn't require a server. For debugging and visualization, SQLiteStudio is recommended (https://sqlitestudio.pl/).

## SQL Schema Conventions

### Collections (Tables)

Collections are the primary data structures in PSRDatabase. They follow strict naming and structure conventions:

- **Naming**: Pascal Case, singular form (e.g., `Resource`, `ThermalPlant`)
- **Primary Key**: Must have `id INTEGER PRIMARY KEY AUTOINCREMENT`
- **Strict Mode**: All tables use `STRICT` keyword

Example:
```sql
CREATE TABLE ThermalPlant(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    label TEXT UNIQUE NOT NULL,
    minimum_generation REAL DEFAULT 0
) STRICT;
```

#### Configuration Collection

Every database must have a `Configuration` collection to store case information. The `label` column is optional for this collection.

```sql
CREATE TABLE Configuration (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    value1 REAL NOT NULL DEFAULT 100
) STRICT;
```

### Attributes

#### Non-Vector Attributes

- **Naming**: snake_case, singular form
- **Label Attribute**: If named `label`, must be `TEXT UNIQUE NOT NULL`
- **Date Attributes**: Names starting with `date` are stored as `TEXT` and map to DateTime objects
- **Relations**: Names starting with another collection name indicate foreign key relationships

Example with relationships:
```sql
CREATE TABLE Plant(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    label TEXT UNIQUE NOT NULL,
    capacity REAL NOT NULL,
    gaugingstation_id INTEGER,
    plant_spill_to INTEGER,
    FOREIGN KEY(gaugingstation_id) REFERENCES GaugingStation(id) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY(plant_spill_to) REFERENCES Plant(id) ON UPDATE SET NULL ON DELETE CASCADE
) STRICT;
```

#### Vector Attributes

Vector attributes are stored in separate tables to handle arrays of values for each element.

- **Naming**: `COLLECTION_vector_GROUP_OF_ATTRIBUTES`
- **Required Columns**: `id`, `vector_index`
- **Purpose**: Group vectors that must have the same size

Example:
```sql
CREATE TABLE ThermalPlant_vector_some_group(
    id INTEGER,
    vector_index INTEGER NOT NULL,
    some_value REAL NOT NULL,
    some_other_value REAL,
    FOREIGN KEY (id) REFERENCES ThermalPlant(id) ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY (id, vector_index)
) STRICT;
```

Vector relations with other collections:
```sql
CREATE TABLE HydroPlant_vector_GaugingStation(
    id INTEGER,
    vector_index INTEGER NOT NULL,
    conversion_factor REAL NOT NULL,
    gaugingstation_id INTEGER,
    FOREIGN KEY (gaugingstation_id) REFERENCES GaugingStation(id) ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY (id, vector_index)
) STRICT;
```

### Time Series

Time series data can be stored in two ways:

#### 1. Time Series Files (External)

References to external time series files:

```sql
CREATE TABLE Plant_time_series_files (
    generation TEXT,
    cost TEXT
) STRICT;
```

#### 2. Time Series in Database (Internal)

- **Naming**: `COLLECTION_time_series_GROUP_OF_ATTRIBUTES`
- **Required Column**: `date_time TEXT NOT NULL` (format: `YYYY-MM-DD HH:MM:SS`)
- **Primary Key**: Includes `id` and `date_time` (and any additional dimensions)

Example:
```sql
CREATE TABLE Resource_time_series_group1 (
    id INTEGER, 
    date_time TEXT NOT NULL,
    some_vector1 REAL,
    some_vector2 REAL,
    FOREIGN KEY(id) REFERENCES Resource(id) ON DELETE CASCADE ON UPDATE CASCADE,
    PRIMARY KEY (id, date_time)
) STRICT; 
```

Multi-dimensional time series with block and scenario:
```sql
CREATE TABLE Resource_time_series_group2 (
    id INTEGER, 
    date_time TEXT NOT NULL,
    block INTEGER NOT NULL,
    some_vector3 REAL,
    some_vector4 REAL,
    FOREIGN KEY(id) REFERENCES Resource(id) ON DELETE CASCADE ON UPDATE CASCADE,
    PRIMARY KEY (id, date_time, block)
) STRICT; 
```

## Time Series Behavior

### Missing Value Handling

PSRDatabase has intelligent missing value handling for time series:

1. If querying a missing value, it returns the most recent previous value
2. If no previous value exists, it returns type-specific defaults:
   - `Float64`: `NaN`
   - `Int64`: `typemin(Int)`
   - `String`: `""` (empty string)
   - `DateTime`: `typemin(DateTime)`

**Example Behavior:**

Given data:
| Date | some_vector1 | some_vector2 |
|------|--------------|--------------|
| 2020 | 1.0          | missing      |
| 2021 | missing      | 1.0          |
| 2022 | 3.0          | missing      |

Queries:
- `some_vector1` at 2020 → `1.0`
- `some_vector2` at 2020 → `NaN` (no previous value)
- `some_vector1` at 2021 → `1.0` (previous value from 2020)
- `some_vector2` at 2021 → `1.0`
- `some_vector1` at 2022 → `3.0`
- `some_vector2` at 2022 → `1.0` (previous value from 2021)

### Operations

#### Creating Elements with Time Series

```julia
using DataFrames, Dates, PSRDatabase

db = PSRDatabase.create_empty_db_from_schema(db_path, path_schema; force = true)

df_group1 = DataFrame(;
    date_time = [DateTime(2000), DateTime(2001), DateTime(2002)],
    some_vector1 = [missing, 1.0, 2.0],
    some_vector2 = [1.0, missing, 5.0],
)

PSRDatabase.create_element!(
    db,
    "Resource";
    label = "Resource 1",
    group1 = df_group1,
)
```

#### Adding Single Time Series Rows

```julia
PSRDatabase.add_time_series_row!(
    db,
    "Resource",
    "some_vector1",
    "Resource 1",
    10.0; # new value
    date_time = DateTime(2000)
)
```

#### Reading Time Series

As a table:
```julia
df = PSRDatabase.read_time_series_table(
    db,
    "Resource",
    "some_vector1",
    "Resource 1",
)
```

As a single row (with caching for performance):
```julia
values = PSRDatabase.read_time_series_row(
    db,
    "Resource",
    "some_vector1",
    Float64;
    date_time = DateTime(2020)
)
```

#### Updating Time Series

```julia
PSRDatabase.update_time_series_row!(
    db,
    "Resource",
    "some_vector3",
    "Resource 1",
    10.0; # new value
    date_time = DateTime(2000),
    block = 1
)
```

#### Deleting Time Series

Deletes all data for a time series group:
```julia
PSRDatabase.delete_time_series!(
    db,
    "Resource",
    "group1",
    "Resource 1",
)
```

## Migrations

Migrations enable database schema versioning and updates without losing data.

### Structure

```
database/migrations
├── 1
│   ├── up.sql
│   └── down.sql
└── 2
    ├── up.sql
    └── down.sql
```

- **up.sql**: Updates schema to new version
- **down.sql**: Reverts changes from up.sql

### Creating Migrations

```julia
# Register migrations directory
PSRDatabase.set_migrations_folder(path)

# Create new migration
PSRDatabase.create_migration(version, name)
```

### Running Migrations

```julia
PSRDatabase.apply_migrations!(db)
```

### Testing Migrations

It's critical to test migrations:
```julia
PSRDatabase.test_migrations()
```

All models should include migration tests in their test suite.

## Key Design Principles

1. **Strict Schemas**: All tables use SQLite's STRICT mode
2. **Cascade Operations**: Foreign keys use `ON UPDATE CASCADE ON DELETE CASCADE` by default
3. **Flexible Time Series**: Support for sparse data and missing values with intelligent defaults
4. **Vector Grouping**: Vectors that must have the same size are grouped together
5. **Migration Support**: Database schemas can evolve over time with proper versioning

## Common Patterns

### Creating a Basic Collection

```sql
CREATE TABLE MyCollection (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    label TEXT UNIQUE NOT NULL,
    some_parameter REAL NOT NULL
) STRICT;
```

### Adding Vector Attributes

```sql
CREATE TABLE MyCollection_vector_group1(
    id INTEGER,
    vector_index INTEGER NOT NULL,
    value1 REAL NOT NULL,
    value2 REAL,
    FOREIGN KEY (id) REFERENCES MyCollection(id) ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY (id, vector_index)
) STRICT;
```

### Adding Time Series

```sql
CREATE TABLE MyCollection_time_series_group1 (
    id INTEGER, 
    date_time TEXT NOT NULL,
    metric1 REAL,
    metric2 REAL,
    FOREIGN KEY(id) REFERENCES MyCollection(id) ON DELETE CASCADE ON UPDATE CASCADE,
    PRIMARY KEY (id, date_time)
) STRICT;
```

## Core API Reference

### Database Connection and Creation

#### Creating Databases

```julia
# Create database from SQL schema file
db = PSRDatabase.create_empty_db_from_schema(
    database_path::String,
    path_schema::String;
    force::Bool = false  # If true, overwrites existing file
)

# Create database from migrations
db = PSRDatabase.create_empty_db_from_migrations(
    database_path::String,
    path_migrations::String;
    force::Bool = false
)

# Load existing database
db = PSRDatabase.load_db(
    database_path::String;
    read_only::Bool = false
)

# Load database with migrations
db = PSRDatabase.load_db(
    database_path::String,
    path_migrations::String
)
```

#### Database Structure

The main database object is `DatabaseSQLite`:
- `sqlite_db::SQLite.DB`: Underlying SQLite connection
- `database_path::String`: Path to database file
- `collections_map::OrderedDict{String, Collection}`: Metadata about all collections
- `read_only::Bool`: Whether database is in read-only mode
- `_time_controller::TimeController`: Internal cache for time series queries (enables read-only optimization)

### CRUD Operations

#### Create

```julia
# Create an element with scalar parameters
PSRDatabase.create_element!(
    db::DatabaseSQLite,
    collection_id::String;
    kwargs...  # attribute_name = value pairs
)

# Examples:
PSRDatabase.create_element!(db, "Configuration"; label = "Case 1", value1 = 100.0)
PSRDatabase.create_element!(db, "Plant"; label = "Plant 1", capacity = 50.0)

# Create with vectors
PSRDatabase.create_element!(
    db, "Plant";
    label = "Plant 1",
    some_vector = [1.0, 2.0, 3.0]  # Must be non-empty
)

# Create with time series (pass DataFrame)
using DataFrames, Dates
df = DataFrame(
    date_time = [DateTime(2020), DateTime(2021)],
    generation = [100.0, 200.0]
)
PSRDatabase.create_element!(
    db, "Plant";
    label = "Plant 1",
    group1 = df  # DataFrame for time series group
)
```

#### Read

**Scalar Parameters:**
```julia
# Read all values for all elements (returns Vector)
values = PSRDatabase.read_scalar_parameters(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String;
    default::Union{Nothing, Any} = nothing
)

# Read single element by label
value = PSRDatabase.read_scalar_parameter(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    label::String;
    default::Union{Nothing, Any} = nothing
)

# Read single element by ID
value = PSRDatabase.read_scalar_parameter(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    id::Integer;
    default::Union{Nothing, Any} = nothing
)
```

**Vector Parameters:**
```julia
# Read vectors for all elements (returns Vector of Vectors)
vectors = PSRDatabase.read_vector_parameters(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String;
    default::Union{Nothing, Any} = nothing
)

# Read vector for specific element
vector = PSRDatabase.read_vector_parameter(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    label::String;
    default::Union{Nothing, Any} = nothing
)
```

**Scalar Relations:**
```julia
# Read relation labels for all elements
labels = PSRDatabase.read_scalar_relations(
    db::DatabaseSQLite,
    collection_from::String,
    collection_to::String,
    relation_type::String;
    default::Union{Nothing, Any} = nothing
)

# Read single relation
label = PSRDatabase.read_scalar_relation(
    db::DatabaseSQLite,
    collection_from::String,
    collection_to::String,
    label_collection_from::String,
    relation_type::String;
    default::Union{Nothing, Any} = nothing
)
```

**Time Series:**
```julia
# Read entire time series table as DataFrame
df = PSRDatabase.read_time_series_table(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    label::String
)

# Read time series row (optimized with caching)
values = PSRDatabase.read_time_series_row(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    ::Type{T};  # Type: Float64, Int64, String, DateTime
    date_time::DateTime,
    additional_dimensions...  # e.g., block=1, scenario=2
)
```

**Utility Functions:**
```julia
# Count elements in collection
n = PSRDatabase.number_of_elements(db::DatabaseSQLite, collection_id::String)

# Get element ID from label
id = PSRDatabase._get_id(db::DatabaseSQLite, collection_id::String, label::String)
```

#### Update

**Scalar Parameters:**
```julia
PSRDatabase.update_scalar_parameter!(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    label::String,
    value
)
```

**Vector Parameters:**
```julia
PSRDatabase.update_vector_parameters!(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    label::String,
    values::Vector  # Must match existing vector size
)
```

**Scalar Relations:**
```julia
PSRDatabase.set_scalar_relation!(
    db::DatabaseSQLite,
    collection_from::String,
    collection_to::String,
    label_collection_from::String,
    label_collection_to::String,
    relation_type::String
)

# By ID
PSRDatabase.set_scalar_relation!(
    db::DatabaseSQLite,
    collection_from::String,
    collection_to::String,
    id_collection_from::Integer,
    id_collection_to::Integer,
    relation_type::String
)
```

**Vector Relations:**
```julia
PSRDatabase.set_vector_relation!(
    db::DatabaseSQLite,
    collection_from::String,
    collection_to::String,
    label_collection_from::String,
    labels_collection_to::Vector{String},
    relation_type::String
)
```

**Time Series:**
```julia
# Add single time series row
PSRDatabase.add_time_series_row!(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    label::String,
    value;
    date_time::DateTime,
    additional_dimensions...
)

# Update existing time series row
PSRDatabase.update_time_series_row!(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    label::String,
    value;
    date_time::DateTime,
    additional_dimensions...
)

# Set time series file path
PSRDatabase.set_time_series_file!(
    db::DatabaseSQLite,
    collection_id::String,
    attribute_id::String,
    label::String,
    file_path::String
)
```

#### Delete

```julia
# Delete element by label (cascades to vectors and relations)
PSRDatabase.delete_element!(
    db::DatabaseSQLite,
    collection_id::String,
    label::String
)

# Delete element by ID
PSRDatabase.delete_element!(
    db::DatabaseSQLite,
    collection_id::String,
    id::Integer
)

# Delete all time series for a group
PSRDatabase.delete_time_series!(
    db::DatabaseSQLite,
    collection_id::String,
    group_id::String,
    label::String
)
```

## Data Types and Attributes

### Attribute Types

PSRDatabase categorizes attributes into six types:

1. **ScalarParameter**: Simple column values (numbers, text, dates)
2. **ScalarRelation**: Foreign key to another collection
3. **VectorParameter**: Array of values in separate table
4. **VectorRelation**: Array of foreign keys
5. **TimeSeries**: Time-indexed data with dimensions
6. **TimeSeriesFile**: Path to external time series file

### Type Mapping

SQL to Julia type conversion:
- `INTEGER` → `Int64`
- `REAL` → `Float64`
- `TEXT` → `String`
- `TEXT` (date_* columns) → `DateTime`

### Default Values for Missing Data

When reading missing/NULL values:
- `Float64`: `NaN`
- `Int64`: `typemin(Int64)`
- `String`: `""`
- `DateTime`: `typemin(DateTime)`

You can override with the `default` parameter in read functions.

## Advanced Features

### TimeController (Caching)

For read-only databases, PSRDatabase automatically caches time series queries to improve performance. The TimeController:
- Stores previous/next non-missing values for each element
- Avoids repeated database queries for the same time period
- Automatically activated when using `read_only=true`

### Validation

The framework validates:
- Table names (Pascal Case)
- Column names (snake_case)
- Foreign key actions (CASCADE)
- Primary keys on id and vector_index
- Label uniqueness and NOT NULL constraints
- STRICT mode on all tables

Access validation with:
```julia
PSRDatabase._validate_database(sqlite_db)
```

### Collections Metadata

Each Collection object contains:
```julia
struct Collection
    id::String
    scalar_parameters::OrderedDict{String, ScalarParameter}
    scalar_relations::OrderedDict{String, ScalarRelation}
    vector_parameters::OrderedDict{String, VectorParameter}
    vector_relations::OrderedDict{String, VectorRelation}
    time_series::OrderedDict{String, TimeSeries}
    time_series_files::OrderedDict{String, TimeSeriesFile}
end
```

Access via: `db.collections_map["CollectionName"]`

## Migrations System

### Migration Structure

Each migration has a version number and two files:
```
migrations/
  1/
    up.sql    # Apply migration
    down.sql  # Revert migration
  2/
    up.sql
    down.sql
```

### Migration Operations

```julia
# Create new migration
PSRDatabase.create_migration(path_migrations_directory::String, version::Int)

# Apply migrations from version X to Y
PSRDatabase.apply_migrations!(
    db::SQLite.DB,
    path_migrations_directory::String,
    from::Int,
    to::Int,
    direction::Symbol  # :up or :down
)

# Get current database version
version = PSRDatabase.get_user_version(db::SQLite.DB)

# Get latest migration version
latest = PSRDatabase.get_last_user_version(path_migrations_directory::String)

# Test all migrations (for test suites)
PSRDatabase.test_migrations()
```

### Migration Features

- Automatic backup creation before applying migrations
- User version tracking via `PRAGMA user_version`
- Validation of migration chain integrity
- Support for both upgrade and downgrade paths

## File Structure

The repository follows this structure:
- `src/`: Core implementation files
  - `PSRDatabase.jl`: Main module
  - `database_sqlite.jl`: Database connection and initialization
  - `create.jl`: Element creation operations
  - `read.jl`: Read operations
  - `update.jl`: Update operations
  - `delete.jl`: Delete operations
  - `attribute.jl`: Attribute type definitions
  - `collection.jl`: Collection metadata management
  - `time_controller.jl`: Time series caching
  - `migrations.jl`: Migration system
  - `validate.jl`: Schema validation
  - `utils.jl`: Utility functions
  - `exceptions.jl`: Custom exception types
  - `docstrings.jl`: Documentation strings
- `test/`: Comprehensive test suite
- `docs/`: Documentation (including this guide)
- `format/`: Code formatting tools
- `revise/`: Development workflow tools

## Testing Philosophy

Tests are organized by functionality:
- `test_create/`: Creation operations
- `test_read/`: Read operations
- `test_update/`: Update operations
- `test_delete/`: Delete operations
- `test_time_series/`: Time series functionality
- `migrations_tests/`: Migration testing
- `test_utils/`: Utility function tests
- `test_valid_database_definitions/`: Schema validation tests

Each test typically includes both Julia test code and SQL schema files.

## Error Handling

Custom exception type:
```julia
struct DatabaseException <: Exception
    msg::String
end
```

Common error scenarios:
- Collection or attribute doesn't exist
- Invalid attribute type
- Constraint violations (NOT NULL, UNIQUE, FOREIGN KEY)
- Migration version mismatches
- Invalid table/column naming conventions
- Empty vectors or DataFrames passed to create functions

## Julia Style Guide (Based on JuMP.jl)

PSRDatabase.jl follows the Julia style guide conventions adopted by JuMP.jl. This section outlines the key principles.

### Formatting

**JuliaFormatter**: The project uses JuliaFormatter.jl for automatic code formatting. Format code before committing:

```julia
using JuliaFormatter
format("src")
format("test")
```

**Whitespace Rules**:
- Never use more than one blank line within a function
- Never begin a function with a blank line
- Use proper spacing for readability

Good:
```julia
function foo(x)
    y = 2 * x
    return y
end
```

Bad:
```julia
function foo(x)
    y = 2 * x


    return y
end

function foo(x)

    y = 2 * x
    return y
end
```

### Code Style

**Juxtaposed Multiplication**:
- Only use juxtaposed multiplication when the right-hand side is a symbol
- Prefer `2 * x` over `2x` when space is not an issue
- Never use `2(x + 1)`, always use `2 * (x + 1)`

**Empty Vectors**:
- Prefer `T[]` over `Vector{T}()` for conciseness

**Comments**:
- Must be proper English sentences with appropriate punctuation
- Good: `# This is a comment demonstrating a good comment.`
- Bad: `# a bad comment`

### Naming Conventions

**Case Conventions**:
```julia
module SomeModule end
function some_function end
const SOME_CONSTANT = ...
struct SomeStruct
    some_field::SomeType
end
@enum SomeEnum ENUM_VALUE_A ENUM_VALUE_B
some_local_variable = ...
some_file.jl  # Except for ModuleName.jl
```

**Exported vs. Non-Exported**:
- Begin private module-level functions and constants with underscore (`_`)
- All public objects should be exported
- Never begin local variable names with underscore

```julia
module MyModule

export public_function, PUBLIC_CONSTANT

function _private_function()
    local_variable = 1  # No underscore for local vars
    return
end

function public_function end

const _PRIVATE_CONSTANT = 3.14159
const PUBLIC_CONSTANT = 1.41421

end
```

**Underscores in Names**:
- Always use underscores to separate words in variable and function names
- This differs from base Julia but provides consistency
- Examples: `has_key`, `remote_call_fetch`, `base_name`

**Use of `!`**:
- Omit `!` when the name makes modification clear: `add_constraint`, `set_name`, `update_scalar_parameter!`
- Use `!` to distinguish modifying vs. non-modifying variants: `scale` vs. `scale!`
- Document which arguments are modified in the docstring

**Abbreviations**:
- Use abbreviations to improve readability, not to save typing
- Use consistently within a codebase
- Common abbreviations:
  - `num` for number
  - `con` for constraint
  - `idx` for index

**Variable Names**:
- Avoid one-letter variable names (except for loop indices)
- Use `model = Model()` instead of `m = Model()`

### Type Safety

**User-Facing MethodError**:
- Users should only see `MethodError` for methods they called directly
- Provide type constraints at the top of the call chain

Good:
```julia
_internal_function(x::Integer) = x + 1

# User sees MethodError for public_function, which is clear
public_function(x::Integer) = _internal_function(x)
```

Bad:
```julia
_internal_function(x::Integer) = x + 1

# User sees confusing MethodError for _internal_function
public_function(x) = _internal_function(x)
```

Alternative pattern:
```julia
_internal_function(x::Integer) = x + 1

function _internal_function(x)
    error(
        "Internal error. This probably means that you called " *
        "public_function() with the wrong type.",
    )
end

public_function(x) = _internal_function(x)
```

### @enum vs. Symbol

- Use `@enum` for type-safe finite sets of values (like C/C++ enum)
- Provides type safety and docstrings
- Use for reporting statuses, modes, etc.
- Reserve `Symbol` for identifiers (e.g., `:my_variable`)
- Use `String` for long-form messages

### Imports

**using vs. import**:
- Avoid `using ModuleName` except in scripts or REPL (like Python's `from module import *`)
- Prefer `using ModuleName: x, p` over `import ModuleName.x, ModuleName.p`
- The `using` form allows method extension without module qualification

### Documentation

**Docstrings**:
- Every exported object needs a docstring
- All examples should be jldoctests
- Use complete English sentences with proper punctuation
- Do not terminate lists with punctuation

Template:
```julia
"""
    function_signature(args; kwargs...)

Short sentence describing the function.

Optional: Add a slightly longer paragraph describing the function.

## Notes
- List any notes that the user should be aware of

## Examples
```jldoctest
julia> 1 + 1
2
```
"""
```

**Documentation Style**:
- Be concise
- Use lists instead of long sentences
- Use numbered lists for sequences: (1) do X, (2) then Y
- Use bullet points when items are not ordered
- Enclose Julia symbols in backticks: `VariableRef`
- Add "s" after backtick for plurals: `VariableRef`s
- Use `@meta` blocks for TODOs that shouldn't be visible

### Testing

**Test Structure**:
- Use modules to encapsulate tests
- Structure all tests as functions
- Prevents variable leakage between tests

```julia
module TestPkg

using Test

_helper_function() = 2

function test_addition()
    @test 1 + 1 == _helper_function()
end

function runtests()
    for name in names(@__MODULE__; all = true)
        if startswith("$(name)", "test_")
            @testset "$(name)" begin
                getfield(@__MODULE__, name)()
            end
        end
    end
end

end # TestPkg

TestPkg.runtests()
```

**Test Organization**:
- Break tests into multiple files
- One module per file
- Allows testing subsets by calling `include` with relevant file

## Best Practices for AI Assistants

1. **Always validate collection/attribute existence** before operations
2. **Use type-safe operations** - check if attribute is scalar/vector before reading
3. **Handle missing values** appropriately with defaults
4. **Test migrations** thoroughly with `test_migrations()`
5. **Follow naming conventions** strictly (PascalCase for tables, snake_case for columns/functions)
6. **Use STRICT mode** on all CREATE TABLE statements
7. **Define proper foreign key cascades** (CASCADE for most cases)
8. **Group related vectors** that must have the same size
9. **Cache time series reads** by using read-only mode when possible
10. **Backup databases** before applying migrations (automatic)
11. **Format code** with JuliaFormatter before committing
12. **Write proper docstrings** for all exported functions
13. **Use underscores** in all multi-word names (differs from base Julia)
14. **Prefix private functions** with underscore (`_`)
15. **Write tests as functions** within test modules
