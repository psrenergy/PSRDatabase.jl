# Manual

The PSRDatabase module provides interfaces to access data structured by PSR to be used in its models. Currently there are two main interfaces. 
 * The interface for studies. This interface is designed to read parameters from the files, some examples are deficit costs, fuel costs, currency, storage capacity etc.
 * The interface for reading and writing time series data. Time series data in the context of most studies have 4 dimensions (agents, stages, scenarios and blocks). Since studies of renewables with multiple agents, scenarios and stages can get quite big, we have designed different formats that are optimized to some objective (human readability, size, fast reading and writing, etc.).

Both interfaces are defined as a set of methods that need to be implemented to make a different file format work. In this manual we will describe the abstract methods and give concrete examples of code to perform the work needed.

When using the PSRDatabase package in your codebase we strongly advise you to create a constant `PSRI` to keep the code concise and explicitly declare that a certain function came from PSRDatabase. This can be done by adding the following code to the top of the code
```julia
using PSRDatabase
const PSRI = PSRDatabase
```

## Initialize Study
```@docs
PSRDatabase.AbstractData
PSRDatabase.AbstractStudyInterface
PSRDatabase.load_study
PSRDatabase.description
PSRDatabase.max_elements
```

## Study dimensions
```@docs
PSRDatabase.StageType
PSRDatabase.total_stages
PSRDatabase.total_scenarios
PSRDatabase.total_blocks
PSRDatabase.total_openings
PSRDatabase.total_stages_per_year
```

## Study duration and blocking
```
PSRDatabase.BlockDurationMode
PSRDatabase.stage_duration
PSRDatabase.block_duration
PSRDatabase.block_from_stage_hour
```

## Read Scalar Attributes
```@docs
PSRDatabase.configuration_parameter
PSRDatabase.get_code
PSRDatabase.get_name
PSRDatabase.get_parm
PSRDatabase.get_parm_1d
PSRDatabase.get_parms
PSRDatabase.get_parms_1d
```

## Read Vector Attributes
### Time controller
```@docs
PSRDatabase.mapped_vector
PSRDatabase.go_to_stage
PSRDatabase.go_to_dimension
PSRDatabase.update_vectors!
```

### Direct access
```@docs
PSRDatabase.get_vector
PSRDatabase.get_vector_1d
PSRDatabase.get_vector_2d
PSRDatabase.get_vectors
PSRDatabase.get_vectors_1d
PSRDatabase.get_vectors_2d
PSRDatabase.get_nonempty_vector
PSRDatabase.get_series
```

## Relations between collections
```@docs
PSRDatabase.RelationType
PSRDatabase.is_vector_relation
PSRDatabase.get_references
PSRDatabase.get_vector_references
PSRDatabase.get_map
PSRDatabase.get_vector_map
PSRDatabase.get_reverse_map
PSRDatabase.get_reverse_vector_map
PSRDatabase.get_related
PSRDatabase.get_vector_related
```

## Reflection
```@docs
PSRDatabase.get_attribute_dim1
PSRDatabase.get_attribute_dim2
PSRDatabase.get_collections
PSRDatabase.get_attributes
PSRDatabase.get_attribute_struct
PSRDatabase.get_data_struct
PSRDatabase.Attribute
PSRDatabase.get_attributes_indexed_by
PSRDatabase.get_relations
PSRDatabase.get_attribute_dim
```

## Read and Write Graf files
### Open and Close
```@docs
PSRDatabase.AbstractReader
PSRDatabase.AbstractWriter
PSRDatabase.open
PSRDatabase.close
```

### Write entire file
```@docs
PSRDatabase.array_to_file
```

### Write registry
```@docs
PSRDatabase.write_registry
```

### Header information
```@docs
PSRDatabase.is_hourly
PSRDatabase.hour_discretization
PSRDatabase.max_stages
PSRDatabase.max_scenarios
PSRDatabase.max_blocks
PSRDatabase.max_blocks_current
PSRDatabase.max_blocks_stage
PSRDatabase.max_agents
PSRDatabase.stage_type
PSRDatabase.initial_stage
PSRDatabase.initial_year
PSRDatabase.data_unit
PSRDatabase.agent_names
```

### Read entire file
```@docs
PSRDatabase.file_to_array
PSRDatabase.file_to_array_and_header
```

### Read registry
```@docs
PSRDatabase.current_stage
PSRDatabase.current_scenario
PSRDatabase.current_block
PSRDatabase.goto
PSRDatabase.next_registry
```

## File conversion
```@docs
PSRDatabase.convert_file
PSRDatabase.add_reader!
```

## Reader mapper
```@docs
PSRDatabase.ReaderMapper
PSRDatabase.add_reader!
<!-- PSRDatabase.goto -->
PSRDatabase.close
```

## Modification API
```@docs
PSRDatabase.create_study
PSRDatabase.create_element!
PSRDatabase.set_parm!
PSRDatabase.set_vector!
PSRDatabase.set_series!
PSRDatabase.write_data
PSRDatabase.set_related!
PSRDatabase.set_vector_related!
```