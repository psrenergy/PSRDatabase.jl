# PSRDatabase.jl

[build-img]: https://github.com/psrenergy/PSRDatabase.jl/actions/workflows/ci.yml/badge.svg?branch=master
[build-url]: https://github.com/psrenergy/PSRDatabase.jl/actions?query=workflow%3ACI

[codecov-img]: https://codecov.io/gh/psrenergy/PSRDatabase.jl/coverage.svg?branch=master
[codecov-url]: https://codecov.io/gh/psrenergy/PSRDatabase.jl?branch=master

| **Build Status** | **Coverage** | **Documentation** |
|:-----------------:|:-----------------:|:-----------------:|
| [![Build Status][build-img]][build-url] | [![Codecov branch][codecov-img]][codecov-url] |[![](https://img.shields.io/badge/docs-latest-blue.svg)](https://psrenergy.github.io/PSRDatabase.jl/dev/)


PSRDatabase is a Julia package that provides a robust interface for creating, managing, and interacting with SQLite databases designed for PSR energy models. It offers a comprehensive framework for handling collections, attributes, relations, and time series data with built-in validation and migration support.


## Features

- **Structured Data Management**: Define collections with scalar and vector attributes
- **Flexible Relations**: Support for scalar and vector relationships between collections
- **Time Series Support**: Store and query time series data with multiple dimensions
- **Database Migrations**: Version control your database schema with automatic migration system
- **Automatic Docstrings**: Generate documentation for model-specific functions

## Installation

This package is registered so you can simply `add` it using Julia's `Pkg` manager:
```julia
julia> import Pkg

julia> Pkg.add("PSRDatabase")
```

## Documentation

For complete usage examples and detailed documentation, including:
- Getting started guide
- Complete API reference
- SQL schema rules and conventions
- Time series handling
- Database migrations
- SQLite examples

Visit the [documentation](https://psrenergy.github.io/PSRDatabase.jl/dev/).

## Contributing

Users are encouraged to contribute by opening issues and pull requests. If you wish to implement a feature, please follow the [JuMP Style Guide](https://jump.dev/JuMP.jl/v0.21.10/developers/style/#Style-guide-and-design-principles).

