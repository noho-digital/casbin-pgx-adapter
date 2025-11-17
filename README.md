# casbin-pgx-adapter

A PostgreSQL adapter for [Casbin](https://casbin.org/) using the [pgx](https://github.com/jackc/pgx) driver.

## Overview

Casbin is a powerful and efficient open-source access control library that supports various access control models (ACL, RBAC, ABAC, etc.). This adapter enables Casbin to use PostgreSQL as its policy storage backend via the pgx driver, providing:

- Standard Casbin adapter interface
- Context-aware operations for request-scoped transactions
- Batch operations for efficient bulk policy updates
- Filtered adapter support for policy filtering
- Updatable adapter support for policy modifications

## Installation

```bash
go get github.com/noho-digital/casbin-pgx-adapter
```

## Usage


```go
package main

import (
    "log"
    
    "github.com/casbin/casbin/v2"
    pgxadapter "github.com/noho-digital/casbin-pgx-adapter"
)

func main() {
    // Database connection string
    connStr := "postgres://postgres:postgres@localhost:5432/casbin?sslmode=disable"
    
    // Create the adapter with optional configuration
    adapter, err := pgxadapter.NewAdapter(connStr,
        pgxadapter.WithTableName("my_casbin_rules"), // Optional: custom table name
        pgxadapter.WithDatabaseName("my_casbin_db"), // Optional: custom database name
    )
    if err != nil {
        log.Fatal("Failed to create adapter:", err)
    }
    defer adapter.Close()
    
    // Create Casbin enforcer with model file and adapter
    enforcer, err := casbin.NewEnforcer("path/to/model.conf", adapter)
    if err != nil {
        log.Fatal("Failed to create enforcer:", err)
    }
    
    // Add some policies
    enforcer.AddPolicy("alice", "data1", "read")
    enforcer.AddPolicy("bob", "data2", "write")
    enforcer.AddGroupingPolicy("alice", "admin")
    
    // Save policies to database
    if err := enforcer.SavePolicy(); err != nil {
        log.Fatal("Failed to save policy:", err)
    }
    
    // Check permissions
    if allowed, _ := enforcer.Enforce("alice", "data1", "read"); allowed {
        log.Println("Alice can read data1")
    }
    
    if allowed, _ := enforcer.Enforce("bob", "data1", "read"); !allowed {
        log.Println("Bob cannot read data1")
    }
}
```

## Supported Interfaces

This adapter implements the following Casbin adapter interfaces:

- `Adapter` - Standard Casbin adapter interface
- `ContextAdapter` - Context-aware operations
- `BatchAdapter` - Batch add/remove operations
- `FilteredAdapter` - Policy filtering support
- `UpdatableAdapter` - Policy update operations

## Development

### Testing

This project includes a Docker Compose setup for running tests against a PostgreSQL database.

#### Quick Start
```
# Start the test database
make test-db-up

# Run tests
make test


# Stop test database when done
make test-db-down
```

#### Manual Setup (existing database)

```bash
# Set environment variable
export TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5433/casbin_test?sslmode=disable"

# Run tests
go test -v ./...
```

#### Test Database Details

- **Host**: localhost:5433
- **Database**: casbin_test
- **Username**: postgres  
- **Password**: postgres
- **Connection String**: `postgres://postgres:postgres@localhost:5433/casbin_test?sslmode=disable`

## Requirements

- Go 1.24.4 or later
- Docker and Docker Compose (for development/testing)
- PostgreSQL database (for production)
- Casbin v2

## Related Projects

- [Casbin](https://github.com/casbin/casbin) - The authorization library
- [pgx](https://github.com/jackc/pgx) - PostgreSQL driver and toolkit for Go

