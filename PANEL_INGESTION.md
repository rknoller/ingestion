# Panel Ingestion Tool

## Overview

The `panel_ingestion_app` is a C++ application that loads data from the `search_opinioncluster_panel` CSV file into the PostgreSQL database while validating foreign key constraints and outputting bad records that cannot be inserted.

## Features

- **Foreign Key Validation**: Pre-validates `opinioncluster_id` against the `search_opinioncluster` table before insertion
- **Bad Record Tracking**: Outputs all records that fail FK validation to a CSV file
- **Batch Processing**: Processes records in configurable batches (default 5000)
- **Simple CSV Format**: Handles the straightforward 3-column CSV structure (id, opinioncluster_id, person_id)
- **Parse-Only Mode**: Test CSV parsing without database connection using `--no-db`

## Usage

```bash
./panel_ingestion_app <panels.csv> [--no-db] [--batch=N] [--bad-records=file.csv]
```

### Options

- `<panels.csv>` - Path to the panel CSV file (required)
- `--no-db` - Skip database insertion (parse and display only)
- `--batch=N` - Batch size for DB insertion (default: 5000)
- `--bad-records=FILE` - Save rejected records to CSV file

### Examples

**1. Parse-only mode (test CSV without database):**
```bash
./panel_ingestion_app search_opinioncluster_panel-2025-07-02.csv --no-db
```

**2. Load data and save bad records:**
```bash
./panel_ingestion_app search_opinioncluster_panel-2025-07-02.csv \
  --bad-records=bad_panel_records.csv
```

**3. Load with custom batch size:**
```bash
./panel_ingestion_app search_opinioncluster_panel-2025-07-02.csv \
  --batch=10000 \
  --bad-records=bad_panel_records.csv
```

## How It Works

1. **Load CSV**: Reads all records from the CSV file
2. **Load Valid IDs**: Queries database for all valid `opinioncluster_id` values from `search_opinioncluster` table
3. **Validate**: Filters records by checking if `opinioncluster_id` exists in the valid ID set
4. **Insert**: Inserts valid records in batches
5. **Output Bad Records**: Writes rejected records (FK violations) to the bad records file

## Bad Records Output

The bad records CSV file contains:
- `id` - Panel record ID
- `opinioncluster_id` - The cluster ID that failed validation
- `person_id` - Person ID
- `reason` - Description of why the record was rejected (e.g., "FK violation: opinioncluster_id=2541571 not found in search_opinioncluster")

### Example Bad Records File:
```csv
id,opinioncluster_id,person_id,reason
3,2541571,202,"FK violation: opinioncluster_id=2541571 not found in search_opinioncluster"
5,999999,204,"FK violation: opinioncluster_id=999999 not found in search_opinioncluster"
```

## Database Schema

The target table structure:
```sql
Table "public.search_opinioncluster_panel"
      Column       |  Type   | Nullable
-------------------+---------+----------
 id                | integer | not null
 opinioncluster_id | integer | not null
 person_id         | integer | not null

Indexes:
    "search_opinioncluster_panel_pkey" PRIMARY KEY, btree (id)
    "search_opinioncluster_panel_opinioncluster_id_judge_id_key" UNIQUE CONSTRAINT, btree (opinioncluster_id, person_id)

Foreign-key constraints:
    "opinioncluster_id_7cdb36cb8a6ff7a7_fk_search_opinioncluster_id" 
        FOREIGN KEY (opinioncluster_id) REFERENCES search_opinioncluster(id)
    "search_opinio_person_id_70c55c02599cc568_fk_people_db_person_id" 
        FOREIGN KEY (person_id) REFERENCES people_db_person(id)
```

## Building

The panel ingestion app is built as part of the main CMake build:

```bash
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug ..
cmake --build .
```

This creates the `panel_ingestion_app` executable.

## Architecture

The implementation consists of:

- **opinion_cluster_panel.h/cpp** - CSV parsing and data structures
- **opinion_cluster_panel_db.h/cpp** - Database operations with FK validation
- **panel_main.cpp** - Main application logic

The code follows the same pattern as the existing `opinion` and `opinion_cluster` ingestion tools but is simplified for the straightforward CSV structure.

## Error Handling

- Parse errors are reported but don't stop processing
- FK violations are tracked and output to the bad records file
- Database errors are reported with context
- Duplicate IDs are handled with `ON CONFLICT DO NOTHING`

## Performance

- Loads all valid cluster IDs once at startup (cached in memory)
- O(1) lookup for FK validation using `std::set`
- Batch inserts reduce database round-trips
- Simple CSV parsing (no complex quote handling needed for integer-only data)
