# OMOP ETL Models

A comprehensive SQLAlchemy-based ETL framework for transforming healthcare data into the OMOP Common Data Model (CDM) format. This system provides robust data loading, validation, and transformation capabilities for healthcare analytics and research, with advanced concept mapping and batch processing features.

## 🏗️ Architecture Overview

The ETL framework follows a modular architecture built around a common inheritance pattern with SQL Server optimization:

```
ParentBase (basetable.py)
    ├── Person_ID (Test Case table to check if the ETL works with your DB server)
    ├── Person
    ├── VisitOccurrence  
    ├── ConditionOccurrence
    ├── ProcedureOccurrence
    ├── ObservationPeriod
    ├── ConceptBuilder (Requires the csv files downloaded from Athena)
    └── ConceptRelationship
```

### Core Components

- **ParentBase Class** (`basetable.py`)
  - Abstract base class providing common ETL functionality
  - Configuration management and database connectivity
  - Schema validation and source table mapping
  - Batch processing and error handling utilities
  - Logging and monitoring infrastructure
  - SQL Server specific optimizations

- **Advanced Features**
  - Semantic concept mapping using ConceptMapper
  - Intelligent batch processing with fallback strategies
  - Duplicate detection and prevention
  - Progress tracking and performance monitoring
  - Configurable retry mechanisms

## 📊 OMOP Table Models

### Clinical Data Models
- **`person_id.py`** - Patient Idenifier
- **`person.py`** - Patient demographics, identifiers, and basic information
- **`visit_occurrence.py`** - Healthcare encounters, admissions, and visit details
- **`condition_occurrence.py`** - Diagnoses, medical conditions, and problems
- **`procedure_occurrence.py`** - Medical procedures, interventions, and treatments
- **`observation_period.py`** - Continuous patient observation windows

### Vocabulary & Concept Models
- **`concept_builder.py`** - Standardized vocabulary concepts and terminology
- **`concept_relationship.py`** - Hierarchical and associative concept relationships

## 🚀 Quick Start Guide

### 1. Installation

```bash
pip install -r requirements.txt or
conda install --file requirements.txt
```

### 2. Environment Setup

Create a `.env` file in your project root:

```env
# Database Configuration
UID=your_database_user
PWD=your_database_password
DB_URL=your_database_connection_url
DB_NAME=target_omop_database

```

### 3. Configuration File

Create `config.json` with your source-to-OMOP mappings:

```json
{
    "tables":{
        "person_id":{
            "source_columns":{
                "person_id":"personid"
            },
            "required_columns":[
                    "personid"
            ],
            "optional_columns":[],
            "column_mappings":{
                "personid":{
                    "omop field": "person_id",
                    "data type": "int",
                    "nullable": false,
                    "description": "Unique Indentifier for person"
                }
            }
        }
    }
}

```

### 4. Basic Usage Example

```python
import os
from dotenv import load_dotenv
from OMOP_server.models.visit_occurrence import VisitOccurrence

# Load environment variables
load_dotenv()

# Initialize model with target schema
visit_model = VisitOccurrence(schema='your target schema')

# Configure the model
visit_model.set_config('config.json')
visit_model.set_source('Source schema', 'Source encounter table')

# Establish database connection
engine = visit_model.connect_db(
    db_url=os.getenv('DB_URL'),
    db_name=os.getenv('DB_NAME')
)

# Execute ETL process
try:
    # Validate source columns exist
    if not visit_model.validate_source_columns(engine):
        raise ValueError("Source validation failed")
    
    # Create target table with SQL Server optimizations
    visit_model.create_table(engine)
    
    # Load data with advanced batch processing
    total_inserted = visit_model.insert_records(
        engine=engine,
        all_records=True,  # Process all available records
        batch_size=20000,  # Primary batch size
        sub_batch_size=5000  # Fallback batch size
    )
    
    print(f"ETL process completed successfully! Inserted {total_inserted} records")
    
except Exception as e:
    print(f"ETL process failed: {e}")
    # Handle cleanup if needed
    visit_model.drop_table(engine)
```

### 5. Advanced Processing Example

```python
# Advanced ETL with concept mapping and monitoring
from OMOP_server.models.visit_occurrence import VisitOccurrence
from OMOP_server.utils.concept_mapper import ConceptMapper

# Initialize with enhanced configuration
visit_model = VisitOccurrence(schema='your target schema')
visit_model.set_config('config.json')
visit_model.set_source('Source schema', 'Source encounter table')

engine = visit_model.connect_db(os.getenv('DB_URL'), os.getenv('DB_NAME'))

# Process with intelligent concept mapping
try:
    # The system automatically uses ConceptMapper for semantic mapping
    # Falls back to hardcoded mapping functions if concept mapping fails
    
    total_records = visit_model.insert_records(
        engine=engine,
        all_records=False,  # Process limited records
        total=100000,      # Maximum records to process
        batch_size=25000,   # Optimize for your system
        sub_batch_size=10000
    )
    
    print(f"Processing complete: {total_records} records inserted")
    
except Exception as e:
    logger.error(f"ETL failed: {e}")
    raise
```

## 🔧 Advanced Configuration

### Batch Processing Configuration

```python
# Or configure programmatically
visit_model.insert_records(
    engine=engine,
    all_records=True,              # Process all available records
    batch_size=25000,              # Override config batch size
    sub_batch_size=10000           # Override config sub-batch size
)
```

### Error Handling and Recovery

```python
# The system includes multiple fallback strategies:
# 1. Primary: SQLAlchemy ORM batch insert
# 2. Fallback 1: Raw SQL batch insert with UNION ALL
# 3. Fallback 2: Individual record inserts

```

### SQL Server Optimizations

```python
# Built-in SQL Server optimizations include:
# - NOT ENFORCED primary key constraints
# - Efficient pagination using ORDER BY
# - Batch INSERT with proper type casting
# - Connection pooling and session management

# Table creation with SQL Server specific features
visit_model.create_table(engine)
# Creates table with:
# - BIGINT columns for IDs
# - DATETIME columns for timestamps
# - VARCHAR columns with appropriate lengths
# - NOT ENFORCED constraints for performance
```

## 📋 Core Methods Reference

### ParentBase Methods

| Method | Description | Parameters | Returns |
|--------|-------------|------------|---------|
| `set_config(config_path)` | Loads configuration from JSON file | `config_path`: Path to config file | Status message |
| `get_config()` | Gets configuration manager instance | None | ConfigManager instance |
| `set_source(schema, table)` | Sets source table information | `schema`: Source schema<br>`table`: Source table | Status message |
| `get_source_fullname()` | Gets full source name | None | `"schema.table"` string |
| `connect_db(db_url, db_name)` | Establishes database connection | `db_url`: Connection URL<br>`db_name`: Database name | SQLAlchemy engine |
| `table_name()` | Gets formatted table name | None | `"[schema].[table]"` |
| `query(engine, query)` | Executes SQL query | `engine`: SQLAlchemy engine<br>`query`: SQL string | pandas DataFrame |
| `drop_table(engine)` | Removes existing table | `engine`: SQLAlchemy engine | None |

### VisitOccurrence Specific Methods

| Method | Description | Parameters | Returns |
|--------|-------------|------------|---------|
| `validate_source_columns(engine)` | Validates source table structure | `engine`: SQLAlchemy engine | Boolean |
| `create_table(engine)` | Creates OMOP table with SQL Server optimizations | `engine`: SQLAlchemy engine | None |
| `insert_records(engine, **kwargs)` | Advanced batch processing with concept mapping | See detailed parameters below | Integer (records inserted) |

### insert_records Parameters

```python
insert_records(
    engine,                    # SQLAlchemy engine (required)
    all_records=False,         # Process all available records
    total=1000000,            # Max records when all_records=False
    batch_size=100000,        # Primary batch size
    sub_batch_size=10000      # Fallback batch size for error handling
)
```

### Advanced Features in insert_records

- **Duplicate Prevention**: Automatically skips existing records
- **Concept Mapping**: Uses ConceptMapper for semantic mapping with fallback
- **Progress Tracking**: Detailed logging with batch statistics
- **Error Recovery**: Multiple fallback strategies for failed inserts
- **Memory Management**: Efficient batch processing with pagination
- **Referential Integrity**: Validates person_id references


## 🔍 Data Validation Features

### Built-in Source Validation

```python
# Validate source columns before processing
if not visit_model.validate_source_columns(engine):
    print("Source validation failed - check required columns")
    
# The system validates:
# - Required columns exist in source table
# - Column data types match expectations
# - Source table accessibility
```

### OMOP Compliance Validation

```python
# Built-in OMOP CDM validations include:
# - visit_start_datetime before visit_end_datetime
# - person_id references valid person records
# - Required fields are not null
# - visit_occurrence_id uniqueness
# - Proper date formats and ranges

# Validation happens automatically during insert_records()
```

### Data Quality Checks

```python
# Automatic data quality monitoring:
# - Duplicate detection and prevention
# - Referential integrity checks
# - Date logic validation
# - Concept mapping success rates
# - Batch processing statistics
```

## 📊 Monitoring and Logging

### Built-in Progress Tracking

```python
# The system provides detailed logging automatically:
# - Batch processing progress with percentages
# - Record counts (processed, inserted, skipped, errors)
# - Performance metrics and timing
# - Concept mapping success rates
# - Error details and recovery actions

# Example log output:
# 2024-01-15 10:30:15 - INFO - Insert started at: 2024-01-15 10:30:15
# 2024-01-15 10:30:16 - INFO - Available records in source: 150000
# 2024-01-15 10:30:16 - INFO - Processing ALL records: 150000
# 2024-01-15 10:30:17 - INFO - Found 1250 existing visit occurrence records to skip
# 2024-01-15 10:30:18 - INFO - --- Processing Batch 1 ---
# 2024-01-15 10:30:18 - INFO - Progress: 0/148750 (0.0%)
# 2024-01-15 10:30:25 - INFO - Successfully mapped 19850 visit types
# 2024-01-15 10:30:26 - INFO - ✓ Successfully inserted batch 1: 20000 records
```

### Performance Monitoring

```python
# Automatic performance tracking includes:
# - Total processing time
# - Records per second
# - Memory usage patterns
# - Database connection efficiency
# - Batch processing effectiveness

# Example performance summary:
# 2024-01-15 10:45:30 - INFO - Successfully inserted 148750 records
# 2024-01-15 10:45:30 - INFO - Total time taken: 00:15:15
# 2024-01-15 10:45:30 - INFO - Average processing rate: 163 records/second
```

### Error Tracking and Recovery

```python
# Comprehensive error handling with:
# - Detailed error messages and context
# - Automatic retry mechanisms
# - Graceful fallback strategies
# - Failed record logging
# - Recovery action documentation

# Example error handling:
# 2024-01-15 10:35:20 - ERROR - Bulk insert failed for batch 5
# 2024-01-15 10:35:20 - INFO - Using alternative insert method...
# 2024-01-15 10:35:25 - INFO - Successfully inserted 18500 out of 20000 records
# 2024-01-15 10:35:25 - INFO - 1500 records failed validation and were skipped
```

## 🔧 Performance Optimization

### Database Connection Optimization

```python
# Built-in optimizations include:
# - Connection pooling for efficient resource usage
# - Session management with proper cleanup
# - Batch processing to minimize roundtrips
# - Parameterized queries for security and performance
```

### Memory Management

```python
# Efficient memory usage through:
# - Pagination-based record processing
# - Batch size optimization
# - Automatic session cleanup
# - DataFrame memory optimization

# Memory-conscious processing example:
visit_model.insert_records(
    engine=engine,
    batch_size=10000,      # Smaller batches for memory constraints
    sub_batch_size=2000    # Further reduce for error recovery
)
```

### SQL Server Performance Features

```python
# SQL Server specific optimizations:
# - NOT ENFORCED constraints for faster inserts
# - Efficient ORDER BY pagination
# - Type-safe CAST operations
# - Optimized UNION ALL statements for fallback inserts

# Example of generated optimized SQL:
# SELECT TOP 20000 [columns] FROM [source] 
# WHERE conditions AND visit_occurrence_id > last_id
# ORDER BY visit_occurrence_id
```

### Batch Processing Optimization

```python
# Intelligent batch processing includes:
# - Adaptive batch sizing based on system performance
# - Duplicate detection to avoid unnecessary processing
# - Concept mapping caching for repeated lookups
# - Efficient pagination to handle large datasets

# Performance tuning guidelines:
# - Batch size: 20,000-50,000 for most systems
# - Sub-batch size: 5,000-10,000 for error recovery
# - Enable concept mapping caching for repeated runs
```

## 🐛 Troubleshooting Guide

### Common Issues and Solutions

1. **Source Column Validation Failures**
   ```python
   # Check if required columns exist
   if not visit_model.validate_source_columns(engine):
       config = visit_model.get_config()
       required_cols = config.get_all_columns('visit_occurrence')
       print(f"Required columns: {required_cols}")
       
   # Verify source table accessibility
   result = visit_model.query(engine, 
       f"SELECT TOP 5 * FROM [{source_schema}].[{source_table}]")
   ```

2. **Concept Mapping Issues**
   ```python
   # Check if ConceptMapper is working
   try:
       from OMOP_server.utils.concept_mapper import ConceptMapper
       mapper = ConceptMapper(engine=engine, schema=schema, domain='Visit')
       concepts = mapper.get_concept_lookups()
       print(f"Loaded {len(concepts)} concepts")
   except Exception as e:
       print(f"ConceptMapper failed: {e}")
       print("System will fall back to hardcoded mapping functions")
   ```

3. **Batch Processing Failures**
   ```python
   # If large batches fail, reduce batch size
   visit_model.insert_records(
       engine=engine,
       batch_size=5000,     # Smaller batch size
       sub_batch_size=1000  # Smaller sub-batches
   )
   ```

4. **Memory Issues**
   ```python
   # Monitor memory usage and adjust batch sizes
   # Check system resources before processing
   import psutil
   memory_percent = psutil.virtual_memory().percent
   if memory_percent > 80:
       print("High memory usage detected - reducing batch size")
       batch_size = 5000
   ```

5. **Performance Issues**
   ```python
   # Check database connection performance
   import time
   start = time.time()
   result = visit_model.query(engine, "SELECT COUNT(*) FROM Source schema.source_table")
   end = time.time()
   print(f"Query took {end-start:.2f} seconds")
   
   # If slow, check:
   # - Network connectivity
   # - Database server load
   # - Index availability on source tables
   ```

### Debug Mode and Logging

```python
# Enable detailed logging for debugging
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# The system logs detailed information about:
# - SQL queries being executed
# - Batch processing progress
# - Concept mapping results
# - Error details and recovery actions
# - Performance metrics
```

### Common Error Messages

- **"Source schema and table must be set"**: Call `set_source()` before validation
- **"Schema must be set before creating table"**: Initialize model with schema parameter
- **"Configuration Errors"**: Check config.json format and required fields
- **"No records available to insert"**: Verify source table has data and required columns
- **"Bulk insert failed"**: Normal - system automatically uses fallback methods

## 📦 Dependencies

### Required Packages

```txt
sqlalchemy>=1.4.0
pandas>=1.3.0
python-dotenv>=0.19.0
transformers>=2.0.0
```

### Optional Packages

```txt
psycopg2-binary>=2.9.0    # PostgreSQL support
pyodbc>=4.0.0             # SQL Server support
mysql-connector-python>=8.0.0  # MySQL support
tqdm>=4.62.0              # Progress bars
```

## 🤝 Contributing

### Development Setup

```bash
# Clone repository
git clone <repository_url>
cd omop-etl-models

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/
```

### Code Style

- Follow PEP 8 guidelines
- Use type hints for all public methods
- Add docstrings for all classes and methods
- Include unit tests for new features

## 📞 Support

For questions, issues, or contributions:
- Create an issue in the GitHub repository
- Contact the development team
- Check the documentation wiki

---

*This ETL framework is designed to streamline OMOP CDM implementation and ensure data quality in healthcare analytics workflows.*