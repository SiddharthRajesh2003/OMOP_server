# Utils Directory

Collection of utility functions and classes that support the OMOP ETL process.

## 🔧 Core Components

### Database Utilities
- **Engine Creation**: Manages database connections with secure credential handling
- **Connection Pooling**: Optimizes database connection management
- **Error Handling**: Robust error handling for database operations

### ConceptMapper
Advanced concept mapping utility that:
- Maps source values to OMOP concept_ids
- Uses transformer models for semantic matching
- Supports multiple mapping strategies:
  - Exact matching
  - Semantic similarity
  - Fuzzy matching
  - Custom mapping rules

## ⚙️ Configuration

### Environment Variables (.env)
Required variables:
```ini
UID=database_username
PWD=database_password
DB_URL=server_url
DB_NAME=database_name
```

### Usage Example
```python
from utils.db_utils import create_engine
from utils.concept_mapper import ConceptMapper

# Create database engine
engine = create_engine()

# Initialize concept mapper
mapper = ConceptMapper(
    engine=engine,
    schema='Target Schema',
    domain='Visit',
    concept_table_name='OMOP_Visit_Concepts'
)

# Map concepts
mapped_concepts = mapper.map_source_to_concept(
    source_df=data,
    source_column='visit_type',
    mapping_strategy='semantic'
)
```

## 🔍 Key Functions

### Database Utilities
- `create_engine()`: Creates SQLAlchemy engine with connection pooling
- `test_connection()`: Validates database connectivity
- `sql_val()`: Safely formats SQL values

### Concept Mapping
- `map_source_to_concept()`: Maps source values to standard concepts
- `get_concept_lookups()`: Retrieves concept reference data
- `compute_semantic_similarity()`: Calculates text similarity scores

## 📦 Dependencies

```txt
sqlalchemy>=1.4.0
python-dotenv>=0.19.0
transformers>=2.0.0
numpy>=1.21.0
pandas>=1.3.0
```

## 🚀 Performance Considerations

- Uses connection pooling for optimal database performance
- Caches concept lookups to minimize database calls
- Batches concept mapping operations for efficiency
- Implements fallback strategies for failed mappings

## 📝 Logging

Comprehensive logging of:
- Database connection events
- Concept mapping results
- Performance metrics
- Error conditions and recovery attempts

---

*For more details about the overall ETL process, see the main README in the models directory.*