# OMOP Server

This directory contains a modular, extensible ETL framework for transforming healthcare data into the OMOP Common Data Model (CDM) using Python, SQLAlchemy, and advanced concept mapping. The system is designed for robust, high-performance data integration with SQL Server and other RDBMS backends.

---

## 📁 Directory Structure

```
omop_server/
│
├── models/         # SQLAlchemy ORM models and ETL logic for OMOP tables
│   ├── basetable.py
│   ├── person_id.py
│   ├── person.py
│   ├── visit_occurrence.py
│   ├── condition_occurrence.py
│   ├── procedure_occurrence.py
│   ├── observation_period.py
│   ├── concept_builder.py
│   ├── concept_relationship.py
│   ├── config.json
│   └── README.md
│
├── sql/            # Raw SQL scripts for table creation, validation, and data quality checks 
│   ├── person.sql   # Not included in public repository for HIPAA purposes
│   ├── visit_occurrence.sql
│   ├── observation_period.sql
│   ├── functions.sql
│   └── README.md
│
├── utils/          # Utility modules for DB connections, concept mapping, config management, etc.
│   ├── utility.py
│   ├── connect.py
│   ├── transformerconceptmapper.py
│   ├── concept_mapper.py
│   ├── config_manager.py
│   ├── utility.py
│   └── README.md
│
├── requirements.txt
├── pyproject.toml
├── setup.py
├── .env
└── __init__.py
```

---

## 🏗️ Key Components

### Models (`models/`)
- **ParentBase**: Abstract base class for all OMOP tables, providing configuration, connection, and ETL utilities.
- **OMOP Table Models**: `person.py`, `visit_occurrence.py`, `condition_occurrence.py`, `procedure_occurrence.py`, `observation_period.py` implement OMOP CDM tables with batch ETL logic.
- **Concept/Vocabulary Models**: `concept_builder.py`, `concept_relationship.py` for loading and managing OMOP vocabularies.
- **Config**: `config.json` defines source-to-OMOP mappings and validation rules.

### SQL Scripts (`sql/`)
- Table creation scripts for OMOP CDM tables (optimized for SQL Server).
- Data validation and quality check queries.
- Reference data/vocabulary loaders.

### Utilities (`utils/`)
- **Database Utilities**: Secure engine creation, connection pooling, error handling.
- **ConceptMapper**: Advanced semantic mapping from source values to OMOP concepts (supports transformer models).
- **ConfigManager**: Loads and validates ETL configuration.
- **General Utilities**: Helper functions for SQL formatting, logging, etc.

---

## 🚀 Quick Start

1. **Clone the repository**
```git
   git clone https://github.com/SiddharthRajesh2003/OMOP_server.git
```

1. **Setup Virtual Environment**
   ```bash
   python -m venv venv
   ./venv/Scripts/activate
   ```

1. **Install dependencies**
   ```bash
   pip install .
   ```

2. **Configure environment**
   - Create a `.env` file with your DB credentials:
     ```
     UID=your_db_user
     PWD=your_db_password
     DB_URL=your_db_url
     DB_NAME=your_db_name
     ```

3. **Set up config**
   - Edit `models/config.json` to map your source columns to OMOP fields.

4. **Run ETL**
   - Example (Python):
     ```python
     from omop_server.models.visit_occurrence import VisitOccurrence
     visit = VisitOccurrence(schema='Target Schema')
     visit.set_config('config.json')
     visit.set_source('Source Schema', 'Source Table')
     engine = visit.connect_db(os.getenv('DB_URL'), os.getenv('DB_NAME'))
     visit.create_table(engine)
     visit.insert_records(engine, batch_size=20000)
     ```

5. **Use SQL scripts**
   - Run scripts in `sql/` for manual table creation or validation as needed.

---

## ⚙️ Features

- **Modular ETL**: Each OMOP table is a class with its own ETL logic.
- **Batch Processing**: Efficient, memory-safe batch inserts with fallback strategies.
- **Semantic Concept Mapping**: Uses transformer models for mapping source values to OMOP concepts.
- **Validation**: Built-in source and OMOP compliance validation.
- **Logging & Monitoring**: Detailed progress, error, and performance logging.
- **SQL Server Optimizations**: Fast inserts, NOT ENFORCED constraints, efficient pagination.

---

## 📦 Dependencies

- `sqlalchemy`
- `pandas`
- `python-dotenv`
- `transformers`
- `pyodbc` (for SQL Server)
- See `requirements.txt` for full list.

---

## 📚 Documentation

- [models/README.md](models/README.md): Full details on ETL architecture, configuration, and usage.
- [utils/README.md](utils/README.md): Utility functions and advanced concept mapping.
- [sql/README.md](sql/README.md): SQL script usage and best practices.

---

## 🤝 Contributing

- Fork the repo and submit pull requests.
- Follow PEP 8 and add docstrings/type hints.
- Add tests for new features.


* This ETL framework is designed to streamline OMOP CDM implementation and ensure data quality in