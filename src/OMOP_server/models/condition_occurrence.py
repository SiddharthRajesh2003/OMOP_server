import datetime as dt
import pandas as pd
import os
from typing import Any
import logging


from sqlalchemy.exc import ProgrammingError
from sqlalchemy import text, Column, Integer, Date, DateTime, VARCHAR
from sqlalchemy.orm import sessionmaker


from OMOP_server.utils.utility import sql_val
from OMOP_server.models.person import Person
from OMOP_server.utils.concept_mapper import ConceptMapper
from OMOP_server.models.basetable import ParentBase
from OMOP_server.utils.logging_manager import get_logger

logger=get_logger(__name__)

class ConditionOccurrence(ParentBase):
    """
    Represents the OMOP Condition Occurrence table and provides methods for ETL operations.

    - Inherits from ParentBase for SQLAlchemy ORM mapping.
    - Defines all OMOP fields as SQLAlchemy columns.
    - Allows dynamic schema assignment and source table configuration.
    - Validates source columns against configuration.
    - Supports custom table creation for SQL Server, including NOT ENFORCED constraints.
    - Handles efficient batch and sub-batch record insertion with error recovery.
    - Integrates with ConceptMapper for semantic mapping of condition concepts and status.
    - Logs progress, errors, and performance metrics throughout the ETL process.
    """
    
    __tablename__ = 'OMOP_Condition_Occurrence'
    __source_schema__ = None
    __source_table__ = None
    __config_manager__ = None
    
    condition_occurrence_id = Column(Integer, nullable=False, primary_key=True, autoincrement=False)
    person_id = Column(Integer, nullable=False)
    condition_concept_id = Column(Integer, nullable=False)
    condition_start_date = Column(Date, nullable=False)
    condition_start_datetime = Column(DateTime, nullable=True)
    condition_end_date = Column(Date, nullable=False)
    condition_end_datetime = Column(DateTime, nullable=True)
    condition_type_concept_id = Column(Integer, nullable=False)
    condition_status_concept_id = Column(Integer, nullable=True)
    stop_reason = Column(VARCHAR, nullable=True)
    provider_id = Column(Integer, nullable=True)
    visit_occurrence_id = Column(Integer, nullable=True)
    visit_detail_id = Column(Integer, nullable=True)
    condition_source_value = Column(VARCHAR, nullable=True)
    condition_source_concept_id = Column(Integer, nullable=True)
    condition_status_source_value = Column(VARCHAR, nullable=True)
    
    
    def __init__(self, schema):
        self.__table__.schema=schema
    
    @classmethod
    def validate_source_columns(cls, engine) -> bool:
        """
        Validate that all required columns exist in the source table.
        
        Args:
            engine: SQLAlchemy engine
            
        Returns:
            bool: True if all columns exist, False otherwise
        """
        if not cls.__source_schema__ or not cls.__source_table__:
            raise ValueError("Source schema and table must be set before validation")
        
        config=cls.get_config()
        errors = config.validate_config('condition_occurrence')
        if errors:
            for error in errors:
                logging.info('Errors found\n',error)
        required_columns=config.get_all_columns('condition_occurrence')
        
        check_column=f'''
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE
        TABLE_SCHEMA='{cls.__source_schema__}' AND TABLE_NAME = '{cls.__source_table__}'
        AND COLUMN_NAME IN ({','.join([f"'{col}'" for col in required_columns])})
        '''
        session=sessionmaker(bind=engine)()
        try:
            result=session.execute(text(check_column))
            existing_columns=[row[0] for row in result.fetchall()]

            missing_columns=set(required_columns)-set(existing_columns)
            
            if missing_columns:
                logging.info(f"Missing columns in source table: {','.join(missing_columns)}")
                return False
            else:
                logging.info("All required columns found in source table")
                return True
        except Exception as e:
            logging.error(f'Error validating source columns: {e}')
            return False
        finally:
            session.close()
    
    @staticmethod
    def create_table(engine) -> None:
        """
        Custom table creation that handles SQL Server's constraint limitations
        """
        schema = ConditionOccurrence.__table__.schema
        if not schema:
            raise ValueError("Schema must be set before creating the table.")
        
        # Check if table already exists
        check_table_sql = f"""
        SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{ConditionOccurrence.__tablename__}'
        """
        
        table_creation_sql = f"""
        CREATE TABLE [{schema}].[{ConditionOccurrence.__tablename__}] (
            condition_occurrence_id BIGINT NOT NULL,
            person_id BIGINT NOT NULL,
            condition_concept_id BIGINT NOT NULL,
            condition_start_date DATE NOT NULL,
            condition_start_datetime DATETIME NULL,
            condition_end_date DATE NOT NULL,
            condition_end_datetime DATETIME NULL,
            condition_type_concept_id INT NOT NULL,
            condition_status_concept_id INT NULL,
            stop_reason VARCHAR(20) NULL,
            provider_id BIGINT NULL,
            visit_occurrence_id BIGINT NULL,
            visit_detail_id BIGINT NULL,
            condition_source_value VARCHAR(50) NULL,
            condition_source_concept_id BIGINT NULL,
            condition_status_source_value VARCHAR(50) NULL
            );
        """
        
        # Add NOT ENFORCED primary key constraint
        pk_constraint_sql = f"""
        ALTER TABLE [{schema}].[{ConditionOccurrence.__tablename__}]
        ADD CONSTRAINT PK_{ConditionOccurrence.__tablename__}_condition_occurrence_id 
        PRIMARY KEY NONCLUSTERED (condition_occurrence_id) NOT ENFORCED
        """
        
        try:
            with engine.connect() as conn:
                result = conn.execute(text(check_table_sql))
                table_exists = result.scalar() > 0
                
                if not table_exists:
                    conn.execute(text(table_creation_sql))
                    logging.info(f"Table {schema}.{ConditionOccurrence.__tablename__} created successfully.")
                    
                    conn.execute(text(pk_constraint_sql))
                    logging.info(f"Primary key constraint added to {schema}.{ConditionOccurrence.__tablename__}")
                else:
                    logging.info(f"Table {schema}.{ConditionOccurrence.__tablename__} already exists.")
                    
        except Exception as e:
            logging.error(f"Error creating table: {e}")
            raise
    
    @staticmethod
    def print_mapping_summary(df, source_col, concept_col, mapping_type="Concept"):
        """
        Print a summary of the mapping results
        
        Args:
            df: DataFrame with mapped data
            source_col: Name of the source column
            concept_col: Name of the concept ID column  
            mapping_type: Type of mapping (e.g., "Condition", "Status")
        """
        print(f"\n=== {mapping_type.upper()} MAPPING SUMMARY ===")
        print(f"Source Column: {source_col}")
        print(f"Concept Column: {concept_col}")
        print(f"Total Records: {len(df)}")
        
        if concept_col in df.columns:
            mapped_count = df[concept_col].notna().sum()
            unmapped_count = df[concept_col].isna().sum() + (df[concept_col] == 0).sum()
            
            print(f"Successfully Mapped: {mapped_count}")
            print(f"Unmapped (NULL/0): {unmapped_count}")
            print(f"Mapping Rate: {(mapped_count/len(df)*100):.1f}%")
            
            # Show mapping distribution
            concept_counts = df[concept_col].value_counts().head(10)
            print(f"\nTop 10 Concept IDs:")
            print(concept_counts.to_string())
            
            # Show sample mappings
            print(f"\nSample Mappings:")
            sample = df[[source_col, concept_col]].drop_duplicates().head(10)
            print(sample.to_string(index=False))
        else:
            print(f"Column {concept_col} not found in DataFrame")
                
    @staticmethod
    def insert_records(engine, all_records=False, total=1000000, batch_size=100000, sub_batch_size=10000) -> Any:
        """
        Insert records into the OMOP_Condition_Occurrence table.
        
        Args:
            engine: SQLAlchemy engine
            all_records (bool): If True, insert all available records. If False, insert up to 'total' records.
            total (int): Maximum number of records to insert when all_records=False
            batch_size (int): Number of records to process in each batch
            sub_batch_size (int): Number of records to process in each sub-batch for fallback insert
        
        Returns:
            int: Number of records successfully inserted
        """
        schema = ConditionOccurrence.__table__.schema
        table_name = ConditionOccurrence.__tablename__
        Person.__table__.schema = schema
        session = sessionmaker(bind=engine)()
        starttime = dt.datetime.now()
        logging.info(f'Insert started at: {starttime.strftime("%Y-%m-%d %H:%M:%S")}')
        
        Person.__table__.schema = ConditionOccurrence.__table__.schema
        config = ConditionOccurrence.get_config()
        config_errors = config.validate_config('condition_occurrence')
        if config_errors:
            raise ValueError(f"Configuration Errors: {','.join(config_errors)}")
        source_columns = config.get_source_columns('condition_occurrence')
        logging.info(source_columns)
        total_records = None
        
        try:
            condition_mapper = ConceptMapper(engine=engine, schema=ConditionOccurrence.__table__.schema,
                                        domain='Condition', concept_table_name='OMOP_Condition_Concepts')
            logging.info(f'Loading condition concepts lookup data...')
            condition_concepts = condition_mapper.get_concept_lookups()
            
            logging.info(f'Loaded {len(condition_concepts)} condition concepts for mapping')
            
        except Exception as e:
            logging.error(f'Error initializing concept mappers: {e}')
            logging.info('Falling back to original mapping functions...')
            # If concept mapper initialization fails, fall back to original functions
            condition_mapper = None
            
        try:
            condition_status_mapper = ConceptMapper(engine=engine, schema=ConditionOccurrence.__table__.schema,
                                        domain='Condition Status', concept_table_name='OMOP_Condition_Status_Concepts')
            logging.info(f'Loading condition status concepts lookup data...')
            condition_status_concepts = condition_status_mapper.get_concept_lookups()
            
            logging.info(f'Loaded {len(condition_status_concepts)} condition status concepts for mapping')
            
        except Exception as e:
            logging.error(f'Error initializing condition status concept mappers: {e}')
            logging.info('Falling back to original mapping functions...')
            # If concept mapper initialization fails, fall back to original functions
            condition_status_mapper = None
            
        
        try:
            # Get total records available
            count_sql = f'''
            SELECT COUNT(*) FROM [{ConditionOccurrence.__source_schema__}].[{ConditionOccurrence.__source_table__}] c
            INNER JOIN [{Person.__table__.schema}].[{Person.__tablename__}] p
            ON c.{source_columns['person_id']} = p.person_id
            WHERE c.{source_columns['condition_occurrence_id']} IS NOT NULL
            AND c.{source_columns['person_id']} IS NOT NULL
            AND c.{source_columns['condition_start_date']} IS NOT NULL
            AND c.{source_columns['condition_start_date']} > '1930-01-01'
            AND c.{source_columns['visit_occurrence_id']} IS NOT NULL
            '''
            
            available_records = session.execute(text(count_sql)).scalar()
            logging.info(f'Available records in source: {available_records}')

            if available_records == 0:
                logging.info('No records available to insert')
                return 0

            # Set total_records based on all_records parameter
            if all_records:
                total_records = available_records
                logging.info(f'Processing ALL records: {total_records}')
            else:
                total_records = min(total, available_records)
                logging.info(f'Processing {total_records} records (requested: {total}, available: {available_records})')

            # Get already inserted IDs to avoid duplicates
            existing_ids_sql = f'''
            SELECT condition_occurrence_id FROM [{schema}].[{table_name}]
            '''
            try:
                existing_result = session.execute(text(existing_ids_sql)).fetchall()
                existing_ids = {row[0] for row in existing_result}
                logging.info(f'Found {len(existing_ids)} existing condition occurrence records to skip')
            except:
                existing_ids = set()
                logging.info('No existing records found (table might be empty)')
            
            # Adjust count for existing records
            if existing_ids:
                ids_list = ','.join(map(str, existing_ids))
                adjusted_count_sql = f'''
                SELECT COUNT(*) FROM [{ConditionOccurrence.__source_schema__}].[{ConditionOccurrence.__source_table__}] c
                INNER JOIN [{Person.__table__.schema}].[{Person.__tablename__}] p
                ON c.{source_columns['person_id']} = p.person_id
                WHERE c.{source_columns['condition_occurrence_id']} IS NOT NULL
                AND c.{source_columns['person_id']} IS NOT NULL
                AND c.{source_columns['condition_start_date']} IS NOT NULL
                AND c.{source_columns['condition_start_date']} > '1930-01-01'
                AND c.{source_columns['visit_occurrence_id']} IS NOT NULL
                AND c.{source_columns['condition_occurrence_id']} NOT IN ({ids_list})
                '''
                available_new_records = session.execute(text(adjusted_count_sql)).scalar()
                logging.info(f'Available new records to insert: {available_new_records}')
                
                if available_new_records == 0:
                    logging.info('No new records to insert')
                    return 0
                
                # Adjust total_records if needed
                if all_records:
                    total_records = available_new_records
                else:
                    total_records = min(total_records, available_new_records)
            
            total_inserted = 0
            batch_num = 1
            last_condition_occurrence_id = None
            consecutive_empty_batches = 0

            while total_inserted < total_records:
                logging.info(f'\n--- Processing Batch {batch_num} ---')
                logging.info(f'Progress: {total_inserted}/{total_records} ({(total_inserted/total_records)*100:.1f}%)')
                
                # Calculate remaining records needed
                remaining_needed = total_records - total_inserted
                current_batch_size = min(batch_size, remaining_needed)
                
                # Build query with pagination using condition_occurrence_id
                base_where = f'''
                c.{source_columns['condition_occurrence_id']} IS NOT NULL
                AND c.{source_columns['condition_start_date']} IS NOT NULL
                AND c.{source_columns['condition_start_date']} > '1930-01-01'
                AND c.{source_columns['person_id']} IS NOT NULL
                AND c.{source_columns['visit_occurrence_id']} IS NOT NULL
                '''
                
                # Add exclusion for existing IDs if any
                if len(existing_ids) > 30000:
                        base_where = f'''
                        c.{source_columns['condition_occurrence_id']} IS NOT NULL
                        AND c.{source_columns['condition_start_date']} IS NOT NULL
                        AND c.{source_columns['condition_start_date']} > '1930-01-01'
                        AND c.{source_columns['person_id']} IS NOT NULL
                        AND c.{source_columns['visit_occurrence_id']} IS NOT NULL
                        AND NOT EXISTS (
                            SELECT 1 FROM [{schema}].[{table_name}] existing 
                            WHERE existing.condition_occurrence_id = c.{source_columns['condition_occurrence_id']}
                    )
                    '''
                else:
                    if existing_ids:
                        ids_list = ','.join(map(str, existing_ids))
                        base_where += f" AND c.{source_columns['condition_occurrence_id']} NOT IN ({ids_list})"
                
                # Add pagination condition
                if last_condition_occurrence_id is not None:
                    base_where += f" AND c.{source_columns['condition_occurrence_id']} > {last_condition_occurrence_id}"
                
                sql = f'''
                SELECT TOP {current_batch_size} 
                    c.{source_columns['condition_occurrence_id']},
                    c.{source_columns['person_id']},
                    c.{source_columns['condition_start_date']},
                    c.{source_columns['condition_end_date']},
                    c.{source_columns['provider_id']},
                    c.{source_columns['visit_occurrence_id']},
                    c.{source_columns['condition_source_value']},
                    c.{source_columns['condition_status_source_value']}
                FROM [{ConditionOccurrence.__source_schema__}].[{ConditionOccurrence.__source_table__}] c
                INNER JOIN [{Person.__table__.schema}].[{Person.__tablename__}] p
                ON c.{source_columns['person_id']} = p.person_id
                WHERE {base_where}
                ORDER BY c.{source_columns['condition_occurrence_id']}
                '''
                
                result = session.execute(text(sql)).fetchall()
                
                # Check if we got any results
                if not result:
                    consecutive_empty_batches += 1
                    logging.info(f'No more records found. Empty batch #{consecutive_empty_batches}')
                    if consecutive_empty_batches >= 3:
                        logging.info('Multiple consecutive empty batches detected. Ending processing.')
                        break
                    batch_num += 1
                    continue
                else:
                    consecutive_empty_batches = 0
                
                logging.info(f'Fetched {len(result)} raw records from database')
                
                if condition_mapper is not None:
                    batch_df = pd.DataFrame(result, columns=[
                        'condition_occurrence_id', 'person_id', 'condition_start_date',
                        'condition_end_date', 'provider_id', 'visit_occurrence_id',
                        'condition_source_value', 'condition_status_source_value'
                    ])
                    
                    logging.info(f'Mapping condition concepts using Condition Concept Mapper...')
                    try:
                        condition_mapped = condition_mapper.map_source_to_concept(
                            source_df=batch_df,
                            source_column='condition_source_value',
                            mapping_strategy='semantic',
                            concept_df=condition_concepts
                        )
                        batch_df['condition_concept_id'] = condition_mapped['concept_id']
                        # Add this to print the results
                        print(f"\n=== CONDITION CONCEPT MAPPING RESULTS ===")
                        mapped_records = condition_mapped[condition_mapped['concept_id'].notna()]
                        
                        print(f"Successfully mapped {len(mapped_records)} out of {len(batch_df)} records")
                        print("\nSample mappings:")
                        print("-" * 80)
                        
                        sample_df = mapped_records[['condition_source_value','matched_concept_name', 'concept_id']].head(10)
                        for idx, row in sample_df.iterrows():
                            print(f"Source: '{row['condition_source_value']}'")
                            print(f"  -> Matched Concept Name: '{row['matched_concept_name']}'")
                            print(f"  -> Concept ID: {row['concept_id']}")
                        
                        
                        
                        # Show unmapped values
                        unmapped = batch_df[batch_df['condition_concept_id'].isna()]
                        if len(unmapped) > 0:
                            print(f"\nUnmapped values ({len(unmapped)} records):")
                            unique_unmapped = unmapped['condition_source_value'].unique()[:5]
                            for val in unique_unmapped:
                                print(f"  - '{val}'")
                    except Exception as e:
                        logging.error(f'Error mapping condition concepts: {e}')
                        # Fallback to default value
                        batch_df['condition_concept_id'] = 0
                
                if condition_status_mapper is not None:
                    if condition_mapper is None:
                        batch_df = pd.DataFrame(result, columns=[
                            'condition_occurrence_id', 'person_id', 'condition_start_date',
                            'condition_end_date', 'provider_id', 'visit_occurrence_id',
                            'condition_source_value', 'condition_status_source_value'
                        ])
                    
                    logging.info(f'Mapping condition status concepts using Condition Status Concept Mapper...')
                    try:
                        status_mapped = condition_status_mapper.map_source_to_concept(
                            source_df=batch_df,
                            source_column='condition_status_source_value',
                            mapping_strategy='semantic',
                            concept_df=condition_status_concepts,
                            model_name='UFNLP/gatortron-base'
                        )
                        batch_df['condition_status_concept_id'] = status_mapped['concept_id']
                        logging.info(f'Successfully mapped {status_mapped["concept_id"].notna().sum()} condition status concepts')
                    except Exception as e:
                        logging.error(f'Error mapping condition status concepts: {e}')
                        batch_df['condition_status_concept_id'] = 0
                
                # Handle case where neither mapper is available
                if condition_mapper is None and condition_status_mapper is None:
                    batch_df = pd.DataFrame(result, columns=[
                        'condition_occurrence_id', 'person_id', 'condition_start_date',
                        'condition_end_date', 'provider_id', 'visit_occurrence_id',
                        'condition_source_value', 'condition_status_source_value'
                    ])
                    batch_df['condition_concept_id'] = 0
                    batch_df['condition_status_concept_id'] = 0
                    
                # Fill NaN values with 0
                batch_df['condition_concept_id'] = batch_df['condition_concept_id'].fillna(0)
                batch_df['condition_status_concept_id'] = batch_df['condition_status_concept_id'].fillna(0)
                                    
                condition_occurrences = []
                processed_in_batch = 0
                
                for idx, row in enumerate(result):
                    if row[0] is None or row[0] in existing_ids:
                        continue

                    # Stop if we've reached our target (only for non-all_records mode)
                    if not all_records and total_inserted + len(condition_occurrences) >= total_records:
                        logging.info(f'Reached target limit. Stopping at {total_inserted + len(condition_occurrences)} records.')
                        break
                    
                    condition_occurrence_id = row[0]
                    person_id = row[1]
                    condition_start_datetime = row[2]
                    condition_end_datetime = row[3]
                    provider_id = row[4]
                    visit_occurrence_id = row[5]
                    condition_source_value = row[6]
                    condition_status_source_value = row[7]
                    
                    # Handle date conversion
                    if isinstance(row[2], str):
                        condition_start_date = dt.datetime.fromisoformat(row[2]).date()
                    else:
                        condition_start_date = row[2].date()

                    if row[3] is not None:
                        if isinstance(row[3], str):
                            condition_end_date = dt.datetime.fromisoformat(row[3]).date()
                        else:
                            condition_end_date = row[3].date()
                    else:
                        condition_end_date = None

                    if condition_mapper is not None or condition_status_mapper is not None:
                        condition_concept_id = int(batch_df.iloc[idx]['condition_concept_id'])
                        condition_status_concept_id = int(batch_df.iloc[idx]['condition_status_concept_id'])
                    else:
                        logging.warning(f'Concept Mappers not initialized, using default values')
                        condition_concept_id = 0
                        condition_status_concept_id = 0
                    
                    co = ConditionOccurrence(schema=ConditionOccurrence.__table__.schema)
                    co.condition_occurrence_id = condition_occurrence_id
                    co.person_id = person_id
                    co.condition_concept_id = condition_concept_id
                    co.condition_start_date = condition_start_date
                    co.condition_start_datetime = condition_start_datetime
                    co.condition_end_date = condition_end_date
                    co.condition_end_datetime = condition_end_datetime
                    co.condition_type_concept_id = 32817
                    co.condition_status_concept_id = condition_status_concept_id
                    co.stop_reason = None
                    co.provider_id = provider_id
                    co.visit_occurrence_id = visit_occurrence_id
                    co.visit_detail_id = None
                    co.condition_source_value = condition_source_value
                    co.condition_source_concept_id = 0
                    co.condition_status_source_value = condition_status_source_value

                    condition_occurrences.append(co)
                    processed_in_batch += 1
                
                logging.info(f'Created {len(condition_occurrences)} condition occurrence objects')
                
                if not condition_occurrences:
                    logging.info(f'No valid condition occurrences found in batch {batch_num}, skipping...')
                    batch_num += 1
                    continue
                
                try:
                    session.add_all(condition_occurrences)
                    session.commit()
                    logging.info(f'✓ Successfully inserted batch {batch_num}: {len(condition_occurrences)} records')
                    total_inserted += len(condition_occurrences)
                    
                    # Add inserted IDs to existing_ids set to prevent future duplicates
                    for co in condition_occurrences:
                        existing_ids.add(co.condition_occurrence_id)
                
                except ProgrammingError as e:
                    session.rollback()
                    if 'DEFAULT VALUES' in str(e) or 'Incorrect syntax near' in str(e):
                        logging.info(f'Bulk insert failed for batch {batch_num}. Using alternative insert method...')
                        
                        success = 0
                        for i in range(0, len(condition_occurrences), sub_batch_size):
                            sub_batch = condition_occurrences[i:i+sub_batch_size]
                            
                            valid_sub_batch = [obj for obj in sub_batch if obj.person_id and obj.condition_occurrence_id is not None]              
                            if not valid_sub_batch:
                                continue
                            
                            try:
                                select_parts = []
                                for obj in valid_sub_batch:
                                    select_parts.append(
                                        f"SELECT CAST({sql_val(obj.condition_occurrence_id)} AS BIGINT) AS condition_occurrence_id,"
                                        f"CAST({sql_val(obj.person_id)} AS BIGINT) AS person_id,"
                                        f"CAST({sql_val(obj.condition_concept_id)} AS BIGINT) AS condition_concept_id,"
                                        f"CAST({sql_val(obj.condition_start_date)} AS DATE) AS condition_start_date, "
                                        f"CAST({sql_val(obj.condition_start_datetime)} AS DATETIME) AS condition_start_datetime,"
                                        f"CAST({sql_val(obj.condition_end_date)} AS DATE) AS condition_end_date,"
                                        f"CAST({sql_val(obj.condition_end_datetime)} AS DATETIME) AS condition_end_datetime,"
                                        f"CAST({sql_val(obj.condition_type_concept_id)} AS INT) AS condition_type_concept_id,"
                                        f"CAST({sql_val(obj.condition_status_concept_id)} AS BIGINT) AS condition_status_concept_id,"
                                        f"CAST({sql_val(obj.stop_reason)} AS VARCHAR) AS stop_reason,"
                                        f"CAST({sql_val(obj.provider_id)} AS BIGINT) AS provider_id,"
                                        f"CAST({sql_val(obj.visit_occurrence_id)} AS BIGINT) AS visit_occurrence_id,"
                                        f"CAST({sql_val(obj.visit_detail_id)} AS BIGINT) AS visit_detail_id,"
                                        f"CAST({sql_val(obj.condition_source_value)} AS VARCHAR) AS condition_source_value,"
                                        f"CAST({sql_val(obj.condition_source_concept_id)} AS BIGINT) AS condition_source_concept_id,"
                                        f"CAST({sql_val(obj.condition_status_source_value)} AS VARCHAR) AS condition_status_source_value"
                                    )
                                    
                                select_str = " UNION ALL ".join(select_parts)
                                
                                insert_sql = f"""
                                INSERT INTO [{schema}].[{table_name}] (
                                    [condition_occurrence_id], [person_id], [condition_concept_id],
                                    [condition_start_date], [condition_start_datetime], [condition_end_date],
                                    [condition_end_datetime], [condition_type_concept_id], [condition_status_concept_id],
                                    [stop_reason], [provider_id], [visit_occurrence_id], [visit_detail_id],
                                    [condition_source_value], [condition_source_concept_id], [condition_status_source_value]
                                    ) {select_str}"""
                                
                                session.execute(text(insert_sql))
                                success += len(valid_sub_batch)
                                
                            except Exception as sub_e:
                                logging.info(f'Sub-batch insert failed, trying individual inserts: {sub_e}')
                                
                                for obj in valid_sub_batch:
                                    if obj.condition_occurrence_id is None or obj.person_id is None:
                                        continue
                                    try:
                                        individual_insert_sql = f"""
                                        INSERT INTO [{schema}].[{table_name}] (
                                        [condition_occurrence_id], [person_id], [condition_concept_id],
                                        [condition_start_date], [condition_start_datetime], [condition_end_date],
                                        [condition_end_datetime], [condition_type_concept_id], [condition_status_concept_id],
                                        [stop_reason], [provider_id], [visit_occurrence_id], [visit_detail_id],
                                        [condition_source_value], [condition_source_concept_id], [condition_status_source_value]
                                        ) VALUES (
                                        {sql_val(obj.condition_occurrence_id)}, {sql_val(obj.person_id)}, {sql_val(obj.condition_concept_id)},
                                        {sql_val(obj.condition_start_date)}, {sql_val(obj.condition_start_datetime)}, {sql_val(obj.condition_end_date)},
                                        {sql_val(obj.condition_end_datetime)}, {sql_val(obj.condition_type_concept_id)}, {sql_val(obj.condition_status_concept_id)},
                                        {sql_val(obj.stop_reason)}, {sql_val(obj.provider_id)}, {sql_val(obj.visit_occurrence_id)},
                                        {sql_val(obj.visit_detail_id)}, {sql_val(obj.condition_source_value)}, {sql_val(obj.condition_source_concept_id)},
                                        {sql_val(obj.condition_status_source_value)})"""
                                        
                                        session.execute(text(individual_insert_sql))
                                        success += 1
                                    
                                    except Exception as ind_e:
                                        logging.info(f'Failed to insert condition_occurrence_id {obj.condition_occurrence_id}: {ind_e}')
                                        continue
                        
                        if success > 0:
                            session.commit()
                            logging.info(f'Successfully inserted {success} out of {len(condition_occurrences)} records in batch {batch_num}')
                            total_inserted += success
                            
                            # Add inserted IDs to existing_ids set to prevent future duplicates
                            for co in condition_occurrences[:success]:
                                existing_ids.add(co.condition_occurrence_id)
                        else:
                            session.rollback()
                            logging.info(f'Failed to insert any records in batch {batch_num}, rolling back...')
                            raise Exception(f'No records could be inserted in this batch')
                    else:
                        logging.info(f'Non-bulk insert error in batch {batch_num}: {e}')
                        raise

                # Update pagination marker - use the LAST condition_occurrence_id from this batch
                if condition_occurrences:
                    last_condition_occurrence_id = condition_occurrences[-1].condition_occurrence_id
                
                batch_num += 1
                
                # Check exit condition for non-all_records mode
                if not all_records and total_inserted >= total_records:
                    logging.info(f'Reached target of {total_records} records. Stopping.')
                    break
                    
            logging.info(f'\n=== FINAL RESULTS ===')
            logging.info(f'Successfully inserted {total_inserted} records into {ConditionOccurrence.table_name()}')
            if all_records:
                logging.info(f'Target was: {total_records} records')
                logging.info(f'Completion: {(total_inserted/total_records)*100:.1f}%' if total_records and total_records > 0 else 'N/A')
            else:
                logging.info(f'Target was: {total_records} records')
                logging.info(f'Completion: {(total_inserted/total_records)*100:.1f}%' if total_records and total_records > 0 else 'N/A')
            
            logging.info(f'Mode: {"All available records" if all_records else f"Limited to {total_records}"}')
            return total_inserted
                            

        except Exception as e:
            session.rollback()
            logging.error(f'Error inserting records into {ConditionOccurrence.table_name()}: {e}')
            raise
        finally:
            session.close()
            endtime = dt.datetime.now()
            logging.info(f'Insert completed at: {endtime.strftime("%Y-%m-%d %H:%M:%S")}')
            duration = endtime - starttime
            total_seconds = int(duration.total_seconds())
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            duration = dt.time(hour=hours, minute=minutes, second=seconds)
            logging.info(f'Total time taken: {duration.strftime("%H:%M:%S")}')
            logging.info('Session Closed')



if __name__ == "__main__":
    co = ConditionOccurrence('Your Target Schema')
    current=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path=os.path.join(current,'config.json')
    co.set_config(config_path)
    co.set_source('Source Schema','Source Table')
    engine = co.connect_db(os.getenv('DB_URL'), os.getenv('DB_NAME'))
    co.drop_table(engine)
    co.create_table(engine)
    co.validate_source_columns(engine)
    co.insert_records(engine, all_records = False, total=100000, batch_size=10000, sub_batch_size=1000)