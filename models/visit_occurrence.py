import pandas as pd
import datetime as dt
import os
import logging
from typing import Any


from sqlalchemy import text, Column, Integer, Date, DateTime, VARCHAR
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError


from omop_server.utils.utility import sql_val, map_visit_type, map_admit, map_discharge
from omop_server.models.person import Person
from omop_server.utils.concept_mapper import ConceptMapper
from omop_server.models.basetable import ParentBase
from omop_server.utils.logging_manager import get_logger

logger=get_logger(__name__)


class VisitOccurrence(ParentBase):

    """
    Represents the OMOP Visit Occurrence entity for the ETL process.

    This class defines the schema and ETL logic for the OMOP_Visit_Occurrence table,
    which stores standardized information about each healthcare visit or encounter
    for persons in the OMOP Common Data Model. It includes methods for creating the table,
    mapping and transforming source encounter data, and inserting visit occurrence records in batches.
    The VisitOccurrence class ensures that all required and optional fields are populated
    according to OMOP CDM standards and supports referential integrity with related tables.
    Utility functions are provided to facilitate the mapping of visit types, admission and discharge sources,
    and to manage efficient ETL operations for large datasets.
    """
    
    __tablename__ = 'OMOP_Visit_Occurrence'
    __source_schema__ = None
    __source_table__ = None
    __config_manager__=None
    
    visit_occurrence_id = Column(Integer, autoincrement=False, primary_key=True,nullable=False)
    person_id = Column(Integer, nullable=False)
    visit_concept_id =Column(Integer, nullable=False)
    visit_start_date = Column(Date, nullable=False)
    visit_start_datetime = Column(DateTime, nullable=True)
    visit_end_date = Column(Date, nullable=True)
    visit_end_datetime = Column(DateTime, nullable=True)
    visit_type_concept_id = Column(Integer, nullable=False)
    provider_id = Column(Integer, nullable=True)
    care_site_id = Column(Integer, nullable=True)
    visit_source_value = Column(VARCHAR(50), nullable=True)
    visit_source_concept_id =Column(Integer, nullable=True)
    admitted_from_concept_id = Column(Integer, nullable=True)
    admitted_from_source_value = Column(VARCHAR(50), nullable=True)
    discharged_to_concept_id = Column(Integer, nullable=True)
    discharged_to_source_value = Column(VARCHAR(50), nullable=True)
    preceding_visit_occurrence_id = Column(Integer, nullable=True)
    
    def __init__(self, schema) -> None:
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
        errors = config.validate_config('visit_occurrence')
        if errors:
            for error in errors:
                logging.info('Errors found\n',error)
        required_columns=config.get_all_columns('visit_occurrence')
        
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
        schema = VisitOccurrence.__table__.schema
        if not schema:
            raise ValueError("Schema must be set before creating the table.")
        
        # Check if table already exists
        check_table_sql = f"""
        SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{VisitOccurrence.__tablename__}'
        """
        
        table_creation_sql = f"""
        CREATE TABLE [{schema}].[{VisitOccurrence.__tablename__}] (
            visit_occurrence_id BIGINT NOT NULL,
            person_id BIGINT NOT NULL,
            visit_concept_id BIGINT NOT NULL,
            visit_start_date DATE NOT NULL,
            visit_start_datetime DATETIME NULL,
            visit_end_date DATE NULL,
            visit_end_datetime DATETIME NULL,
            visit_type_concept_id BIGINT NOT NULL,
            provider_id BIGINT NULL,
            care_site_id BIGINT NULL,
            visit_source_value VARCHAR(50) NULL,
            visit_source_concept_id BIGINT NULL,
            admitted_from_concept_id BIGINT NULL,
            admitted_from_source_value VARCHAR(50) NULL,
            discharged_to_concept_id BIGINT NULL,
            discharged_to_source_value VARCHAR(50) NULL,
            preceding_visit_occurrence_id BIGINT NULL
            );
        """
        
        # Add NOT ENFORCED primary key constraint
        pk_constraint_sql = f"""
        ALTER TABLE [{schema}].[{VisitOccurrence.__tablename__}]
        ADD CONSTRAINT PK_{VisitOccurrence.__tablename__}_visit_occurrence_id 
        PRIMARY KEY NONCLUSTERED (visit_occurrence_id) NOT ENFORCED
        """
        
        try:
            with engine.connect() as conn:
                result = conn.execute(text(check_table_sql))
                table_exists = result.scalar() > 0
                
                if not table_exists:
                    conn.execute(text(table_creation_sql))
                    logging.info(f"Table {schema}.{VisitOccurrence.__tablename__} created successfully.")
                    
                    conn.execute(text(pk_constraint_sql))
                    logging.info(f"Primary key constraint added to {schema}.{VisitOccurrence.__tablename__}")
                else:
                    logging.info(f"Table {schema}.{VisitOccurrence.__tablename__} already exists.")
                    
        except Exception as e:
            logging.error(f"Error creating table: {e}")
            raise  

    @staticmethod
    def insert_records(engine, all_records=False, total=1000000, batch_size=100000, sub_batch_size=10000) -> Any:
        """
        Insert records into the OMOP_Visit_Occurrence table.
        
        Args:
            engine: SQLAlchemy engine
            all_records (bool): If True, insert all available records. If False, insert up to 'total' records.
            total (int): Maximum number of records to insert when all_records=False
            batch_size (int): Number of records to process in each batch
            sub_batch_size (int): Number of records to process in each sub-batch for fallback insert
        
        Returns:
            int: Number of records successfully inserted
        """
        session = sessionmaker(bind=engine)()
        starttime = dt.datetime.now()
        logging.info(f'Insert started at: {starttime.strftime("%Y-%m-%d %H:%M:%S")}')
        
        schema = VisitOccurrence.__table__.schema
        table_name = VisitOccurrence.__tablename__
        Person.__table__.schema = VisitOccurrence.__table__.schema
        config = VisitOccurrence.get_config()
        config_errors = config.validate_config('visit_occurrence')
        if config_errors:
            raise ValueError(f"Configuration Errors: {','.join(config_errors)}")
        source_columns = config.get_source_columns('visit_occurrence')
        logging.info(source_columns)
        
        try:
            visit_mapper = ConceptMapper(engine = engine, schema = VisitOccurrence.__table__.schema,
                                        domain = 'Visit', concept_table_name= 'OMOP_Visit_Concepts')
            logging.info(f'Loading visit concepts lookup data...')
            visit_concepts = visit_mapper.get_concept_lookups()
            
            logging.info(f'Loaded {len(visit_concepts)} visit concepts for mapping')
            
        except Exception as e:
            logging.error(f'Error initializing concept mappers: {e}')
            logging.info('Falling back to original mapping functions...')
            # If concept mapper initialization fails, fall back to original functions
            visit_mapper = None

        try:
            # Get total records available
            count_sql = f'''
            SELECT COUNT(*) FROM [{VisitOccurrence.__source_schema__}].[{VisitOccurrence.__source_table__}] v
            INNER JOIN [{Person.__table__.schema}].[{Person.__tablename__}] p
            ON v.{source_columns['person_id']} = p.person_id
            WHERE v.{source_columns['visit_occurrence_id']} IS NOT NULL
            AND v.{source_columns['visit_start_datetime']} IS NOT NULL
            AND v.{source_columns['visit_end_datetime']} IS NOT NULL
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
            SELECT visit_occurrence_id FROM [{VisitOccurrence.__table__.schema}].[{VisitOccurrence.__tablename__}]
            '''
            try:
                existing_result = session.execute(text(existing_ids_sql)).fetchall()
                existing_ids = {row[0] for row in existing_result}
                logging.info(f'Found {len(existing_ids)} existing visit occurrence records to skip')
            except:
                existing_ids = set()
                logging.info('No existing records found (table might be empty)')
            
            # Adjust count for existing records
            if existing_ids:
                ids_list = ','.join(map(str, existing_ids))
                adjusted_count_sql = f'''
                SELECT COUNT(*) FROM [{VisitOccurrence.__source_schema__}].[{VisitOccurrence.__source_table__}] v
                INNER JOIN [{Person.__table__.schema}].[{Person.__tablename__}] p
                ON v.{source_columns['person_id']} = p.person_id
                WHERE v.{source_columns['visit_occurrence_id']} IS NOT NULL
                AND v.{source_columns['visit_start_datetime']} IS NOT NULL
                AND v.{source_columns['visit_end_datetime']} IS NOT NULL
                AND v.{source_columns['visit_occurrence_id']} NOT IN ({ids_list})
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
            last_visit_occurrence_id = None
            person_last_visit = {}
            consecutive_empty_batches = 0

            while total_inserted < total_records:
                logging.info(f'\n--- Processing Batch {batch_num} ---')
                logging.info(f'Progress: {total_inserted}/{total_records} ({(total_inserted/total_records)*100:.1f}%)')
                
                # Calculate remaining records needed
                remaining_needed = total_records - total_inserted
                current_batch_size = min(batch_size, remaining_needed)
                
                # Build query with pagination using visit_occurrence_id
                base_where = f'''
                v.{source_columns['visit_occurrence_id']} IS NOT NULL
                AND v.{source_columns['visit_start_datetime']} IS NOT NULL
                AND v.{source_columns['visit_end_datetime']} IS NOT NULL
                '''
                
                # Add exclusion for existing IDs if any
                if len(existing_ids)>30000:
                    base_where = f'''
                    v.{source_columns['visit_occurrence_id']} IS NOT NULL
                    AND v.{source_columns['visit_start_datetime']} IS NOT NULL
                    AND v.{source_columns['visit_end_datetime']} IS NOT NULL
                    AND NOT EXISTS (
                                SELECT 1 FROM [{schema}].[{table_name}] existing 
                                WHERE existing.visit_occurrence_id = v.{source_columns['visit_occurrence_id']}
                    )'''
                else:
                    if existing_ids:
                        ids_list = ','.join(map(str, existing_ids))
                        base_where += f" AND v.{source_columns['visit_occurrence_id']} NOT IN ({ids_list})"
                
                # Add pagination condition
                if last_visit_occurrence_id is not None:
                    base_where += f" AND v.{source_columns['visit_occurrence_id']} > {last_visit_occurrence_id}"
                
                sql = f'''
                SELECT TOP {current_batch_size} 
                    v.{source_columns['visit_occurrence_id']},
                    v.{source_columns['person_id']},
                    v.{source_columns['visit_source_value']},
                    v.{source_columns['visit_start_datetime']},
                    v.{source_columns['visit_end_datetime']},
                    v.{source_columns['care_site_id']},
                    v.{source_columns['admitted_from']},
                    v.{source_columns['discharged_to']}
                FROM [{VisitOccurrence.__source_schema__}].[{VisitOccurrence.__source_table__}] v
                INNER JOIN [{Person.__table__.schema}].[{Person.__tablename__}] p
                ON v.{source_columns['person_id']} = p.person_id
                WHERE {base_where}
                ORDER BY v.{source_columns['visit_occurrence_id']}
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
                
                if visit_mapper is not None:
                    batch_df = pd.DataFrame(result, columns = [
                        'visit_occurrence_id','person_id', 'visit_source_value',
                        'visit_start_datetime', 'visit_end_datetime', 'care_site_id',
                        'admitted_from', 'discharged_to'
                    ])
                    
                    logging.info('Mapping visit concepts using Visit Concept Mapper...')
                    try:
                        visit_mapped = visit_mapper.map_source_to_concept(
                            source_df = batch_df,
                            source_column='visit_source_value',
                            mapping_strategy='semantic',
                            concept_df = visit_concepts
                        )
                        batch_df['visit_concept_id'] = visit_mapped['concept_id']
                        logging.info(f'Successfully mapped {visit_mapped["concept_id"].notna().sum()} visit types')

                    except Exception as e:
                        logging.error(f'Error in visit type mapping: {e}')
                        # Fallback to original function
                        batch_df['visit_concept_id'] = batch_df['visit_source_value'].apply(map_visit_type)

                    logging.info('Mapping admission source values using Concept Mapper...')
                    try:
                        admitted_mapped = visit_mapper.map_source_to_concept(
                            source_df = batch_df,
                            source_column='admitted_from',
                            mapping_strategy='semantic',
                            concept_df = visit_concepts
                        )
                        batch_df['admitted_from_concept_id'] = admitted_mapped['concept_id']
                        logging.info(f'Successfully mapped {admitted_mapped["concept_id"].notna().sum()} admission sources')

                    except Exception as e:
                        logging.error(f'Error in admission source mapping: {e}')
                        # Fallback to original function
                        batch_df['admitted_from_concept_id'] = batch_df['admitted_from'].apply(map_admit)
                        
                    
                    logging.info('Mapping discharge source values using Concept Mapper...')
                    try:
                        discharge_mapped = visit_mapper.map_source_to_concept(
                            source_df = batch_df,
                            source_column='discharged_to',
                            mapping_strategy='semantic',
                            concept_df = visit_concepts
                        )
                        batch_df['discharged_to_concept_id'] = discharge_mapped['concept_id']
                        logging.info(f'Successfully mapped {discharge_mapped["concept_id"].notna().sum()} discharge destinations')

                    except Exception as e:
                        logging.error(f'Error in discharge destination mapping: {e}')
                        # Fallback to original function
                        batch_df['discharged_to_concept_id'] = batch_df['discharged_to'].apply(map_discharge)
                        
                    batch_df['visit_concept_id'] = batch_df['visit_concept_id'].fillna(0)
                    batch_df['admitted_from_concept_id'] = batch_df['admitted_from_concept_id'].fillna(0)
                    batch_df['discharged_to_concept_id'] = batch_df['discharged_to_concept_id'].fillna(0)
                
                encounters = []
                processed_in_batch = 0
                
                for idx, row in enumerate(result):
                    if row[0] is None or row[0] in existing_ids:
                        continue

                    # Stop if we've reached our target (only for non-all_records mode)
                    if not all_records and total_inserted + len(encounters) >= total_records:
                        logging.info(f'Reached target limit. Stopping at {total_inserted + len(encounters)} records.')
                        break

                    visit_occurrence_id = row[0]
                    person_id = row[1]
                    visit_start_datetime = row[3]
                    visit_end_datetime = row[4]
                    visit_source_value = row[2]
                    
                    if visit_mapper is not None:
                        visit_concept_id = int(batch_df.iloc[idx]['visit_concept_id'])
                        admitted_from_concept_id = int(batch_df.iloc[idx]['admitted_from_concept_id'])
                        discharged_to_concept_id = int(batch_df.iloc[idx]['discharged_to_concept_id'])
                    else:
                        # Fallback to original mapping functions
                        visit_concept_id = map_visit_type(visit_source_value)
                        admitted_from_concept_id = map_admit(row[6])
                        discharged_to_concept_id = map_discharge(row[7])

                    # Get preceding visit occurrence ID for this person
                    preceding_visit_occurrence_id = person_last_visit.get(person_id)
                    
                    # Update the last visit for this person
                    person_last_visit[person_id] = visit_occurrence_id

                    # Handle date conversion
                    if isinstance(row[3], str):
                        visit_start_date = dt.datetime.fromisoformat(row[3]).date()
                    else:
                        visit_start_date = row[3].date()

                    if isinstance(row[4], str):
                        visit_end_date = dt.datetime.fromisoformat(row[4]).date()
                    else:
                        visit_end_date = row[4].date()

                    vo = VisitOccurrence(schema=VisitOccurrence.__table__.schema)
                    vo.visit_occurrence_id = visit_occurrence_id
                    vo.person_id = person_id
                    vo.visit_concept_id = visit_concept_id
                    vo.visit_start_date = visit_start_date
                    vo.visit_start_datetime = visit_start_datetime
                    vo.visit_end_date = visit_end_date
                    vo.visit_end_datetime = visit_end_datetime
                    vo.visit_type_concept_id = 32827
                    vo.provider_id = None
                    vo.care_site_id = row[5] or ''
                    vo.visit_source_value = visit_source_value
                    vo.visit_source_concept_id = None
                    vo.admitted_from_concept_id = admitted_from_concept_id
                    vo.admitted_from_source_value = row[6] or ''
                    vo.discharged_to_concept_id = discharged_to_concept_id
                    vo.discharged_to_source_value = row[7] or ''
                    vo.preceding_visit_occurrence_id = preceding_visit_occurrence_id

                    encounters.append(vo)
                    processed_in_batch += 1

                logging.info(f'Created {len(encounters)} visit occurrence objects')

                if not encounters:
                    logging.info(f'No valid encounters found in batch {batch_num}, skipping...')
                    batch_num += 1
                    continue

                try:
                    session.add_all(encounters)
                    session.commit()
                    logging.info(f'✓ Successfully inserted batch {batch_num}: {len(encounters)} records')
                    total_inserted += len(encounters)
                    
                    # Add inserted IDs to existing_ids set to prevent future duplicates
                    for encounter in encounters:
                        existing_ids.add(encounter.visit_occurrence_id)
                
                except ProgrammingError as e:
                    session.rollback()
                    if 'DEFAULT VALUES' in str(e) or 'Incorrect syntax near' in str(e):
                        logging.info(f'Bulk insert failed for batch {batch_num}. Using alternative insert method...')
                    
                        success = 0
                        for i in range(0, len(encounters), sub_batch_size):
                            sub_batch = encounters[i:i+sub_batch_size]
                            
                            valid_sub_batch = [obj for obj in sub_batch if obj.person_id and obj.visit_occurrence_id is not None]              
                            if not valid_sub_batch:
                                continue
                            
                            try:
                                select_parts = []
                                for obj in valid_sub_batch:
                                    select_parts.append(
                                        f"SELECT CAST({sql_val(obj.visit_occurrence_id)} AS BIGINT) AS visit_occurrence_id,"
                                        f"CAST({sql_val(obj.person_id)} AS BIGINT) AS person_id, "
                                        f"CAST({sql_val(obj.visit_concept_id)} AS BIGINT) AS visit_concept_id, "
                                        f"CAST({sql_val(obj.visit_start_date)} AS DATE) AS visit_start_date, "
                                        f"CAST({sql_val(obj.visit_start_datetime)} AS DATETIME) AS visit_start_datetime, "
                                        f"CAST({sql_val(obj.visit_end_date)} AS DATE) AS visit_end_date, "
                                        f"CAST({sql_val(obj.visit_end_datetime)} AS DATETIME) AS visit_end_datetime, "
                                        f"CAST({sql_val(obj.visit_type_concept_id)} AS BIGINT) AS visit_type_concept_id, "
                                        f"CAST({sql_val(obj.provider_id)} AS BIGINT) AS provider_id, "
                                        f"CAST({sql_val(obj.care_site_id)} AS BIGINT) AS care_site_id, "
                                        f"CAST({sql_val(obj.visit_source_value)} AS VARCHAR) AS visit_source_value, "
                                        f"CAST({sql_val(obj.visit_source_concept_id)} AS BIGINT) AS visit_source_concept_id, "
                                        f"CAST({sql_val(obj.admitted_from_concept_id)} AS BIGINT) AS admitted_from_concept_id, "
                                        f"CAST({sql_val(obj.admitted_from_source_value)} AS VARCHAR) AS admitted_from_source_value, "
                                        f"CAST({sql_val(obj.discharged_to_concept_id)} AS BIGINT) AS discharged_to_concept_id, "
                                        f"CAST({sql_val(obj.discharged_to_source_value)} AS VARCHAR) AS discharged_to_source_value, "
                                        f"CAST({sql_val(obj.preceding_visit_occurrence_id)} AS BIGINT) AS preceding_visit_occurrence_id"
                                    )
                                    
                                select_str = " UNION ALL ".join(select_parts)
                                
                                insert_sql = f"""
                                INSERT INTO [{VisitOccurrence.__table__.schema}].[{VisitOccurrence.__tablename__}](
                                    [visit_occurrence_id], [person_id], [visit_concept_id], [visit_start_date],
                                    [visit_start_datetime], [visit_end_date], [visit_end_datetime], [visit_type_concept_id],
                                    [provider_id], [care_site_id], [visit_source_value], [visit_source_concept_id],
                                    [admitted_from_concept_id], [admitted_from_source_value], [discharged_to_concept_id], 
                                    [discharged_to_source_value], [preceding_visit_occurrence_id])
                                    {select_str}
                                """
                                
                                session.execute(text(insert_sql))
                                success += len(valid_sub_batch)
                                
                            except Exception as sub_e:
                                logging.info(f'Sub-batch insert failed, trying individual inserts: {sub_e}')
                                
                                for obj in valid_sub_batch:
                                    if obj.visit_occurrence_id and obj.person_id is None:
                                        continue
                                    try:
                                        individual_insert_sql = f"""
                                        INSERT INTO [{VisitOccurrence.__table__.schema}].[{VisitOccurrence.__tablename__}](
                                        [visit_occurrence_id], [person_id], [visit_concept_id], [visit_start_date],
                                        [visit_start_datetime], [visit_end_date], [visit_end_datetime], [visit_type_concept_id],
                                        [provider_id], [care_site_id], [visit_source_value], [visit_source_concept_id],
                                        [admitted_from_concept_id], [admitted_from_source_value], [discharged_to_concept_id], 
                                        [discharged_to_source_value], [preceding_visit_occurrence_id])
                                        VALUES 
                                        ({sql_val(obj.visit_occurrence_id)}, {sql_val(obj.person_id)},
                                        {sql_val(obj.visit_concept_id)}, {sql_val(obj.visit_start_date)},
                                        {sql_val(obj.visit_start_datetime)}, {sql_val(obj.visit_end_date)},
                                        {sql_val(obj.visit_end_datetime)}, {sql_val(obj.visit_type_concept_id)},
                                        {sql_val(obj.provider_id)}, {sql_val(obj.care_site_id)},
                                        {sql_val(obj.visit_source_value)}, {sql_val(obj.visit_source_concept_id)},
                                        {sql_val(obj.admitted_from_concept_id)},{sql_val(obj.admitted_from_source_value)},
                                        {sql_val(obj.discharged_to_concept_id)}, {sql_val(obj.discharged_to_source_value)},
                                        {sql_val(obj.preceding_visit_occurrence_id)})
                                        """
                                        session.execute(text(individual_insert_sql))
                                        success += 1
                                    
                                    except Exception as ind_e:
                                        logging.info(f'Failed to insert visit_occurrence_id {obj.visit_occurrence_id}: {ind_e}')
                                        continue
                                    
                        if success > 0:
                            session.commit()
                            logging.info(f'Successfully inserted {success} out of {len(encounters)} records in batch {batch_num}')
                            total_inserted += success
                        
                        else:
                            session.rollback()
                            logging.info(f'Failed to insert any records in batch {batch_num}, rolling back...')
                            raise Exception(f'No records could be inserted in this batch')
                    else:
                        logging.info(f'Non-bulk insert error in batch {batch_num}: {e}')
                        raise

                # Update pagination marker - use the LAST visit_occurrence_id from this batch
                if encounters:
                    last_visit_occurrence_id = encounters[-1].visit_occurrence_id
                
                batch_num += 1
                
                # Check exit condition for non-all_records mode
                if not all_records and total_inserted >= total_records:
                    logging.info(f'Reached target of {total_records} records. Stopping.')
                    break

            logging.info(f'\n=== FINAL RESULTS ===')
            logging.info(f'Successfully inserted {total_inserted} records into {VisitOccurrence.table_name()}')
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
            logging.error(f'Error inserting records into {VisitOccurrence.table_name()}: {e}')
            raise
        finally:
            session.close()
            endtime = dt.datetime.now()
            logging.info(f'Insert completed at: {endtime.strftime("%Y-%m-%d %H:%M:%S")}')
            duration=endtime-starttime
            total_seconds=int(duration.total_seconds())
            hours=total_seconds//3600
            minutes=(total_seconds%3600) // 60
            seconds=total_seconds%60
            duration=dt.time(hour=hours, minute=minutes, second=seconds)
            logging.info(f'Total time taken: {duration.strftime("%H:%M:%S")}')
            logging.info('Session Closed')

# Example Usage
if __name__=='__main__':
    vo = VisitOccurrence(schema = 'Target Schema')
    vo.set_source('Source Schema','Source Table')
    print(f'Source set to: {VisitOccurrence.get_source_fullname()}')
    engine = vo.connect_db(os.getenv('DB_URL'), os.getenv('DB_NAME'))
    current=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path=os.path.join(current,'config.json')
    vo.set_config(config_path)
    vo.drop_table(engine)
    vo.create_table(engine)
    vo.insert_records(engine,all_records=True, batch_size=20000, sub_batch_size=5000)