import pandas as pd
from typing import Any
import datetime as dt
import os
import logging


from sqlalchemy import text, Column, Integer, Date, DateTime, VARCHAR
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError


from ..utils.concept_mapper import ConceptMapper
from ..utils.utility import sql_val
from ..models.basetable import ParentBase
from ..models.visit_occurrence import VisitOccurrence
from ..utils.logging_manager import get_logger

logger=get_logger(__name__)


class ProcedureOccurrence(ParentBase):
    
    """
    Represents the OMOP Procedure Occurrence table and provides methods for ETL operations.

    - Inherits from ParentBase for SQLAlchemy ORM mapping.
    - Defines all OMOP fields as SQLAlchemy columns.
    - Allows dynamic schema assignment and source table configuration.
    - Validates source columns against configuration.
    - Supports custom table creation for SQL Server, including NOT ENFORCED constraints.
    - Handles efficient batch and sub-batch record insertion with error recovery.
    - Integrates with ConceptMapper for semantic mapping of procedure concepts.
    - Logs progress, errors, and performance metrics throughout the ETL process.
    """
    
    __tablename__ = 'OMOP_Procedure_Occurrence'
    __source_schema__ = None
    __source_table__ = None
    __config_manager__ = None
    
    procedure_occurrence_id = Column(Integer, primary_key=True, autoincrement=False, nullable=False)
    person_id = Column(Integer, nullable=False)
    procedure_concept_id = Column(Integer, nullable=False)
    procedure_date = Column(Date, nullable=False)
    procedure_datetime = Column(DateTime, nullable=True)
    procedure_end_date = Column(Date, nullable=True)
    procedure_end_datetime = Column(DateTime, nullable=True)
    procedure_type_concept_id = Column(Integer, nullable=False)
    modifier_concept_id = Column(Integer, nullable=True)
    quantity = Column(Integer, nullable=True)
    provider_id = Column(Integer, nullable=True)
    visit_occurrence_id = Column(Integer, nullable=True)
    visit_detail_id = Column(Integer, nullable=True)
    procedure_source_value = Column(VARCHAR(50), nullable=True)
    procedure_source_concept_id = Column(Integer, nullable=True)
    modifier_source_value = Column(VARCHAR(50), nullable=True)
    
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
        errors = config.validate_config('procedure_occurrence')
        if errors:
            for error in errors:
                logging.info('Errors found\n',error)
        required_columns=config.get_all_columns('procedure_occurrence')
        
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
        schema = ProcedureOccurrence.__table__.schema
        if not schema:
            raise ValueError("Schema must be set before creating the table.")
        
        # Check if table already exists
        check_table_sql = f"""
        SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{ProcedureOccurrence.__tablename__}'
        """
        
        table_creation_sql = f"""
        CREATE TABLE [{schema}].[{ProcedureOccurrence.__tablename__}] (
            procedure_occurrence_id BIGINT NOT NULL,
            person_id BIGINT NOT NULL,
            procedure_concept_id BIGINT NOT NULL,
            procedure_date DATE NOT NULL,
            procedure_datetime DATETIME NULL,
            procedure_end_date DATE NOT NULL,
            procedure_end_datetime DATETIME NULL,
            procedure_type_concept_id INT NOT NULL,
            modifier_concept_id INT NULL,
            quantity INT NULL,
            provider_id BIGINT NULL,
            visit_occurrence_id BIGINT NULL,
            visit_detail_id BIGINT NULL,
            procedure_source_value VARCHAR(50) NULL,
            procedure_source_concept_id INT NULL,
            modifier_source_value VARCHAR(50) NULL
            );
        """
        
        # Add NOT ENFORCED primary key constraint
        pk_constraint_sql = f"""
        ALTER TABLE [{schema}].[{ProcedureOccurrence.__tablename__}]
        ADD CONSTRAINT PK_{ProcedureOccurrence.__tablename__}_procedure_occurrence_id 
        PRIMARY KEY NONCLUSTERED (procedure_occurrence_id) NOT ENFORCED
        """
        
        try:
            with engine.connect() as conn:
                result = conn.execute(text(check_table_sql))
                table_exists = result.scalar() > 0
                
                if not table_exists:
                    conn.execute(text(table_creation_sql))
                    logging.info(f"Table {schema}.{ProcedureOccurrence.__tablename__} created successfully.")
                    
                    conn.execute(text(pk_constraint_sql))
                    logging.info(f"Primary key constraint added to {schema}.{ProcedureOccurrence.__tablename__}")
                else:
                    logging.info(f"Table {schema}.{ProcedureOccurrence.__tablename__} already exists.")
                    
        except Exception as e:
            logging.error(f"Error creating table: {e}")
            raise
    @staticmethod
    def insert_records(
        engine,
        all_records=False,
        total=1000000,
        batch_size=100000,
        sub_batch_size=10000,
    ) -> Any:
        """
        Insert records into the OMOP_Procedure_Occurrence table.

        Args:
            engine: SQLAlchemy engine
            all_records (bool): If True, insert all available records. If False, insert up to 'total' records.
            total (int): Maximum number of records to insert when all_records=False
            batch_size (int): Number of records to process in each batch
            sub_batch_size (int): Number of records to process in each sub-batch for fallback insert

        Returns:
            int: Number of records successfully inserted
        """
        schema = ProcedureOccurrence.__table__.schema
        table_name = ProcedureOccurrence.__tablename__
        VisitOccurrence.__table__.schema = schema
        session = sessionmaker(bind=engine)()
        starttime = dt.datetime.now()
        logging.info(f'Insert started at: {starttime.strftime("%Y-%m-%d %H:%M:%S")}')

        config = ProcedureOccurrence.get_config()
        config_errors = config.validate_config('procedure_occurrence')
        if config_errors:
            raise ValueError(f"Configuration Errors: {','.join(config_errors)}")
        source_columns = config.get_source_columns('procedure_occurrence')
        logging.info(source_columns)
        total_records = None

        try:
            procedure_mapper = ConceptMapper(
                engine=engine,
                schema=ProcedureOccurrence.__table__.schema,
                domain='Procedure',
                concept_table_name='OMOP_procedure_concepts'
            )
            logging.info(f'Loading procedure concepts lookup data...')
            procedure_concepts = procedure_mapper.get_concept_lookups()
            logging.info(f'Loaded {len(procedure_concepts)} procedure concepts for mapping')
        except Exception as e:
            logging.error(f'Error initializing concept mappers: {e}')
            logging.info('Falling back to original mapping functions...')
            procedure_mapper = None

        try:
            count_sql = f'''
                SELECT COUNT(*) FROM [{ProcedureOccurrence.__source_schema__}].[{ProcedureOccurrence.__source_table__}] po
                    INNER JOIN [{VisitOccurrence.__table__.schema}].[{VisitOccurrence.__tablename__}] v
                    ON po.{source_columns['visit_occurrence_id']} = v.visit_occurrence_id
                    WHERE po.{source_columns['procedure_occurrence_id']} IS NOT NULL
                    AND v.person_id IS NOT NULL
                    AND po.{source_columns['procedure_date']} IS NOT NULL
                    AND po.{source_columns['procedure_date']} > '1930-01-01'
                    AND po.{source_columns['visit_occurrence_id']} IS NOT NULL
            '''
            available_records = session.execute(text(count_sql)).scalar()
            logging.info(f'Available records in source: {available_records}')

            if available_records == 0:
                logging.info('No records available to insert')
                return 0

            if all_records:
                total_records = available_records
                logging.info(f'Processing ALL records: {total_records}')
            else:
                total_records = min(total, available_records)
                logging.info(f'Processing {total_records} records (requested: {total}, available: {available_records})')

            existing_ids_sql = f'''
                SELECT procedure_occurrence_id FROM [{schema}].[{table_name}]
            '''
            try:
                existing_result = session.execute(text(existing_ids_sql)).fetchall()
                existing_ids = {row[0] for row in existing_result}
                logging.info(f'Found {len(existing_ids)} existing procedure occurrence records to skip')
            except:
                existing_ids = set()
                logging.info('No existing records found (table might be empty)')

            if existing_ids:
                ids_list = ','.join(map(str, existing_ids))
                adjusted_count_sql = f'''
                    SELECT COUNT(*) FROM [{ProcedureOccurrence.__source_schema__}].[{ProcedureOccurrence.__source_table__}] po
                    INNER JOIN [{VisitOccurrence.__table__.schema}].[{VisitOccurrence.__tablename__}] v
                    ON po.{source_columns['visit_occurrence_id']} = v.visit_occurrence_id
                    WHERE po.{source_columns['procedure_occurrence_id']} IS NOT NULL
                    AND v.person_id IS NOT NULL
                    AND po.{source_columns['procedure_date']} IS NOT NULL
                    AND po.{source_columns['procedure_date']} > '1930-01-01'
                    AND po.{source_columns['visit_occurrence_id']} IS NOT NULL
                    AND po.{source_columns['procedure_occurrence_id']} NOT IN ({ids_list})
                '''
                available_new_records = session.execute(text(adjusted_count_sql)).scalar()
                logging.info(f'Available new records to insert: {available_new_records}')

                if available_new_records == 0:
                    logging.info('No new records to insert')
                    return 0

                if all_records:
                    total_records = available_new_records
                else:
                    total_records = min(total_records, available_new_records)

            total_inserted = 0
            batch_num = 1
            last_procedure_occurrence_id = None
            consecutive_empty_batches = 0

            while total_inserted < total_records:
                logging.info(f'\n--- Processing Batch {batch_num} ---')
                logging.info(f'Progress: {total_inserted}/{total_records} ({(total_inserted/total_records)*100:.1f}%)')

                remaining_needed = total_records - total_inserted
                current_batch_size = min(batch_size, remaining_needed)

                base_where = f'''
                    po.{source_columns['procedure_occurrence_id']} IS NOT NULL
                    AND po.{source_columns['procedure_date']} IS NOT NULL
                    AND po.{source_columns['procedure_date']} > '1930-01-01'
                    AND v.person_id IS NOT NULL
                    AND po.{source_columns['visit_occurrence_id']} IS NOT NULL
                '''

                if len(existing_ids) > 30000:
                    base_where = f'''
                        po.{source_columns['procedure_occurrence_id']} IS NOT NULL
                        AND po.{source_columns['procedure_date']} IS NOT NULL
                        AND po.{source_columns['procedure_date']} > '1930-01-01'
                        AND v.person_id IS NOT NULL
                        AND po.{source_columns['visit_occurrence_id']} IS NOT NULL
                        AND NOT EXISTS (
                            SELECT 1 FROM [{schema}].[{table_name}] existing 
                            WHERE existing.procedure_occurrence_id = po.{source_columns['procedure_occurrence_id']}
                        )
                    '''
                else:
                    if existing_ids:
                        ids_list = ','.join(map(str, existing_ids))
                        base_where += f" AND po.{source_columns['procedure_occurrence_id']} NOT IN ({ids_list})"

                if last_procedure_occurrence_id is not None:
                    base_where += f" AND po.{source_columns['procedure_occurrence_id']} > {last_procedure_occurrence_id}"

                sql = f'''
                    SELECT TOP {current_batch_size} 
                        po.{source_columns['procedure_occurrence_id']},
                        v.person_id,
                        po.{source_columns['procedure_date']},
                        po.{source_columns['visit_occurrence_id']},
                        po.{source_columns['procedure_source_value']}
                    FROM [{ProcedureOccurrence.__source_schema__}].[{ProcedureOccurrence.__source_table__}] po
                    INNER JOIN [{VisitOccurrence.__table__.schema}].[{VisitOccurrence.__tablename__}] v
                    ON po.{source_columns['visit_occurrence_id']} = v.visit_occurrence_id
                    WHERE {base_where}
                    ORDER BY po.{source_columns['procedure_occurrence_id']}
                '''

                result = session.execute(text(sql)).fetchall()

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

                if procedure_mapper is not None:
                    batch_df = pd.DataFrame(result, columns=[
                        'procedure_occurrence_id', 'person_id', 'procedure_date',
                        'visit_occurrence_id', 'procedure_source_value'
                    ])
                    logging.info(f'Mapping procedure concepts using Procedure Concept Mapper...')
                    try:
                        procedure_mapped = procedure_mapper.map_source_to_concept(
                            source_df=batch_df,
                            source_column='procedure_source_value',
                            mapping_strategy='semantic',
                            concept_df=procedure_concepts
                        )
                        batch_df['procedure_concept_id'] = procedure_mapped['concept_id']
                        logging.info(f'Successfully mapped {procedure_mapped["concept_id"].notna().sum()} procedure concepts')
                    except Exception as e:
                        logging.error(f'Error mapping procedure concepts: {e}')
                        batch_df['procedure_concept_id'] = 0
                    batch_df['procedure_concept_id'] = batch_df['procedure_concept_id'].fillna(0)

                procedures = []
                processed_in_batch = 0

                for idx, row in enumerate(result):
                    if row[0] is None or row[0] in existing_ids:
                        continue

                    if not all_records and total_inserted + len(procedures) >= total_records:
                        logging.info(f'Reached target limit. Stopping at {total_inserted + len(procedures)} records.')
                        break

                    procedure_occurrence_id = row[0]
                    person_id = row[1]
                    procedure_datetime = row[2]
                    procedure_end_datetime = row[2]
                    visit_occurrence_id = row[3]
                    procedure_source_value = row[4]

                    if isinstance(row[2], str):
                        procedure_date = dt.datetime.fromisoformat(row[2]).date()
                    else:
                        procedure_date = row[2].date()

                    if isinstance(row[2], str):
                        procedure_end_date = dt.datetime.fromisoformat(row[2]).date()
                    else:
                        procedure_end_date = row[2].date()

                    if procedure_mapper is not None:
                        procedure_concept_id = int(batch_df.loc[idx]['procedure_concept_id'])
                    else:
                        logging.warning(f'Concept Mappers not initialized, using default values')
                        procedure_concept_id = 0

                    po = ProcedureOccurrence(schema=ProcedureOccurrence.__table__.schema)
                    po.procedure_occurrence_id = procedure_occurrence_id
                    po.person_id = person_id
                    po.procedure_concept_id = procedure_concept_id
                    po.procedure_date = procedure_date
                    po.procedure_datetime = procedure_datetime
                    po.procedure_end_date = procedure_end_date
                    po.procedure_end_datetime = procedure_end_datetime
                    po.procedure_type_concept_id = 32817
                    po.modifier_concept_id = None
                    po.quantity = 1
                    po.provider_id = None
                    po.visit_occurrence_id = visit_occurrence_id
                    po.visit_detail_id = None
                    po.procedure_source_value = procedure_source_value
                    po.procedure_source_concept_id = None
                    po.modifier_source_value = None

                    procedures.append(po)
                    processed_in_batch += 1

                logging.info(f'Created {len(procedures)} procedure_occurrence objects')

                if not procedures:
                    logging.info(f'No valid encounters found in batch {batch_num}, skipping...')
                    batch_num += 1
                    continue

                try:
                    session.add_all(procedures)
                    session.commit()
                    logging.info(f'✓ Successfully inserted batch {batch_num}: {len(procedures)} records')
                    total_inserted += len(procedures)
                    for procedure in procedures:
                        existing_ids.add(procedure.procedure_occurrence_id)
                except ProgrammingError as e:
                    session.rollback()
                    if 'DEFAULT VALUES' in str(e) or 'Incorrect syntax near' in str(e):
                        logging.info(f'Bulk insert failed for batch {batch_num}. Using alternative insert method...')
                        success = 0
                        for i in range(0, len(procedures), sub_batch_size):
                            sub_batch = procedures[i: i+sub_batch_size]
                            valid_sub_batch = [obj for obj in sub_batch if obj.procedure_occurrence_id and obj.person_id is not None]
                            if not valid_sub_batch:
                                continue
                            try:
                                select_parts = []
                                for obj in valid_sub_batch:
                                    select_parts.append(
                                        f"SELECT CAST ({sql_val(obj.procedure_occurrence_id)} AS BIGINT) AS procedure_occurrence_id,"
                                        f"CAST({sql_val(obj.person_id)} AS BIGINT) AS person_id,"
                                        f"CAST({sql_val(obj.procedure_concept_id)} AS BIGINT) AS procedure_concept_id,"
                                        f"CAST({sql_val(obj.procedure_date)} AS DATE) AS procedure_date,"
                                        f"CAST({sql_val(obj.procedure_datetime)} AS DATETIME) AS procedure_datetime,"
                                        f"CAST({sql_val(obj.procedure_end_date)} AS DATE) AS procedure_end_date,"
                                        f"CAST({sql_val(obj.procedure_end_datetime)} AS DATETIME) AS procedure_end_datetime,"
                                        f"CAST({sql_val(obj.procedure_type_concept_id)} AS BIGINT) AS procedure_type_concept_id,"
                                        f"CAST({sql_val(obj.modifier_concept_id)} AS INT) AS modifier_concept_id,"
                                        f"CAST({sql_val(obj.quantity)} AS INT) AS quantity,"
                                        f"CAST({sql_val(obj.provider_id)} AS BIGINT) AS provider_id,"
                                        f"CAST({sql_val(obj.visit_occurrence_id)} AS BIGINT) AS visit_occurrence_id,"
                                        f"CAST({sql_val(obj.visit_detail_id)} AS BIGINT) AS visit_detail_id,"
                                        f"CAST({sql_val(obj.procedure_source_value)} AS VARCHAR) AS procedure_source_value,"
                                        f"CAST({sql_val(obj.procedure_source_concept_id)} AS INT) AS procedure_source_concept_id,"
                                        f"CAST({sql_val(obj.modifier_source_value)} AS VARCHAR) AS modifier_source_value"
                                    )
                                select_str = " UNION ALL ".join(select_parts)
                                insert_sql = f'''
                                    INSERT INTO [{schema}].[{table_name}](
                                        [procedure_occurrence_id], [person_id], [procedure_concept_id],
                                        [procedure_date], [procedure_datetime], [procedure_end_date],
                                        [procedure_end_datetime], [procedure_type_concept_id], [modifier_concept_id],
                                        [quantity], [provider_id], [visit_occurrence_id],
                                        [visit_detail_id], [procedure_source_value], [procedure_source_concept_id],
                                        [modifier_source_value])
                                    {select_str}'''
                                session.execute(text(insert_sql))
                                success += len(valid_sub_batch)
                            except Exception as sub_e:
                                logging.info(f'Sub-batch insert failed, trying individual inserts: {sub_e}')
                                for obj in valid_sub_batch:
                                    if obj.procedure_occurrence_id and obj.person_id is not None:
                                        continue
                                    try:
                                        individual_insert_sql = f"""
                                            INSERT INTO [{schema}].[{table_name}](
                                            [procedure_occurrence_id], [person_id], [procedure_concept_id],
                                            [procedure_date], [procedure_datetime], [procedure_end_date],
                                            [procedure_end_datetime], [procedure_type_concept_id], [modifier_concept_id],
                                            [quantity], [provider_id], [visit_occurrence_id],
                                            [visit_detail_id], [procedure_source_value], [procedure_source_concept_id],
                                            [modifier_source_value]) VALUES (
                                            {sql_val(obj.procedure_occurrence_id)}, {sql_val(obj.person_id)}, {sql_val(obj.procedure_concept_id)},
                                            {sql_val(obj.procedure_date)}, {sql_val(obj.procedure_datetime)}, {sql_val(obj.procedure_end_date)},
                                            {sql_val(obj.procedure_end_datetime)}, {sql_val(obj.procedure_type_concept_id)}, {sql_val(obj.modifier_concept_id)},
                                            {sql_val(obj.quantity)}, {sql_val(obj.provider_id)}, {sql_val(obj.visit_occurrence_id)},
                                            {sql_val(obj.visit_detail_id)}, {sql_val(obj.procedure_source_value)}, {sql_val(obj.procedure_source_concept_id)},
                                            {sql_val(obj.modifier_source_value)})"""
                                        session.execute(text(individual_insert_sql))
                                        success += 1
                                    except Exception as ind_e:
                                        logging.info(f'Failed to insert procedure_occurrence_id {obj.procedure_occurrence_id}: {ind_e}')
                                        continue
                        if success > 0:
                            session.commit()
                            logging.info(f'Successfully inserted {success} out of {len(procedures)} records in batch {batch_num}')
                            total_inserted += success
                        else:
                            session.rollback()
                            logging.info(f'Failed to insert any records in batch {batch_num}, rolling back...')
                            raise Exception(f'No records could be inserted in this batch')
                    else:
                        logging.error(f'Non-bulk insert error in batch {batch_num}: {e}')
                        raise

                if procedures:
                    last_procedure_occurrence_id = procedures[-1].procedure_occurrence_id

                batch_num += 1

                if not all_records and total_inserted >= total_records:
                    logging.info(f'Reached target of {total_records} records. Stopping.')
                    break

            logging.info(f'\n=== FINAL RESULTS ===')
            logging.info(f'Successfully inserted {total_inserted} records into {ProcedureOccurrence.table_name()}')

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
            logging.error(f'Error inserting records into {table_name}: {e}')
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
    po = ProcedureOccurrence(schema='Target Schema')
    po.set_source('Source Schema','Source table')
    print(f'Source set to: {po.get_source_fullname()}')
    engine = po.connect_db(os.getenv('DB_URL'), os.getenv('DB_NAME'))
    current=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path=os.path.join(current,'config.json')
    po.set_config(config_path)
    po.drop_table(engine)
    po.create_table(engine)
    po.insert_records(engine,all_records=True, batch_size=20000, sub_batch_size=5000)