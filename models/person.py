import datetime as dt
import logging
from typing import Any
import os


from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, DateTime, VARCHAR


from omop_server.utils.utility import map_ethnicity, map_race, map_gender, sql_val
from omop_server.models.person_id import Person_ID  # Import Person_ID for reference
from omop_server.models.basetable import ParentBase
from omop_server.utils.logging_manager import get_logger

logger=get_logger(__name__)


class Person(ParentBase):
    
    """
    Represents the OMOP Person entity for the ETL process.

    This class defines the schema and ETL logic for the OMOP_Persons table, 
    which stores standardized demographic and identifying information for each person 
    in the OMOP Common Data Model. It includes methods for creating the table, 
    mapping and transforming source data, and inserting person records in batches. 
    The Person class ensures that all required and optional fields are populated 
    according to OMOP CDM standards and supports referential integrity with related tables.
    """
    
    __tablename__ = 'OMOP_Persons'
    __source_schema__ = None
    __source_table__ = None
    __config_manager__=None
    
    # Fix: Add autoincrement=False to prevent SQLAlchemy from treating this as identity
    person_id = Column(Integer, primary_key=True, autoincrement=False)
    gender_concept_id = Column(Integer, nullable=False)
    year_of_birth = Column(Integer, nullable=False)
    month_of_birth = Column(Integer, nullable=False)
    day_of_birth = Column(Integer, nullable=False)
    birth_datetime = Column(DateTime, nullable=False)
    race_concept_id = Column(Integer, nullable=False)
    ethnicity_concept_id = Column(Integer, nullable=False)
    location_id = Column(Integer, nullable=True)
    provider_id = Column(Integer, nullable=True)
    care_site_id = Column(Integer, nullable=True)
    person_source_value = Column(Integer, nullable=False)
    gender_source_value = Column(VARCHAR(50), nullable=False)
    race_source_value = Column(VARCHAR(50), nullable=False)
    ethnicity_source_value = Column(VARCHAR(50), nullable=False)
    
    def __init__(self, schema):
        self.__table__.schema = schema
        
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
        errors = config.validate_config('person')
        if errors:
            for error in errors:
                logging.info('Errors found\n',error)
        required_columns=config.get_all_columns('person')
        
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
        schema = Person.__table__.schema
        if not schema:
            raise ValueError("Schema must be set before creating the table.")
        
        # Check if table already exists
        check_table_sql = f"""
        SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{Person.__tablename__}'
        """
        
        table_creation_sql = f"""
        CREATE TABLE [{Person.__table__.schema}].[{Person.__tablename__}] (
            person_id INT NOT NULL,
            gender_concept_id INT,
            year_of_birth INT,
            month_of_birth INT NULL,
            day_of_birth INT NULL,
            birth_datetime DATETIME NULL,
            race_concept_id INT,
            ethnicity_concept_id INT,
            location_id INT NULL,
            provider_id INT NULL,
            care_site_id INT NULL,
            person_source_value INT NULL,
            gender_source_value VARCHAR(50) NULL,
            race_source_value VARCHAR(50) NULL,
            ethnicity_source_value VARCHAR(50) NULL
            );
        """
        
        # Add NOT ENFORCED primary key constraint
        pk_constraint_sql = f"""
        ALTER TABLE [{schema}].[{Person.__tablename__}]
        ADD CONSTRAINT PK_{Person.__tablename__}_person_id 
        PRIMARY KEY NONCLUSTERED (person_id) NOT ENFORCED
        """
        
        try:
            with engine.connect() as conn:
                result = conn.execute(text(check_table_sql))
                table_exists = result.scalar() > 0
                
                if not table_exists:
                    conn.execute(text(table_creation_sql))
                    logging.info(f"Table {schema}.{Person.__tablename__} created successfully.")
                    
                    conn.execute(text(pk_constraint_sql))
                    logging.info(f"Primary key constraint added to {schema}.{Person.__tablename__}")
                else:
                    logging.info(f"Table {schema}.{Person.__tablename__} already exists.")
                    
        except Exception as e:
            logging.error(f"Error creating table: {e}")
            raise
    
    @staticmethod
    def insert_records(engine, all_records=False, total=None, batch_size=1000000, sub_batch_size=1000) -> Any:
        '''
        Insert records from the source table into OMOP_Persons table.
        Args:
            engine: SQLAlchemy engine
            all_records (bool): If True, insert all records; if False, insert specified number
            total (int): Total number of records to insert (only used when all_records=False)
            batch_size (int): Number of records to process in each batch
            sub_batch_size (int): Number of packets to process in each batch at a time
        Returns:
            int: Total number of records inserted
        '''
        # Validate parameters
        if not all_records and total is None:
            raise ValueError("When all_records=False, total parameter must be specified")
        
        if not all_records and batch_size > 100000:
            batch_size = 100000  # Use smaller batch size for sample records
        
        if not all_records and sub_batch_size > 1000:
            sub_batch_size = 1000  # Keep sub_batch_size reasonable for sample records
        
        Session = sessionmaker(bind=engine)
        session = Session()
        starttime = dt.datetime.now()
        logging.info(f'Insert started at: {starttime.strftime("%Y-%m-%d %H:%M:%S")}')
        Person_ID.__table__.schema = Person.__table__.schema
        config = Person.get_config()
        config_errors = config.validate_config('person')
        if config_errors:
            raise ValueError(f"Configuration Errors:{','.join(config_errors)}")
        source_columns = config.get_source_columns('person')
        logging.info(source_columns)
        total_records=None
        
        try:
            # Get total count for progress tracking (only for all_records mode)
            if all_records:
                count_sql = f'''
                SELECT COUNT(*) FROM [{Person.__source_schema__}].[{Person.__source_table__}] s
                INNER JOIN [{Person_ID.__table__.schema}].[{Person_ID.__tablename__}] p
                ON s.{source_columns['person_id']} = p.person_id
                WHERE s.{source_columns['person_id']} IS NOT NULL
                AND s.{source_columns['birth_datetime']} IS NOT NULL
                '''
                total_records = session.execute(text(count_sql)).scalar()
                logging.info(f'Total records to insert: {total_records}')
                
                if total_records == 0:
                    logging.info('No records to insert')
                    return 0
            
            total_inserted = 0
            batch_num = 1
            last_person_id = None
            
            while True:
                # Check if we should continue processing
                if not all_records and total_inserted >= total:
                    break
                
                # Build query based on whether this is the first batch or not
                if last_person_id is None:
                    sql = f'''
                    SELECT TOP {batch_size} s.{source_columns['person_id']},
                    s.{source_columns['gender']},
                    s.{source_columns['race']},
                    s.{source_columns['ethnicity']},
                    s.{source_columns['birth_datetime']}
                    FROM [{Person.__source_schema__}].[{Person.__source_table__}] s
                    INNER JOIN [{Person_ID.__table__.schema}].[{Person_ID.__tablename__}] p
                    ON s.{source_columns['person_id']} = p.person_id
                    WHERE s.{source_columns['person_id']} IS NOT NULL
                    AND s.{source_columns['birth_datetime']} IS NOT NULL
                    ORDER BY s.{source_columns['person_id']}
                    '''
                    params = {}
                else:
                    sql = f'''
                    SELECT TOP {batch_size} s.{source_columns['person_id']},
                    s.{source_columns['gender']},
                    s.{source_columns['race']},
                    s.{source_columns['ethnicity']},
                    s.{source_columns['birth_datetime']}
                    FROM [{Person.__source_schema__}].[{Person.__source_table__}] s
                    INNER JOIN [{Person_ID.__table__.schema}].[{Person_ID.__tablename__}] p
                    ON s.{source_columns['person_id']} = p.person_id
                    WHERE s.{source_columns['person_id']} IS NOT NULL 
                    AND s.{source_columns['birth_datetime']} IS NOT NULL
                    AND s.{source_columns['person_id']} > :last_person_id
                    ORDER BY s.{source_columns['person_id']}
                    '''
                    params = {'last_person_id': last_person_id}
                
                result = session.execute(text(sql), params).fetchall()
                if not result:
                    break
                
                omop_persons = []
                for row in result:
                    if row[0] is None:
                        continue
                    
                    # Break if we've reached the desired total for sample records
                    if not all_records and total_inserted + len(omop_persons) >= total:
                        break
                    
                    gender_concept_id = map_gender(row[1])
                    race_concept_id = map_race(row[2])
                    ethnicity_concept_id = map_ethnicity(row[3])
                    birth_datetime = row[4]
                    
                    year_of_birth = birth_datetime.year if birth_datetime else None
                    month_of_birth = birth_datetime.month if birth_datetime else None
                    day_of_birth = birth_datetime.day if birth_datetime else None
                    
                    omop_person = Person(schema=Person.__table__.schema)
                    omop_person.person_id = row[0]
                    omop_person.gender_concept_id = gender_concept_id
                    omop_person.year_of_birth = year_of_birth
                    omop_person.month_of_birth = month_of_birth
                    omop_person.day_of_birth = day_of_birth
                    omop_person.birth_datetime = birth_datetime
                    omop_person.race_concept_id = race_concept_id
                    omop_person.ethnicity_concept_id = ethnicity_concept_id
                    omop_person.location_id = None
                    omop_person.provider_id = None
                    omop_person.care_site_id = None
                    omop_person.person_source_value = row[0] or ''
                    omop_person.gender_source_value = row[1] or ''
                    omop_person.race_source_value = row[2] or ''
                    omop_person.ethnicity_source_value = row[3] or ''
                    
                    omop_persons.append(omop_person)
                
                # Trim the list if it exceeds the remaining records needed (for sample records)
                if not all_records:
                    remaining = total - total_inserted
                    if len(omop_persons) > remaining:
                        omop_persons = omop_persons[:remaining]
                
                if not omop_persons:
                    logging.info(f'No valid persons found in batch {batch_num}, skipping...')
                    batch_num += 1
                    continue
                
                # Try bulk insert first
                batch_inserted = 0
                try:
                    session.bulk_save_objects(omop_persons)
                    session.commit()
                    logging.info(f'✓ Bulk inserted batch {batch_num}: {len(omop_persons)} records')
                    batch_inserted = len(omop_persons)
                
                except ProgrammingError as e:
                    session.rollback()
                    if 'DEFAULT VALUES' in str(e) or 'Incorrect syntax near' in str(e):
                        logging.info(f'Bulk insert failed for batch {batch_num}. Using alternative insert method...')
                        
                        success = 0
                        for i in range(0, len(omop_persons), sub_batch_size):
                            sub_batch = omop_persons[i:i+sub_batch_size]
                            
                            valid_sub_batch = [obj for obj in sub_batch if obj.person_id is not None]
                            if not valid_sub_batch:
                                continue
                            
                            try:
                                # Try sub-batch insert using UNION ALL
                                select_parts = []
                                for obj in valid_sub_batch:
                                    select_parts.append(
                                        f"SELECT {sql_val(obj.person_id)} AS person_id, "
                                        f"{sql_val(obj.gender_concept_id)} AS gender_concept_id, "
                                        f"{sql_val(obj.year_of_birth)} AS year_of_birth, "
                                        f"{sql_val(obj.month_of_birth)} AS month_of_birth, "
                                        f"{sql_val(obj.day_of_birth)} AS day_of_birth, "
                                        f"{sql_val(obj.birth_datetime)} AS birth_datetime, "
                                        f"{sql_val(obj.race_concept_id)} AS race_concept_id, "
                                        f"{sql_val(obj.ethnicity_concept_id)} AS ethnicity_concept_id, "
                                        f"{sql_val(obj.location_id)} AS location_id, "
                                        f"{sql_val(obj.provider_id)} AS provider_id, "
                                        f"{sql_val(obj.care_site_id)} AS care_site_id, "
                                        f"{sql_val(obj.person_source_value)} AS person_source_value, "
                                        f"{sql_val(obj.gender_source_value)} AS gender_source_value, "
                                        f"{sql_val(obj.race_source_value)} AS race_source_value, "
                                        f"{sql_val(obj.ethnicity_source_value)} AS ethnicity_source_value"
                                    )

                                select_str = " UNION ALL ".join(select_parts)

                                insert_sql = f"""
                                INSERT INTO [{Person.__table__.schema}].[{Person.__tablename__}]
                                ([person_id], [gender_concept_id], [year_of_birth], [month_of_birth],
                                [day_of_birth], [birth_datetime], [race_concept_id], [ethnicity_concept_id],
                                [location_id], [provider_id], [care_site_id], [person_source_value],
                                [gender_source_value], [race_source_value], [ethnicity_source_value])
                                {select_str}
                                """

                                session.execute(text(insert_sql))
                                session.commit()
                                success += len(valid_sub_batch)
                                logging.info(f'✓ Sub-batch insert successful: {len(valid_sub_batch)} records')
                                
                            except Exception as sub_e:
                                session.rollback()
                                logging.info(f'Sub-batch insert failed, trying individual inserts: {sub_e}')
                                
                                # Try individual inserts as last resort
                                for obj in valid_sub_batch:
                                    if obj.person_id is None:
                                        continue
                                    try:
                                        individual_insert_sql = f"""
                                        INSERT INTO [{Person.__table__.schema}].[{Person.__tablename__}]
                                        ([person_id], [gender_concept_id], [year_of_birth], [month_of_birth], 
                                        [day_of_birth], [birth_datetime], [race_concept_id], [ethnicity_concept_id], 
                                        [location_id], [provider_id], [care_site_id], [person_source_value], 
                                        [gender_source_value], [race_source_value], [ethnicity_source_value])
                                        VALUES
                                        ({sql_val(obj.person_id)}, {sql_val(obj.gender_concept_id)}, 
                                        {sql_val(obj.year_of_birth)}, {sql_val(obj.month_of_birth)}, 
                                        {sql_val(obj.day_of_birth)}, {sql_val(obj.birth_datetime)}, 
                                        {sql_val(obj.race_concept_id)}, {sql_val(obj.ethnicity_concept_id)}, 
                                        {sql_val(obj.location_id)}, {sql_val(obj.provider_id)}, 
                                        {sql_val(obj.care_site_id)}, {sql_val(obj.person_source_value)}, 
                                        {sql_val(obj.gender_source_value)}, {sql_val(obj.race_source_value)}, 
                                        {sql_val(obj.ethnicity_source_value)})
                                        """
                                        session.execute(text(individual_insert_sql))
                                        session.commit()
                                        success += 1
                                    except Exception as ind_e:
                                        session.rollback()
                                        logging.info(f'Failed to insert person_id {obj.person_id}: {ind_e}')
                                        continue
                        
                        if success > 0:
                            logging.info(f'Fallback method inserted {success} out of {len(omop_persons)} records in batch {batch_num}')
                            batch_inserted = success
                        else:
                            logging.info(f'Failed to insert any records in batch {batch_num}')
                            if all_records:
                                raise Exception(f'No records could be inserted in batch {batch_num}')
                            else:
                                session.rollback()
                                logging.info(f'Failed to insert any records in batch {batch_num}, rolling back...')
                                raise Exception(f'No records could be inserted in this batch')
                            
                    else:
                        logging.info(f'Non-recoverable bulk insert error in batch {batch_num}: {e}')
                        raise
                
                total_inserted += batch_inserted
                batch_num += 1
                
                if omop_persons:
                    last_person_id = omop_persons[-1].person_id
                
                logging.info(f'Batch {batch_num - 1} complete. Total inserted so far: {total_inserted}')
                
                # Break if we've reached the total for sample records
                if not all_records and total_inserted >= total:
                    break
            
            logging.info(f'\n=== FINAL RESULTS ===')
            logging.info(f'Successfully inserted {total_inserted} records into {Person.table_name()}')


            if all_records:
                logging.info(f'Target was: {total_records} records')
                logging.info(f'Completion: {(total_inserted/total_records)*100:.1f}%' if total_records and total_records > 0 else 'N/A')
            else:
                logging.info(f'Target was: {total} records')
                logging.info(f'Completion: {(total_inserted/total)*100:.1f}%' if total and total > 0 else 'N/A')
            
            logging.info(f'Mode: {"All available records" if all_records else f"Limited to {total}"}')

            return total_inserted
        
            
            
        except Exception as e:
            session.rollback()
            logging.error(f'Error inserting records into {Person.table_name()}: {e}')
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


# Example usage:

if __name__ == "__main__":
    # Create Person instance
    person_instance = Person('Target Schema')
    print(f'Person table Name: {person_instance.table_name()}')
    
    # Set source and logging.info the result
    result = Person.set_source('Source Schema', 'Source Table')
    print(result)
    
    # logging.info current source information
    print(f"Current source: {Person.get_source_fullname()}")
    
    # Connect to database and insert filtered records
    engine = person_instance.connect_db(os.getenv('DB_URL'), os.getenv('DB_NAME'))
    current=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path=os.path.join(current,'config.json')
    person_instance.set_config(config_path)
    person_instance.drop_table(engine)
    person_instance.create_table(engine)
    person_instance.insert_records(engine,all_records=False, total=500000,batch_size=10000, sub_batch_size=1000)
    query_result=person_instance.query(engine,f"SELECT * FROM {person_instance.table_name()} WHERE year_of_birth=1990")
