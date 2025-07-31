import os
import datetime as dt
import logging
from typing import Any


from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer
from sqlalchemy.exc import ProgrammingError


from ..models.basetable import ParentBase
from ..utils.logging_manager import get_logger

logger=get_logger(__name__)


class Person_ID(ParentBase):
    
    """
    Represents the OMOP Person_ID entity for the ETL process.
    
    Use this script as a test case to verify if the object is able to perform the ETL and extract the
    records to OMOP_Person_IDs table

    This class defines the schema and ETL logic for the OMOP_Person_IDs table, 
    which stores unique person identifiers mapped from the source system to the OMOP Common Data Model.
    It includes methods for creating the table with appropriate constraints, 
    setting and retrieving the source schema and table, and efficiently inserting 
    person IDs in batches. The Person_ID class ensures referential integrity and 
    provides utility functions to support the person mapping process within the OMOP ETL pipeline.
    """
    
    __tablename__ = 'OMOP_Person_IDs'
    __source_schema__ = None
    __source_table__ = None
    __config_manager__=None
    
    # Define person_id as primary key - SQLAlchemy needs this for ORM mapping
    person_id = Column(Integer, nullable=False, primary_key=True, autoincrement=False)
    
    def __init__(self, schema, person_id=None) -> None:
        # Set the schema for this table instance
        if not hasattr(self.__class__, '_schema_set') or not self.__class__._schema_set:
            self.__table__.schema = schema
            self.__class__._schema_set = True
        
        if person_id:
            self.person_id = person_id
    
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
        errors = config.validate_config('person_id')
        if errors:
            for error in errors:
                logger.error('Errors found\n',error)
        required_columns=config.get_all_columns('person_id')
        
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
                logger.info(f"Missing columns in source table: {','.join(missing_columns)}")
                return False
            else:
                logger.info("All required columns found in source table")
                return True
        except Exception as e:
            logger.error(f'Error validating source columns: {e}')
            return False
        finally:
            session.close()
 
    @staticmethod
    def create_table(engine) -> None:
        """
        Custom table creation that handles SQL Server's constraint limitations
        """
        schema = Person_ID.__table__.schema
        if not schema:
            raise ValueError("Schema must be set before creating the table.")
        
        # Check if table already exists
        check_table_sql = f"""
        SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{Person_ID.__tablename__}'
        """
        
        # Create table manually without primary key constraint
        table_creation_sql = f"""
        CREATE TABLE [{schema}].[{Person_ID.__tablename__}] (
            person_id INTEGER NOT NULL
        )
        """
        
        # Add NOT ENFORCED primary key constraint
        pk_constraint_sql = f"""
        ALTER TABLE [{schema}].[{Person_ID.__tablename__}]
        ADD CONSTRAINT PK_{Person_ID.__tablename__}_person_id 
        PRIMARY KEY NONCLUSTERED (person_id) NOT ENFORCED
        """
        
        try:
            # Use autocommit mode to avoid transaction issues
            with engine.connect() as conn:
                # Check if table already exists
                result = conn.execute(text(check_table_sql))
                table_exists = result.scalar() > 0
                
                if not table_exists:
                    # Execute each statement separately in autocommit mode
                    conn.execute(text(table_creation_sql))
                    logger.info(f"Table {schema}.{Person_ID.__tablename__} created successfully.")
                    
                    conn.execute(text(pk_constraint_sql))
                    logger.info(f"Primary key constraint added to {schema}.{Person_ID.__tablename__}")
                else:
                    logger.info(f"Table {schema}.{Person_ID.__tablename__} already exists.")
                    
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise
    
    @staticmethod
    def insert_records(engine, all_records=False, total=None, batch_size=1000000, sub_batch_size=1000) -> Any:
        '''
        Insert records from the source table into OMOP_Person_IDs table.
        Args:
            engine: SQLAlchemy engine
            all_records (bool): If True, insert all records; if False, insert specified number
            total (int): Total number of records to insert (only used when all_records=False)
            batch_size (int): Number of records to process in each batch
            sub_batch_size (int): Number of packets to process in each batch at a time
        Returns:
            int: Total number of records inserted
        '''
        if not all_records and total is None:
            raise ValueError("When all_records=False, total parameter must be specified")

        Session = sessionmaker(bind=engine)
        session = Session()
        starttime = dt.datetime.now()
        logger.info(f'Insert started at: {starttime.strftime("%Y-%m-%d %H:%M:%S")}')
        config = Person_ID.get_config()
        config_errors = config.validate_config('person_id')
        if config_errors:
            raise ValueError(f"Configuration Errors:{','.join(config_errors)}")
        source_columns = config.get_source_columns('person_id')
        logger.info(source_columns)
        total_records=None
        try:
            count_sql = f'''
            SELECT COUNT(*) FROM [{Person_ID.__source_schema__}].[{Person_ID.__source_table__}]
            WHERE [{source_columns['person_id']}] IS NOT NULL'''
            total_records = session.execute(text(count_sql)).scalar()
            logger.info(f'Total records to insert: {total_records}')

            if total_records == 0:
                logger.info('No records found to insert')
                return 0

            if not all_records:
                total_records = min(total, total_records)

            inserted = 0
            batch_num = 1
            last_person_id = None
            while inserted < total_records:
                if last_person_id is None:
                    sql = f'''
                        SELECT TOP {batch_size} [{source_columns['person_id']}] FROM [{Person_ID.__source_schema__}].[{Person_ID.__source_table__}]
                        WHERE [{source_columns['person_id']}] IS NOT NULL
                        ORDER BY [{source_columns['person_id']}]'''
                    params = {}
                else:
                    sql = f'''
                    SELECT TOP {batch_size} [{source_columns['person_id']}] FROM [{Person_ID.__source_schema__}].[{Person_ID.__source_table__}]
                    WHERE [{source_columns['person_id']}] IS NOT NULL AND 
                    [{source_columns['person_id']}] > :last_person_id ORDER BY [{source_columns['person_id']}]
                    '''
                    params = {'last_person_id': last_person_id}

                result = session.execute(text(sql), params).fetchall()
                if not result:
                    break

                omop_person_ids = [Person_ID(schema=Person_ID.__table__.schema, person_id=row[0])
                                for row in result if row[0] is not None]

                # Trim the list if it exceeds the remaining records needed (for sample records)
                remaining = total_records - inserted
                if len(omop_person_ids) > remaining:
                    omop_person_ids = omop_person_ids[:remaining]

                if not omop_person_ids:
                    logger.info(f'No valid person_ids found in batch {batch_num}, skipping...')
                    batch_num += 1
                    continue

                try:
                    session.bulk_save_objects(omop_person_ids)
                    session.commit()
                    logger.info(f'✓ Inserted batch {batch_num}: {len(omop_person_ids)} records')
                    inserted += len(omop_person_ids)
                except ProgrammingError as e:
                    session.rollback()
                    if 'DEFAULT VALUES' in str(e) or 'Incorrect syntax near' in str(e):
                        logger.info(f"Bulk insert failed for batch {batch_num}. Using alternative insert method.")
                        successful_inserts = 0
                        for i in range(0, len(omop_person_ids), sub_batch_size):
                            sub_batch = omop_person_ids[i:i + sub_batch_size]
                            valid_sub_batch = [obj for obj in sub_batch if obj.person_id is not None]
                            if not valid_sub_batch:
                                continue
                            try:
                                select_parts = []
                                for obj in valid_sub_batch:
                                    select_parts.append(f"SELECT {int(obj.person_id)} AS person_id")
                                select_str = " UNION ALL ".join(select_parts)
                                raw_insert_sql = f"""
                                INSERT INTO [{Person_ID.__table__.schema}].[{Person_ID.__tablename__}] ([person_id])
                                {select_str}
                                """
                                session.execute(text(raw_insert_sql))
                                successful_inserts += len(valid_sub_batch)
                            except Exception as sub_e:
                                logger.info(f"Sub-batch insert failed, trying individual inserts: {sub_e}")
                                for obj in valid_sub_batch:
                                    if obj.person_id is None:
                                        continue
                                    try:
                                        individual_insert_sql = f"""
                                        INSERT INTO [{Person_ID.__table__.schema}].[{Person_ID.__tablename__}] ([person_id])
                                        VALUES ({obj.person_id})
                                        """
                                        session.execute(text(individual_insert_sql))
                                        successful_inserts += 1
                                    except Exception as individual_e:
                                        logger.info(f"Failed to insert person_id {obj.person_id}: {individual_e}")
                                        continue
                        if successful_inserts > 0:
                            session.commit()
                            logger.info(f'Successfully inserted {successful_inserts} out of {len(omop_person_ids)} records in batch {batch_num}')
                            inserted += successful_inserts
                        else:
                            session.rollback()
                            logger.info(f"Failed to insert any records in batch {batch_num}")
                            raise Exception("No records could be inserted in this batch")
                    else:
                        logger.info(f"Non-bulk insert error in batch {batch_num}: {e}")
                        raise

                batch_num += 1
                last_person_id = omop_person_ids[-1].person_id if omop_person_ids else last_person_id

            logging.info(f'\n=== FINAL RESULTS ===')
            logging.info(f'Successfully inserted {inserted} records into {Person_ID.table_name()}')

            if all_records:
                logging.info(f'Target was: {total_records} records')
                logging.info(f'Completion: {(inserted/total_records)*100:.1f}%' if total_records and total_records > 0 else 'N/A')
            else:
                logging.info(f'Target was: {total_records} records')
                logging.info(f'Completion: {(inserted/total_records)*100:.1f}%' if total_records and total_records > 0 else 'N/A')
            
            logging.info(f'Mode: {"All available records" if all_records else f"Limited to {total_records}"}')

            return inserted

        
        except Exception as e:
            session.rollback()
            logger.error(f'Error inserting records into {Person_ID.table_name()}: {e}')
            raise
        finally:
            session.close()
            endtime = dt.datetime.now()
            logger.info(f'Insert completed at: {endtime.strftime("%Y-%m-%d %H:%M:%S")}')
            duration = endtime - starttime
            total_seconds = int(duration.total_seconds())
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            duration = dt.time(hour=hours, minute=minutes, second=seconds)
            logger.info(f'Total time taken: {duration.strftime("%H:%M:%S")}')   

# Example Usage

if __name__ == '__main__':
    
    id_instance = Person_ID('Target Schema')
    engine = id_instance.connect_db(os.getenv('DB_URL'), os.getenv('DB_NAME'))
    id_instance.set_source('Source Schema', 'Source Table')
    current=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path=os.path.join(current,'config.json')
    id_instance.set_config(config_path)
    id_instance.drop_table(engine)
    id_instance.create_table(engine)
    id_instance.insert_records(engine,total=500000,batch_size=20000, sub_batch_size=10000)
