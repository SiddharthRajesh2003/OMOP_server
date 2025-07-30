import logging
import datetime as dt
import os

from sqlalchemy import text, Column, Integer, Date
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError


from omop_server.utils.utility import sql_val
from omop_server.models.person import Person
from omop_server.models.basetable import ParentBase
from omop_server.utils.logging_manager import get_logger

logger=get_logger(__name__)


class ObservationPeriod(ParentBase):
    
    '''
    The ObservationPeriod class represents the OMOP Observation Period entity within the ETL 
    pipeline. 
    It is responsible for mapping and managing periods of continuous observation for each person
    in the OMOP data model. 
    This class defines the schema for the OMOP_Observation_Period table, including fields such as 
    observation_period_id, person_id, observation_period_start_date, observation_period_end_date,
    and period_type_concept_id. 
    It provides methods for creating the table, inserting records, and performing ETL operations
    to generate observation periods based on encounter data. 
    The class ensures that each person's observation periods are accurately captured and 
    stored according to OMOP CDM standards.'''
    
    __tablename__='OMOP_Observation_Period'
    __source_schema__ = None
    __source_table__ = None
    __config_manager__=None
    
    observation_period_id=Column(Integer, primary_key=True, nullable=False,autoincrement=False)
    person_id=Column(Integer, nullable=False, autoincrement=False)
    observation_period_start_date=Column(Date, nullable=False)
    observation_period_end_date=Column(Date, nullable=False)
    period_type_concept_id=Column(Integer, nullable=True)
    
    def __init__(self, schema):
        self.__table__.schema=schema
  
    @classmethod
    def validate_source_columns(cls, engine):
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
        errors = config.validate_config('observation_period')
        if errors:
            for error in errors:
                logging.error('Errors found\n',error)
        required_columns=config.get_all_columns('observation_period')
        
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
    def create_table(engine):
        """
        Custom table creation that handles SQL Server's constraint limitations
        """
        schema = ObservationPeriod.__table__.schema
        if not schema:
            raise ValueError("Schema must be set before creating the table.")
        
        # Check if table already exists
        check_table_sql = f"""
        SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{ObservationPeriod.__tablename__}'
        """
        
        # Create table manually without primary key constraint
        table_creation_sql = f"""
        CREATE TABLE [{schema}].[{ObservationPeriod.__tablename__}] (
            observation_period_id BIGINT NOT NULL,
            person_id INT NOT NULL,
            observation_period_start_date DATE NULL,
            observation_period_end_date DATE NULL,
            period_type_concept_id INT NOT NULL
            );
        """
        
        # Add NOT ENFORCED primary key constraint
        pk_constraint_sql = f"""
        ALTER TABLE [{schema}].[{ObservationPeriod.__tablename__}]
        ADD CONSTRAINT PK_{ObservationPeriod.__tablename__}_observation_period_id 
        PRIMARY KEY NONCLUSTERED (observation_period_id) NOT ENFORCED
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
                    logging.info(f"Table {schema}.{ObservationPeriod.__tablename__} created successfully.")
                    
                    conn.execute(text(pk_constraint_sql))
                    logging.info(f"Primary key constraint added to {schema}.{ObservationPeriod.__tablename__}")
                else:
                    logging.info(f"Table {schema}.{ObservationPeriod.__tablename__} already exists.")
                    
        except Exception as e:
            logging.error(f"Error creating table: {e}")
            raise
    
    @staticmethod
    def insert_records(engine, all_records=False, total_records=None, batch_size=1000000, sub_batch_size=1000):
        '''
        Inserts records from the source table into the OMOP_Observation_Period table.
        Args:
            engine: SQLAlchemy engine
            all_records (bool): If True, insert all records; if False, insert specified number
            total_records (int): total_records number of records to insert (only used when all_records=False)
            batch_size (int): Number of records to batch process
            sub_batch_size(int): Number of records to insert in each sub_batch
        Returns:
            int: total_records number of records inserted
        '''
        # Validate parameters
        if not all_records and total_records is None:
            raise ValueError("When all_records=False, total_records parameter must be specified")
        
        if not all_records and batch_size > 10000:
            batch_size = 10000  # Use smaller batch size for sample records
        
        if not all_records and sub_batch_size > 200:
            sub_batch_size = 200  # Use smaller sub-batch size for sample records
        
        schema = ObservationPeriod.__table__.schema
        session = sessionmaker(bind=engine)()
        starttime = dt.datetime.now()
        logging.info(f'Insert started at: {starttime.strftime("%Y-%m-%d %H:%M:%S")}')
        Person.__table__.schema = schema
        config = ObservationPeriod.get_config()
        config_errors = config.validate_config('observation_period')
        if config_errors:
            raise ValueError(f"Configuration Errors:{','.join(config_errors)}")
        source_columns = config.get_source_columns('observation_period')
        logging.info(source_columns)
        total_records = None
        
        try:
            # Get total_records count for progress tracking (only for all_records mode)
            if all_records:
                count_sql = f'''
                SELECT COUNT(*) FROM [{ObservationPeriod.__source_schema__}].[{ObservationPeriod.__source_table__}] e
                INNER JOIN [{Person.__table__.schema}].[{Person.__tablename__}] p
                ON e.{source_columns['person_id']} = p.person_id 
                WHERE e.{source_columns['person_id']} IS NOT NULL
                AND e.{source_columns['observation_period_start_date']} IS NOT NULL
                AND e.{source_columns['observation_period_end_date']} IS NOT NULL'''
                
                total_source_records = session.execute(text(count_sql)).scalar()
                logging.info(f'total_records persons with encounters: {total_source_records}')
                
                if total_source_records == 0:
                    logging.info('No persons with encounters found in the source table.')
                    return 0
            
            total_inserted = 0
            batch_num = 1
            last_person_id = None
            
            while True:
                # Check if we should continue processing
                if not all_records and total_inserted >= total_records:
                    break
                
                # Build query based on whether this is the first batch or not
                if last_person_id is None:
                    obs_periods_sql = f'''
                    WITH ENCOUNTERS AS (
                        SELECT TOP {batch_size} e.{source_columns['person_id']},
                        e.{source_columns['observation_period_start_date']},
                        e.{source_columns['observation_period_end_date']}
                        FROM [{ObservationPeriod.__source_schema__}].[{ObservationPeriod.__source_table__}] e
                        INNER JOIN [{Person.__table__.schema}].[{Person.__tablename__}] OPI on OPI.person_id = e.{source_columns['person_id']}
                        WHERE e.{source_columns['person_id']} IS NOT NULL 
                        AND e.{source_columns['observation_period_start_date']} IS NOT NULL
                        {'' if all_records else "AND e." + source_columns['observation_period_start_date'] + " > '1930-01-01'"}
                        AND e.{source_columns['observation_period_end_date']} IS NOT NULL
                        {'' if all_records else "AND e." + source_columns['observation_period_end_date'] + " > '1930-01-01'"}
                        ORDER BY e.{source_columns['person_id']}, e.{source_columns['observation_period_start_date']}
                    ),
                    ORDERED_ENCOUNTERS AS (
                        SELECT {source_columns['person_id']},
                        {source_columns['observation_period_start_date']},
                        {source_columns['observation_period_end_date']},
                        LAG({source_columns['observation_period_end_date']}) OVER (PARTITION BY {source_columns['person_id']} ORDER BY {source_columns['observation_period_start_date']}) AS PrevEndDate
                        FROM ENCOUNTERS
                    ),
                    FLAGGED_ENCOUNTERS AS (
                        SELECT {source_columns['person_id']},
                        {source_columns['observation_period_start_date']},
                        {source_columns['observation_period_end_date']},
                        CASE
                            WHEN PrevEndDate IS NULL THEN 1
                            WHEN DATEDIFF(DAY, PrevEndDate, {source_columns['observation_period_start_date']}) > 7300 THEN 1
                            ELSE 0
                        END AS NewPeriodFlag
                        FROM ORDERED_ENCOUNTERS
                    ),
                    NUMBERED_ENCOUNTERS AS (
                        SELECT *,
                        SUM(NewPeriodFlag) OVER (PARTITION BY {source_columns['person_id']} ORDER BY {source_columns['observation_period_start_date']} ROWS UNBOUNDED PRECEDING) AS PeriodGroup
                        FROM FLAGGED_ENCOUNTERS
                    ),
                    OBSERVATION_PERIODS AS (
                        SELECT {source_columns['person_id']},
                        PeriodGroup,
                        MIN({source_columns['observation_period_start_date']}) AS observation_period_start_date,
                        MAX({source_columns['observation_period_end_date']}) AS observation_period_end_date
                        FROM NUMBERED_ENCOUNTERS
                        GROUP BY {source_columns['person_id']}, PeriodGroup
                    )
                    SELECT {source_columns['person_id']}, observation_period_start_date, observation_period_end_date
                    FROM OBSERVATION_PERIODS
                    ORDER BY {source_columns['person_id']}, observation_period_start_date
                    '''
                    params = {}
                else:
                    obs_periods_sql = f'''
                    WITH ENCOUNTERS AS (
                        SELECT TOP {batch_size} e.{source_columns['person_id']},
                        e.{source_columns['observation_period_start_date']},
                        e.{source_columns['observation_period_end_date']}
                        FROM [{ObservationPeriod.__source_schema__}].[{ObservationPeriod.__source_table__}] e
                        INNER JOIN [{Person.__table__.schema}].[{Person.__tablename__}] OPI on OPI.person_id = e.{source_columns['person_id']}
                        WHERE e.{source_columns['person_id']} IS NOT NULL 
                        AND e.{source_columns['observation_period_start_date']} IS NOT NULL
                        {'' if all_records else "AND e." + source_columns['observation_period_start_date'] + " > '1930-01-01'"}
                        AND e.{source_columns['observation_period_end_date']} IS NOT NULL
                        {'' if all_records else "AND e." + source_columns['observation_period_end_date'] + " > '1930-01-01'"}
                        AND e.{source_columns['person_id']} > :last_person_id
                        ORDER BY e.{source_columns['person_id']}, e.{source_columns['observation_period_start_date']}
                    ),
                    ORDERED_ENCOUNTERS AS (
                        SELECT {source_columns['person_id']},
                        {source_columns['observation_period_start_date']},
                        {source_columns['observation_period_end_date']},
                        LAG({source_columns['observation_period_end_date']}) OVER (PARTITION BY {source_columns['person_id']} ORDER BY {source_columns['observation_period_start_date']}) AS PrevEndDate
                        FROM ENCOUNTERS
                    ),
                    FLAGGED_ENCOUNTERS AS (
                        SELECT {source_columns['person_id']},
                        {source_columns['observation_period_start_date']},
                        {source_columns['observation_period_end_date']},
                        CASE
                            WHEN PrevEndDate IS NULL THEN 1
                            WHEN DATEDIFF(DAY, PrevEndDate, {source_columns['observation_period_start_date']}) > 7300 THEN 1
                            ELSE 0
                        END AS NewPeriodFlag
                        FROM ORDERED_ENCOUNTERS
                    ),
                    NUMBERED_ENCOUNTERS AS (
                        SELECT *,
                        SUM(NewPeriodFlag) OVER (PARTITION BY {source_columns['person_id']} ORDER BY {source_columns['observation_period_start_date']} ROWS UNBOUNDED PRECEDING) AS PeriodGroup
                        FROM FLAGGED_ENCOUNTERS
                    ),
                    OBSERVATION_PERIODS AS (
                        SELECT {source_columns['person_id']},
                        PeriodGroup,
                        MIN({source_columns['observation_period_start_date']}) AS observation_period_start_date,
                        MAX({source_columns['observation_period_end_date']}) AS observation_period_end_date
                        FROM NUMBERED_ENCOUNTERS
                        GROUP BY {source_columns['person_id']}, PeriodGroup
                    )
                    SELECT {source_columns['person_id']}, observation_period_start_date, observation_period_end_date
                    FROM OBSERVATION_PERIODS
                    ORDER BY {source_columns['person_id']}, observation_period_start_date
                    '''
                    params = {'last_person_id': last_person_id}
                
                logging.info(f'Processing batch {batch_num}...')
                result = session.execute(text(obs_periods_sql), params=params).fetchall()
                batch_records_count = len(result)
                
                logging.info(f'Batch {batch_num}: Found {batch_records_count} observation periods')
                
                # If no records found, we're done
                if batch_records_count == 0:
                    logging.info('No more observation periods found. Processing complete.')
                    break
                
                # Process the batch - create OMOP objects
                omop_obs_periods = []
                for idx, row in enumerate(result):
                    person_id = row[0]
                    start_date = row[1]
                    end_date = row[2]
                    
                    if person_id is None or start_date is None or end_date is None:
                        logging.info(f"Skipping record {idx} due to missing values: {source_columns['person_id']}={person_id}, StartDate={start_date}, EndDate={end_date}")
                        continue
                    
                    # Break if we've reached the desired total_records for sample records
                    if not all_records and total_inserted + len(omop_obs_periods) >= total_records:
                        break
                    
                    observation_period_id = total_inserted + len(omop_obs_periods) + 1
                    omop_observation_period = ObservationPeriod(schema=ObservationPeriod.__table__.schema)
                    omop_observation_period.observation_period_id = observation_period_id
                    omop_observation_period.person_id = person_id
                    omop_observation_period.observation_period_start_date = start_date
                    omop_observation_period.observation_period_end_date = end_date
                    omop_observation_period.period_type_concept_id = 32813
                    
                    omop_obs_periods.append(omop_observation_period)
                
                # Trim the list if it exceeds the remaining records needed (for sample records)
                if not all_records:
                    remaining = total_records - total_inserted
                    if len(omop_obs_periods) > remaining:
                        omop_obs_periods = omop_obs_periods[:remaining]
                
                if not omop_obs_periods:
                    logging.info(f'No valid records found in batch {batch_num}, skipping...')
                    batch_num += 1
                    continue
                
                # Try bulk insert first
                batch_inserted = 0
                try:
                    if all_records:
                        session.add_all(omop_obs_periods)
                    else:
                        session.bulk_save_objects(omop_obs_periods)
                    session.commit()
                    logging.info(f'✓ Bulk inserted batch {batch_num}: {len(omop_obs_periods)} records')
                    batch_inserted = len(omop_obs_periods)
                
                except ProgrammingError as e:
                    session.rollback()
                    if 'DEFAULT VALUES' in str(e) or 'Incorrect syntax near' in str(e):
                        logging.info(f'Bulk insert failed for batch {batch_num}. Using alternative insert method...')
                        
                        # Fallback method inline
                        success = 0
                        for j in range(0, len(omop_obs_periods), sub_batch_size):
                            sub_batch = omop_obs_periods[j:j + sub_batch_size]
                            valid_sub_batch = [obj for obj in sub_batch if obj.observation_period_id is not None]
                            
                            if not valid_sub_batch:
                                continue
                            
                            try:
                                # Try sub-batch insert using UNION ALL
                                select_parts = []
                                for obj in valid_sub_batch:
                                    select_parts.append(
                                        f'''SELECT {sql_val(obj.observation_period_id)} as observation_period_id, '''
                                        f'''{sql_val(obj.person_id)} as person_id, '''
                                        f'''{sql_val(obj.observation_period_start_date)} as observation_period_start_date, '''
                                        f'''{sql_val(obj.observation_period_end_date)} as observation_period_end_date, '''
                                        f'''{sql_val(obj.period_type_concept_id)} as period_type_concept_id'''
                                    )
                                
                                select_str = ' UNION ALL '.join(select_parts)
                                
                                insert_sql = f'''
                                INSERT INTO [{ObservationPeriod.__table__.schema}].[{ObservationPeriod.__tablename__}]
                                ([observation_period_id], [person_id], 
                                [observation_period_start_date], [observation_period_end_date], 
                                [period_type_concept_id]) 
                                {select_str}'''
                                
                                session.execute(text(insert_sql))
                                session.commit()
                                success += len(valid_sub_batch)
                                logging.info(f'✓ Sub-batch insert successful: {len(valid_sub_batch)} records')
                                
                            except Exception as sub_e:
                                session.rollback()
                                logging.info(f'Sub-batch insert failed, trying individual inserts: {sub_e}')
                                
                                # Try individual inserts as last resort
                                for obj in valid_sub_batch:
                                    if obj.observation_period_id is None:
                                        continue
                                        
                                    try:
                                        individual_insert_sql = f"""
                                        INSERT INTO [{ObservationPeriod.__table__.schema}].[{ObservationPeriod.__tablename__}]
                                        ([observation_period_id], [person_id], [observation_period_start_date], 
                                        [observation_period_end_date], [period_type_concept_id])
                                        VALUES
                                        ({sql_val(obj.observation_period_id)}, {sql_val(obj.person_id)}, 
                                        {sql_val(obj.observation_period_start_date)}, {sql_val(obj.observation_period_end_date)}, 
                                        {sql_val(obj.period_type_concept_id)})
                                        """
                                        session.execute(text(individual_insert_sql))
                                        session.commit()
                                        success += 1
                                    except Exception as ind_e:
                                        session.rollback()
                                        logging.info(f'Failed to insert observation_period_id {obj.observation_period_id}: {ind_e}')
                                        continue
                        
                        if success > 0:
                            logging.info(f'Fallback method inserted {success} out of {len(omop_obs_periods)} records in batch {batch_num}')
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
                
                # Update last_person_id for next iteration
                if result:
                    last_person_id = result[-1][0]  # person_id from last row
                elif omop_obs_periods:
                    last_person_id = omop_obs_periods[-1].person_id
                
                batch_num += 1
                
                logging.info(f'Batch {batch_num - 1} complete. total_records inserted so far: {total_inserted}')
                
                # Break if we've reached the total_records for sample records
                if not all_records and total_inserted >= total_records:
                    break
            
            logging.info(f'\n=== FINAL RESULTS ===')
            logging.info(f'Successfully inserted {total_inserted} records into {ObservationPeriod.table_name()}')

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
            logging.error(f'Error inserting records into {ObservationPeriod.table_name()}: {e}')
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
            logging.info(f'total_records time taken: {duration.strftime("%H:%M:%S")}')
            logging.info('Session Closed')       

# Example Usage

if __name__=='__main__':
    obs=ObservationPeriod(schema='Target Schema')
    obs.set_source('Source Schema','Source Table')
    print(obs.get_source_fullname())
    print(obs.table_name())
    engine=obs.connect_db(os.getenv('DB_URL'),os.getenv('DB_NAME'))
    current=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path=os.path.join(current,'config.json')
    obs.set_config(config_path)
    obs.drop_table(engine)
    obs.create_table(engine)
    obs.insert_records(engine, all_records=True,batch_size=10000, sub_batch_size=1000)
