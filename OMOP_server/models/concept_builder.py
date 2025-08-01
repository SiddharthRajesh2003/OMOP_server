import pandas as pd
import os
from typing import Any
import logging
import datetime as dt


from sqlalchemy import text, Column, Integer, Date, VARCHAR
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError


from OMOP_server.utils.utility import sql_val
from OMOP_server.models.basetable import ParentBase
from OMOP_server.utils.logging_manager import get_logger

logger=get_logger(__name__)

class ConceptBuilder(ParentBase):
    """
    Download the Vocabularies from Athena:
    
    https://athena.ohdsi.org/vocabulary/list
    
    Use this class to build a concept lookup table that can be referenced in the concept columns in
    other OMOP tables as it uses the csv files from the downloaded portion to create the lookup 
    table in SQL Server
    """
    
    __tablename__='concept'
    
    concept_id = Column(Integer, primary_key=True, nullable=False)
    concept_name = Column(VARCHAR, nullable=False)
    domain_id = Column(VARCHAR, nullable=False)
    vocabulary_id = Column(VARCHAR, nullable=False)
    concept_class_id = Column(Integer, nullable = False)
    standard_concept = Column(VARCHAR, nullable = True)
    concept_code = Column(VARCHAR, nullable = False)
    valid_start_date = Column(Date, nullable=False)
    valid_end_date = Column(Date, nullable=False)
    invalid_reason = Column(VARCHAR, nullable=True)
    
    
    def __init__(self, schema, domain, path_to_folder, table_suffix='Concepts') -> None:
        
        """
        Initialize the ConceptBuilder
        
        Args:
            schema (str): Database schema name
            domain (str): OMOP domain (e.g., 'Condition', 'Drug', 'Procedure')
            path_to_folder (str): Path to folder containing Athena vocabulary CSV files
        """
        self.domain = domain
        self.__table__.schema = schema
        self.path = path_to_folder
        
        domain_clean = domain.replace(' ', '_') if domain else domain
        
        if domain and domain.upper()!='ALL':
            table_name= f'OMOP_{domain_clean}_{table_suffix}'
        else:
            table_name=table_suffix
        
        self.__table__.name= table_name
        self.__table__.schema=schema
        self.table_name = f'{schema}.{table_name}' if schema else table_name
        
        self.concept_file = os.path.join(self.path, 'concept.csv')

    
    def validate_files(self) -> bool:
        """Validate that required CSV files exist"""
        
        if not os.path.exists(self.concept_file):
            raise FileNotFoundError(f"concept.csv not found in : {self.path}")
        
        logger.info(f"Found concept.csv at {self.concept_file}")
        return True
    
    def load_concept_data(self)  -> pd.DataFrame | None:
        """Load concept data from CSV file"""
        try:
            logger.info(f'Loading concept file from {self.concept_file}')
            
            df=pd.read_csv(self.concept_file, sep='\t',
                           dtype={
                            'concept_id': 'int64',
                            'concept_name': 'str',
                            'domain_id': 'str',
                            'vocabulary_id': 'str',  
                            'concept_class_id': 'str',
                            'standard_concept': 'str',
                            'concept_code': 'str',
                            'valid_start_date': 'str',
                            'valid_end_date': 'str',
                            'invalid_reason': 'str'   
                           })
            df['valid_start_date']=pd.to_datetime(df['valid_start_date'])
            df['valid_end_date']=pd.to_datetime(df['valid_end_date'])
            
            if self.domain and self.domain !='ALL':
                original_count=len(df)
                df = df[df['domain_id'].str.lower() == self.domain.lower()]
                logger.info(f'Filtered from {original_count} to {len(df)} concepts for domain {self.domain}')
            else:
                df = df
                logger.info(f'No domain filtering applied. Using all{len(df)} concept relationships for domain: {self.domain}')
            
            logger.info(f'Loaded {len(df)} {self.domain} concept records')
            return df

        except Exception as e:
            logger.error(f"Error loading concept data: {str(e)}")
            raise
        
    @staticmethod 
    def create_table(engine) -> None:
        schema = ConceptBuilder.__table__.schema
    
        check_sql = f'''SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{ConceptBuilder.__table__.name}'
        '''
        
        table_create_sql=f'''
        CREATE TABLE [{schema}].[{ConceptBuilder.__table__.name}] (
            concept_id BIGINT NOT NULL,
            concept_name VARCHAR(255) NOT NULL,
            domain_id VARCHAR(20) NOT NULL,
            vocabulary_id VARCHAR(20) NOT NULL,
            concept_class_id VARCHAR(20) NOT NULL,
            standard_concept VARCHAR(1) NULL,
            concept_code VARCHAR(50)  NOT NULL,
            valid_start_date DATE NOT NULL,
            valid_end_date DATE NOT NULL,
            invalid_reason VARCHAR(1) NULL
        )
        '''
        
        pk_constraint_sql = f'''
        ALTER TABLE [{schema}].[{ConceptBuilder.__table__.name}]
        ADD CONSTRAINT PK_{ConceptBuilder.__table__.name}_concept_id
        PRIMARY KEY NONCLUSTERED (concept_id) NOT ENFORCED'''
        
        session=sessionmaker(bind=engine)()
        
        try:
            result=session.execute(text(check_sql))
            table_exists = result.scalar()>0
            
            if not table_exists:
                session.execute(text(table_create_sql))
                logging.info(f"Table {schema}.{ConceptBuilder.__table__.name} created successfully.")
                    
                session.execute(text(pk_constraint_sql))
                logging.info(f"Primary key constraint added to {schema}.{ConceptBuilder.__table__.name}")
            else:
                logging.info(f"Table {schema}.{ConceptBuilder.__table__.name} already exists.")
                
        except Exception as e:
            logging.error(f"Error creating table: {e}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def insert_data(engine, df, batch_size=10000, sub_batch_size=1000) -> Any:
        """
    Insert all concept records using the template pattern from ConceptBuilder
    
    Args:
        engine: SQLAlchemy engine
        df: DataFrame containing concept data
        batch_size: Number of records to process per batch
        sub_batch_size: Number of records for sub-batches when bulk insert fails
    """
    
        session = sessionmaker(bind=engine)()
        starttime = dt.datetime.now()
        logging.info(f'Insert started at: {starttime.strftime("%Y-%m-%d %H:%M:%S")}')
        schema = ConceptBuilder.__table__.schema
        table_name = ConceptBuilder.__table__.name
        
        try:
            total=len(df)
            logging.info(f'Total records to insert: {total}')
            
            if total == 0:
                logging.info("No records to insert")
                return 0
            
            existing_ids_sql = f"""
            SELECT concept_id from [{schema}].[{table_name}]
            """
            try:
                existing_result = session.execute(text(existing_ids_sql)).fetchall()
                existing_ids = {row[0] for row in existing_result}
                logging.info(f'Found {len(existing_ids)} existing concept records to skip')
            except:
                existing_ids = set()
                logging.info('No existing records found (table might be empty)')
            
            if existing_ids:
                original_count =total
                df = df[~df['concept_id'].isin(existing_ids)]
                logging.info(f'Filtered from {original_count} to {len(df)} records after removing duplicates')
                total = len(df)
            
            total_inserted = 0
            batch_num = 1
            consecutive_empty_batches = 0
            
            for start_idx in range(0, total, batch_size):
                end_idx = min(start_idx + batch_size, total)
                batch_df = df.iloc[start_idx:end_idx]
                
                logging.info(f'\n--- Processing Batch {batch_num} ---')
                logging.info(f'Progress: {total_inserted}/{total} ({(total_inserted/total)*100:.1f}%)')
                logging.info(f'Batch range: {start_idx} to {end_idx-1}')
                
                if batch_df.empty:
                    consecutive_empty_batches+=1
                    logging.info(f'Empty batch #{consecutive_empty_batches}')
                    if consecutive_empty_batches>=3:
                        logging.info('Multiple consecutive empty batches detected. Ending process')
                        break
                    batch_num+=1
                    continue
                    
                else:
                    consecutive_empty_batches = 0
                    
                logging.info(f'Processing {len(batch_df)} records in batch {batch_num}')
                
                concepts = []
                for _, row in batch_df.iterrows():
                    concept = {
                        'concept_id': int(row['concept_id']),
                        'concept_name': str(row['concept_name']) if pd.notna(row['concept_name']) else None,
                        'domain_id': str(row['domain_id']) if pd.notna(row['domain_id']) else None,
                        'vocabulary_id': str(row['vocabulary_id']) if pd.notna(row['vocabulary_id']) else None,
                        'concept_class_id': str(row['concept_class_id']) if pd.notna(row['concept_class_id']) else None,
                        'standard_concept': str(row['standard_concept']) if pd.notna(row['standard_concept']) else None,
                        'concept_code': str(row['concept_code']) if pd.notna(row['concept_code']) else None,
                        'valid_start_date': row['valid_start_date'] if pd.notna(row['valid_start_date']) else None,
                        'valid_end_date': row['valid_end_date'] if pd.notna(row['valid_end_date']) else None,
                        'invalid_reason': str(row['invalid_reason']) if pd.notna(row['invalid_reason']) else None
                    }
                    concepts.append(concept)
                logging.info(f'Created {len(concepts)} concept objects')
                
                if not concepts:
                    logging.error(f'No valid conceptts found in batch {batch_num}, skipping...')
                    batch_num+=1
                    continue
                
                try:
                    insert_sql = f"""
                    INSERT INTO [{schema}].[{table_name}] (
                        concept_id, concept_name, domain_id, vocabulary_id, concept_class_id,
                        standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason
                    ) VALUES (
                        :concept_id, :concept_name, :domain_id, :vocabulary_id, :concept_class_id,
                        :standard_concept, :concept_code, :valid_start_date, :valid_end_date, :invalid_reason
                    )
                    """
                    
                    session.execute(text(insert_sql), concepts)
                    session.commit()
                    logging.info(f'✓ Successfully inserted batch {batch_num}: {len(concepts)} records')
                    total_inserted+=len(concepts)
                    
                    for concept in concepts:
                        existing_ids.add(concept['concept_id'])
                
                except ProgrammingError as e:
                    session.rollback()
                    if 'DEFAULT VALUES' in str(e) or 'Incorrect syntax near' in str(e):
                        logging.info(f'Bulk insert failed for batch {batch_num}. Using alternative insert method ...')
                        
                        success = 0
                        for i in range(0, len(concepts), sub_batch_size):
                            sub_batch = concepts[i:i+sub_batch_size]
                            
                            valid_sub_batch = [obj for obj in sub_batch if obj['concept_id'] is not None]
                            
                            if not valid_sub_batch:
                                continue
                            
                            try:
                                select_parts = []
                                for obj in valid_sub_batch:
                                    select_parts.append(
                                        f"SELECT CAST({sql_val(obj['concept_id'])} AS BIGINT) AS concept_id,"
                                        f"CAST({sql_val(obj['concept_name'])} AS VARCHAR) AS concept_name,"
                                        f"CAST({sql_val(obj['domain_id'])} AS VARCHAR) AS domain_id,"
                                        f"CAST({sql_val(obj['vocabulary_id'])} AS VARCHAR) AS vocabulary_id,"
                                        f"CAST({sql_val(obj['concept_class_id'])} AS VARCHAR) AS concept_class_id,"
                                        f"CAST({sql_val(obj['standard_concept'])} AS VARCHAR) AS standard_concept,"
                                        f"CAST({sql_val(obj['concept_code'])} AS VARCHAR) AS concept_code,"
                                        f"CAST({sql_val(obj['valid_start_date'])} AS DATE) AS valid_start_date,"
                                        f"CAST({sql_val(obj['valid_end_date'])} AS DATE) AS valid_end_date,"
                                        f"CAST({sql_val(obj['invalid_reason'])} AS VARCHAR) AS invalid_reason"
                                    )
                                
                                select_str = " UNION ALL ".join(select_parts)
                                insert_sql = f"""
                                INSERT INTO [{schema}].[{table_name}] (
                                    concept_id, concept_name, domain_id, vocabulary_id, concept_class_id,
                                    standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason
                                ) {select_str}
                                """
                                
                                session.execute(text(insert_sql))
                                success += len(valid_sub_batch)
                            
                            except Exception as sub_e:
                                logging.info(f'Sub-batch insert failed, trying individual inserts: {sub_e}')
                            
                                for obj in valid_sub_batch:
                                    if obj['concept_id'] is None:
                                        continue
                                    try:
                                        individual_insert_sql = f"""
                                        INSERT INTO [{schema}].[{table_name}](
                                        [concept_id], [concept_name], [domain_id], [vocabulary_id],
                                        [concept_class_id], [standard_concept], [concept_code], [valid_start_date],
                                        [valid_end_date], [invalid_reason])
                                        VALUES 
                                        ({sql_val(obj['concept_id'])}, {sql_val(obj['concept_name'])},
                                        {sql_val(obj['domain_id'])}, {sql_val(obj['vocabulary_id'])},
                                        {sql_val(obj['concept_class_id'])}, {sql_val(obj['standard_concept'])},
                                        {sql_val(obj['concept_code'])}, {sql_val(obj['valid_start_date'])},
                                        {sql_val(obj['valid_end_date'])}, {sql_val(obj['invalid_reason'])})
                                        """
                                        session.execute(text(individual_insert_sql))
                                        success += 1
                                    
                                    except Exception as ind_e:
                                        logging.info(f'Failed to insert concept_id {obj["concept_id"]}: {ind_e}')
                                        continue
                        if success > 0:
                            session.commit()
                            logging.info(f'Successfully inserted {success} out of {len(concepts)} records in batch {batch_num}')
                            total_inserted+=success
                        
                            for concept in concepts[:success]:
                                existing_ids.add(concept['concept_id'])
                        
                        else:
                            session.rollback()
                            logging.info(f'Failed to insert any records in batch {batch_num}, rolling back...')
                            raise Exception(f'No records could be inserted in this batch')
                    else:
                        logging.info(f'Non-bulk insert error in batch {batch_num}: {e}')
                
                batch_num+=1
                
            logging.info(f'\n=== FINAL RESULTS ===')
            logging.info(f'Successfully inserted {total_inserted} records into {schema}.{table_name}')
            logging.info(f'Total available records: {total}')
            logging.info(f'Completion: {(total_inserted/total)*100:.1f}%' if total > 0 else 'N/A')
            
            return total_inserted
        
        except Exception as e:
            session.rollback()
            logging.error(f'Error inserting records into {schema}.{table_name}: {e}')
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
            duration_time = dt.time(hour=hours, minute=minutes, second=seconds)
            logging.info(f'Total time taken: {duration_time.strftime("%H:%M:%S")}')
            logging.info('Session Closed')

    @staticmethod
    def drop_table(engine) -> None:
        '''
        Drops the OMOP_Persons table if it exists.
        Args:
            engine: SQLAlchemy engine
        '''
        schema = ConceptBuilder.__table__.schema
        if not schema:
            raise ValueError("Schema must be set before dropping the table.")
        
        drop_sql = f"""
        IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                   WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{ConceptBuilder.__table__.name}')
        BEGIN
            DROP TABLE [{schema}].[{ConceptBuilder.__table__.name}]
        END
        """
        
        try:
            with engine.connect() as conn:
                conn.execute(text(drop_sql))
                logging.info(f"Table {schema}.{ConceptBuilder.__table__.name} dropped successfully.")
        except Exception as e:
            logging.error(f"Error dropping table: {e}")
            raise

if __name__=='__main__':
    condition = ConceptBuilder(schema='Target Schema', domain='Condition', path_to_folder='path to the downloaded athena files/OMOP_Vocab')
    print(condition.table_name)
    
    condition.validate_files()
    
    df=condition.load_concept_data()
    
    engine=condition.connect_db(os.getenv('DB_URL'), os.getenv('DB_NAME'))
    condition.create_table(engine)
    
    condition.insert_data(engine, df, batch_size=500, sub_batch_size=250)