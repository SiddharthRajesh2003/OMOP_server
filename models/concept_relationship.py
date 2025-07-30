import pandas as pd
import os
from typing import Any
import logging
import datetime as dt


from sqlalchemy import text, Column, Integer, Date, VARCHAR, PrimaryKeyConstraint
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError


from omop_server.models.concept_builder import ConceptBuilder
from omop_server.utils.utility import sql_val
from omop_server.models.basetable import ParentBase
from omop_server.utils.logging_manager import get_logger

logger=get_logger(__name__)


class ConceptRelationship(ParentBase):
    """
    Download the Vocabularies from Athena:
    
    https://athena.ohdsi.org/vocabulary/list
    
    Use this class to build a concept relationship lookup table that can be referenced in the concept columns in
    other OMOP tables as it uses the csv files from the downloaded portion to create the lookup 
    table in SQL Server
    """
    
    __tablename__ = 'concept_relationship'
    __table_args__ = (
        PrimaryKeyConstraint('concept_id_1', 'concept_id_2', 'relationship_id'),
    )
    concept_id_1 = Column(Integer, nullable = False)
    concept_id_2 = Column(Integer, nullable = False)
    relationship_id = Column(VARCHAR, nullable = False)
    valid_start_date = Column(Date, nullable = False)
    valid_end_date = Column(Date, nullable = False)
    invalid_reason = Column(VARCHAR, nullable = True)
    
    def __init__(self, schema, domain, path_to_folder, table_suffix='Concept_Relationships') -> None:
        
        ConceptRelationship.domain = domain
        ConceptRelationship.file_path = path_to_folder
        
        self.__table__.schema = schema
        self.domain = domain
        self.file_path = path_to_folder

        if domain and domain.upper()!='ALL':
            table_name= f'OMOP_{domain}_{table_suffix}'
        else:
            table_name=table_suffix

        self.__table__.name = table_name
        self.table_name = f'{schema}.{table_name}'
        
        self.cr_file = os.path.join(self.file_path, 'concept_relationship.csv')
    
    def validate_files(self) -> bool:
        """Validate that required CSV files exist"""
        
        if not os.path.exists(self.cr_file):
            raise FileNotFoundError(f"concept_relationship.csv not found in : {self.file_path}")
        
        logger.info(f"Found concept_relationship.csv at {self.cr_file}")
        return True
    
    def load_concept_data(self)  -> pd.DataFrame | None:
        """Load concept data from CSV file"""
        try:
            logger.info(f'Loading concept_relationship file from {self.cr_file}')
            
            df=pd.read_csv(self.cr_file, sep='\t',
                           dtype={
                            'concept_id_1': 'int64',
                            'concept_id_2': 'int64',
                            'relationship_id': 'str',
                            'valid_start_date': 'str',
                            'valid_end_date': 'str',
                            'invalid_reason': 'str'   
                           })
            df['valid_start_date']=pd.to_datetime(df['valid_start_date'])
            df['valid_end_date']=pd.to_datetime(df['valid_end_date'])
            
            concept_builder = ConceptBuilder(schema = self.__table__.schema, domain = self.domain, path_to_folder=self.file_path)
            concept_builder.validate_files()
            
            concepts_df = concept_builder.load_concept_data()
            domain_concept_ids = set(concepts_df['concept_id'])
            
            if self.domain and self.domain.upper() != 'ALL':
                original_count = len(df)
                filtered_cr_df = df[df['concept_id_1'].isin(domain_concept_ids)
                                 & df['concept_id_2'].isin(domain_concept_ids)
                                 ]
                logger.info(f'Filtered from {original_count} to {len(filtered_cr_df)} concept relationships for domain {self.domain}')
            else:
                filtered_cr_df = df
                logger.info(f'No domain filtering applied. Using all {len(filtered_cr_df)} concept relationships.')
            
            
            return filtered_cr_df

        except Exception as e:
            logger.error(f"Error loading concept data: {str(e)}")
            raise
    
    @staticmethod 
    def create_table(engine) -> None:
        schema = ConceptRelationship.__table__.schema
        name = ConceptRelationship.__table__.name
        check_sql = f'''SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{name}'
        '''
        
        table_create_sql=f'''
        CREATE TABLE [{schema}].[{name}] (
            concept_id_1 BIGINT NOT NULL,
            concept_id_2 BIGINT NOT NULL,
            relationship_id VARCHAR(20) NOT NULL,
            valid_start_date DATE NOT NULL,
            valid_end_date DATE NOT NULL,
            invalid_reason VARCHAR(1) NULL
        )
        '''
        session=sessionmaker(bind=engine)()
        
        try:
            result=session.execute(text(check_sql))
            table_exists = result.scalar()>0
            
            if not table_exists:
                session.execute(text(table_create_sql))
                logging.info(f"Table {schema}.{name} created successfully.")

            else:
                logging.info(f"Table {schema}.{name} already exists.")
                
        except Exception as e:
            logging.error(f"Error creating table: {e}")
            raise
        finally:
            session.close()
    
    @staticmethod
    def get_existing_concept_ids(engine) -> set:
        """
        Get existing concept IDs from the database concept table
        
        Args:
            engine: SQLAlchemy engine
            schema: Database schema name
            concept_table_name: Name of the concept table
            
        Returns:
            set: Set of existing concept IDs
        """
        concept_builder = ConceptBuilder(schema=ConceptRelationship.__table__.schema, domain=ConceptRelationship.domain, path_to_folder=ConceptRelationship.file_path)
        schema = concept_builder.__table__.schema
        table_name = concept_builder.__table__.name
        session = sessionmaker(bind = engine)()
        try:
            existing_concepts_sql = f"""
            SELECT DISTINCT concept_id FROM [{schema}].[{table_name}]
            """
            
            results = session.execute(text(existing_concepts_sql)).fetchall()
            existing_concept_ids = {row[0] for row in results}
            logger.info(f'Found {len(existing_concept_ids)} existing concept IDs in database')
            return existing_concept_ids
        
        except Exception as e:
            logger.error(f'Error getting existing concept IDs: {e}')
            return set()
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
        schema = ConceptRelationship.__table__.schema
        table_name = ConceptRelationship.__table__.name
        concept_builder = ConceptBuilder(schema=schema, domain=ConceptRelationship.domain, path_to_folder=ConceptRelationship.file_path)
        
        try:
            total = len(df)
            logging.info(f'Total records to available for insert: {total}')
            
            if total == 0:
                logging.info("No records to insert")
                return 0
            
            existing_concept_ids = ConceptRelationship.get_existing_concept_ids(engine)
            if not existing_concept_ids:
                logging.error("No existing concept IDs found in database. Cannot proceed.")
                return 0
            
            # Filter dataframe to only include relationships where both concepts exist
            original_count = len(df)
            df = df[
                df['concept_id_1'].isin(existing_concept_ids) & 
                df['concept_id_2'].isin(existing_concept_ids)
            ]
            
            logging.info(f'Filtered from {original_count} to {len(df)} relationships')
            logging.info(f'Only relationships between your {len(existing_concept_ids)} existing concepts will be inserted')
            logging.info(f'Records to insert: {len(df)}')
            
            if len(df) == 0:
                logging.info("No valid relationships found after filtering")
                return 0
            
            total = len(df)
            
            existing_ids_sql = f"""
            SELECT concept_id_1, concept_id_2, relationship_id
            from [{schema}].[{table_name}] cr
            INNER JOIN [{concept_builder.__table__.schema}].[{concept_builder.__table__.name}] c1
            ON cr.concept_id_1 = c1.concept_id
            INNER JOIN [{concept_builder.__table__.schema}].[{concept_builder.__table__.name}] c2
            ON cr.concept_id_2 = c2.concept_id
            """
            try:
                existing_result = session.execute(text(existing_ids_sql)).fetchall()
                existing_keys = {(row[0], row[1], row[2]) for row in existing_result}
                logging.info(f'Found {len(existing_keys)} existing concept records to skip')
            except Exception as e:
                existing_keys = set()
                logging.info(f'No existing records found (table might be empty): {e}')
            
            if existing_keys:
                original_count = total
                df = df[~df.apply(lambda x: (x['concept_id_1'], x['concept_id_2'], x['relationship_id']) in existing_keys, axis=1)]
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
                    consecutive_empty_batches += 1
                    logging.info(f'Empty batch #{consecutive_empty_batches}')
                    if consecutive_empty_batches >= 3:
                        logging.info('Multiple consecutive empty batches detected. Ending process')
                        break
                    batch_num += 1
                    continue
                else:
                    consecutive_empty_batches = 0
                    
                logging.info(f'Processing {len(batch_df)} records in batch {batch_num}')
                
                relationships = []
                for _, row in batch_df.iterrows():
                    relationship = {
                        'concept_id_1': int(row['concept_id_1']),
                        'concept_id_2': int(row['concept_id_2']),
                        'relationship_id': str(row['relationship_id']) if pd.notna(row['relationship_id']) else None,
                        'valid_start_date': row['valid_start_date'] if pd.notna(row['valid_start_date']) else None,
                        'valid_end_date': row['valid_end_date'] if pd.notna(row['valid_end_date']) else None,
                        'invalid_reason': str(row['invalid_reason']) if pd.notna(row['invalid_reason']) else None
                    }
                    relationships.append(relationship)
                logging.info(f'Created {len(relationships)} concept objects')
                
                if not relationships:
                    logging.error(f'No valid concepts found in batch {batch_num}, skipping...')
                    batch_num += 1
                    continue
                
                try:
                    insert_sql = f"""
                    INSERT INTO [{schema}].[{table_name}] (
                        concept_id_1, concept_id_2, relationship_id, 
                        valid_start_date, valid_end_date, invalid_reason
                    ) VALUES (
                        :concept_id_1, :concept_id_2, :relationship_id,
                        :valid_start_date, :valid_end_date, :invalid_reason
                    )
                    """
                    
                    session.execute(text(insert_sql), relationships)
                    session.commit()
                    logging.info(f'✓ Successfully inserted batch {batch_num}: {len(relationships)} records')
                    total_inserted += len(relationships)
                    
                    for rel in relationships:
                        existing_keys.add((rel['concept_id_1'], rel['concept_id_2'], rel['relationship_id']))
                
                except ProgrammingError as e:
                    session.rollback()
                    if 'DEFAULT VALUES' in str(e) or 'Incorrect syntax near' in str(e):
                        logging.info(f'Bulk insert failed for batch {batch_num}. Using alternative insert method ...')
                        
                        success = 0
                        successful_relationships = []
                        
                        for i in range(0, len(relationships), sub_batch_size):
                            sub_batch = relationships[i:i+sub_batch_size]
                            
                            valid_sub_batch = [obj for obj in sub_batch if obj['concept_id_1'] is not None and obj['concept_id_2'] is not None and obj['relationship_id'] is not None]                            
                            if not valid_sub_batch:
                                continue
                            
                            try:
                                select_parts = []
                                for obj in valid_sub_batch:
                                    select_parts.append(
                                        f"SELECT CAST({sql_val(obj['concept_id_1'])} AS BIGINT) AS concept_id_1,"
                                        f"CAST({sql_val(obj['concept_id_2'])} AS BIGINT) AS concept_id_2,"
                                        f"CAST({sql_val(obj['relationship_id'])} AS VARCHAR) AS relationship_id,"
                                        f"CAST({sql_val(obj['valid_start_date'])} AS DATE) AS valid_start_date,"
                                        f"CAST({sql_val(obj['valid_end_date'])} AS DATE) AS valid_end_date,"
                                        f"CAST({sql_val(obj['invalid_reason'])} AS VARCHAR) AS invalid_reason"
                                    )
                                
                                select_str = " UNION ALL ".join(select_parts)
                                insert_sql = f"""
                                INSERT INTO [{schema}].[{table_name}] (
                                    concept_id_1, concept_id_2, relationship_id, 
                                    valid_start_date, valid_end_date, invalid_reason
                                ) {select_str}
                                """
                                
                                session.execute(text(insert_sql))
                                success += len(valid_sub_batch)
                                successful_relationships.extend(valid_sub_batch)
                            
                            except Exception as sub_e:
                                logging.info(f'Sub-batch insert failed, trying individual inserts: {sub_e}')
                            
                                for obj in valid_sub_batch:
                                    # Fixed condition - should be OR, not AND
                                    if obj['concept_id_1'] is None or obj['concept_id_2'] is None:
                                        continue
                                    try:
                                        individual_insert_sql = f"""
                                        INSERT INTO [{schema}].[{table_name}](
                                        [concept_id_1], [concept_id_2], [relationship_id], 
                                        [valid_start_date], [valid_end_date], [invalid_reason])
                                        VALUES 
                                        ({sql_val(obj['concept_id_1'])}, {sql_val(obj['concept_id_2'])},
                                        {sql_val(obj['relationship_id'])}, {sql_val(obj['valid_start_date'])},
                                        {sql_val(obj['valid_end_date'])}, {sql_val(obj['invalid_reason'])})
                                        """
                                        session.execute(text(individual_insert_sql))
                                        success += 1
                                        successful_relationships.append(obj)
                                    
                                    except Exception as ind_e:
                                        # Fixed reference - was obj["concept_id"]
                                        logging.info(f'Failed to insert concept_id_1 {obj["concept_id_1"]}, concept_id_2 {obj["concept_id_2"]}: {ind_e}')
                                        continue
                        
                        if success > 0:
                            session.commit()
                            logging.info(f'Successfully inserted {success} out of {len(relationships)} records in batch {batch_num}')
                            total_inserted += success
                        
                            # Add all successfully inserted relationships to existing_keys
                            for rel in successful_relationships:
                                existing_keys.add((rel['concept_id_1'], rel['concept_id_2'], rel['relationship_id']))
                        
                        else:
                            session.rollback()
                            logging.info(f'Failed to insert any records in batch {batch_num}, rolling back...')
                            raise Exception(f'No records could be inserted in this batch')
                    else:
                        logging.error(f'Non-bulk insert error in batch {batch_num}: {e}')
                        raise
                
                batch_num += 1
                
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
            
        
if __name__=='__main__':
    cr = ConceptRelationship(schema='Your target schema', domain='Condition', path_to_folder="Path to the downloaded Athena files/OMOP_Vocab")
    
    cr.validate_files()
    df=cr.load_concept_data()
    
    engine = cr.connect_db(os.getenv('DB_URL'), os.getenv('DB_NAME'))
    cr.drop_table(engine)
    cr.create_table(engine)
    
    cr.insert_data(engine, df)