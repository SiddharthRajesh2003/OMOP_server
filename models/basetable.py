import logging
from typing import Any
import pandas as pd
import os


from sqlalchemy import text
from sqlalchemy.orm import declarative_base


from omop_server.utils.config_manager import ConfigManager
from omop_server.utils.connect import connect_db
from omop_server.utils.logging_manager import get_logger

Base = declarative_base()
logger = get_logger(__name__)

class ParentBase(Base):
    
    __abstract__ = True
    __source_schema__ = None
    __source_table__ = None
    __config_manager__ = None

    @classmethod
    def set_config(cls, config_file_path:str='config.json') -> str:
        """
        Set the configuration manager for the Person class.
        
        Args:
            config_file_path (str): Path to the configuration file
        """
        cls.__config_manager__ = ConfigManager(config_file_path)
        return f"Configuration loaded from: {config_file_path}"
    
    @classmethod
    def get_config(cls)->ConfigManager:
        """
        Get the configuration manager instance.
        
        Returns:
            ConfigManager: Configuration manager instance
        """
        if cls.__config_manager__ is None:
            cls.__config_manager__ = ConfigManager()
        return cls.__config_manager__
    
        
    @classmethod
    def get_source_fullname(cls) -> str|None:
        '''
        Returns the full source name in the format 'schema.table'.
        '''
        if cls.__source_schema__ and cls.__source_table__:
            return f"{cls.__source_schema__}.{cls.__source_table__}"
        return None
    
        
    @staticmethod
    def connect_db(db_url, db_name) -> Any:
        """
        Connects to the database using the provided URL and database name.
        Returns a SQLAlchemy engine.
        """
        engine = connect_db(db_url, db_name)
        return engine
    
        
    @classmethod
    def table_name(cls) -> str:
        '''
        Prints the full table name including schema.
        Returns:
            str: Full table name in the format '[schema].[tablename]'
        '''
        return f"[{cls.__table__.schema}].[{cls.__tablename__}]"
    
        
    @classmethod
    def set_source(cls, schema, table) -> str:
        '''
        Sets the source schema and table for this class.
        Args:
            schema (str): The schema name.
            table (str): The table name.
        '''
        cls.__source_schema__ = schema
        cls.__source_table__ = table
        return f"Source set to: {schema}.{table}"
    
        
    @staticmethod
    def query(engine, query) -> pd.DataFrame | None:
        '''
        Executes a query on the database and returns the results.
        Args:
        engine: SQLAlchemy engine
        query: SQL query string to execute
        Returns:
        DataFrame: Pandas DataFrame containing the query results
        '''
        try:
            df=pd.read_sql(text(query),engine)
            return df
        except Exception as e:
            logging.error(f'Error executing query: {e}')
            return None
        
    @classmethod
    def drop_table(cls,engine) -> None:
        '''
        Drops the OMOP table if it exists.
        Args:
            engine: SQLAlchemy engine
        '''
        schema = cls.__table__.schema
        if not schema:
            raise ValueError("Schema must be set before dropping the table.")
        
        drop_sql = f"""
        IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                   WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{cls.__tablename__}')
        BEGIN
            DROP TABLE [{schema}].[{cls.__tablename__}]
        END
        """
        
        try:
            with engine.connect() as conn:
                conn.execute(text(drop_sql))
                logging.info(f"Table {schema}.{cls.__tablename__} dropped successfully.")
        except Exception as e:
            logging.error(f"Error dropping table: {e}")
            raise