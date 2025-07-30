import json
import os
from typing import Dict, List, Any, Optional


class ConfigManager:
    """
    Configuration manager for OMOP table source column mappings.
    Handles loading and accessing column configuration from JSON file.
    """
    def __init__(self, config_file_path:str = 'config.json'):
        """
        Intialize the configuration manager.
        Args:
        config_file_path(str): Path to the JSON Configuration file
        """
        self.config_file_path=config_file_path
        self.config= self.load_config()
    
    
    def load_config(self) -> Dict[str, Any]:
        """
        Load Configuration from the JSON file.
        Returns:
            Dict[str, Any]: Configuration Dictionary
            
        Raises:
            FileNotFoundError: If config file doesn't exist.
            json.JSONDecodeError: If config file has invalid JSON
        """
        if not os.path.exists(self.config_file_path):
            raise FileNotFoundError(f'Configuration file not found: {self.config_file_path}')
        
        try:
            with open(self.config_file_path,'r',encoding='utf-8') as file:
                config=json.load(file)
            return config
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f'Invalid JSON format in config file: {e}')
    
    def reload_config(self)->None:
        """
        Reload Configuration from file. Used when config file is updated
        """
        self.config=self.load_config()
    
    def get_source_columns(self,table_name)-> Dict[str,str]:
        """
        Gets the source column mappings.
        
        Args:
            table_name: The OMOP table which is being implemented

        Returns:
            Dict[str,str]: Dictionary Mapping OMOP fields to source column names
        """
        return self.config["tables"].get(table_name,{}).get("source_columns",{})
    
    def get_required_columns(self, table_name)->List[str]:
        """
        Gets the list of required source columns.
        
        Returns:
            List(str): List of required column names
        """
        return self.config["tables"].get(table_name,{}).get("required_columns",[])
    
    
    def get_optional_columns(self, table_name) -> List[str]:
        """
        Get list of optional source columns.
        
        Returns:
            List[str]: List of optional column names
        """
        return self.config["tables"].get(table_name,{}).get('optional_columns', [])
    
    
    def get_all_columns(self, table_name) -> List[str]:
        """
        Get all configured source column names (required + optional).
        
        Returns:
            List[str]: List of all column names
        """
        required = self.get_required_columns(table_name)
        optional = self.get_optional_columns(table_name)
        return list(set(required + optional))
    
    def get_column_mapping(self, table_name,column_name: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed mapping information for a specific column.
        
        Args:
            column_name (str): Source column name
            
        Returns:
            Optional[Dict[str, Any]]: Column mapping details or None if not found
        """
        return self.config["tables"].get(table_name,{}).get('column_mappings', {}).get(column_name)
    
    def get_omop_field_for_column(self, table_name,column_name: str) -> Optional[str]:
        """
        Get the OMOP field name for a given source column.
        
        Args:
            column_name (str): Source column name
            
        Returns:
            Optional[str]: OMOP field name or None if not found
        """
        mapping = self.get_column_mapping(table_name, column_name)
        return mapping.get('omop_field') if mapping else None
    
    def get_mapping_function_for_column(self, table_name, column_name: str) -> Optional[str]:
        """
        Get the mapping function name for a given source column.
        
        Args:
            column_name (str): Source column name
            
        Returns:
            Optional[str]: Mapping function name or None if not specified
        """
        mapping = self.get_column_mapping(table_name,column_name)
        return mapping.get('mapping_function') if mapping else None
    
    def validate_config(self, table_name) -> List[str]:
        """
        Validate the configuration for consistency and completeness.
        
        Returns:
            List[str]: List of validation errors (empty if valid)
        """
        errors = []
        table_cfg = self.config["tables"].get(table_name, {})
        required_sections = ['source_columns', 'required_columns', 'column_mappings']
        for section in required_sections:
            if section not in table_cfg:
                errors.append(f"Missing required section: {section}")
        required_cols = self.get_required_columns(table_name)
        column_mappings = table_cfg.get('column_mappings', {})
        for col in required_cols:
            if col not in column_mappings:
                errors.append(f"Required column '{col}' missing from column_mappings")
        source_cols = self.get_source_columns(table_name)
        for omop_field, source_col in source_cols.items():
            if source_col not in column_mappings:
                errors.append(f"Source column '{source_col}' from source_columns missing from column_mappings")
        return errors

    def config_summary(self,table_name)->str:
        """
        Get a summary of the current configuration.
        
        Returns:
            str: Formatted configuration summary
        """
        source_cols = self.get_source_columns(table_name)
        required_cols = self.get_required_columns(table_name)
        optional_cols = self.get_optional_columns(table_name)
        
        summary = f"""
        Configuration Summary:
        =====================
        Source Columns Mapped: {len(source_cols)}
        Required Columns: {len(required_cols)}
        Optional Columns: {len(optional_cols)}
        Total Columns: {len(self.get_all_columns())}

        Column Mappings:
        {'-' * 50}
        """
        for omop_field, source_col in source_cols.items():
            mapping = self.get_column_mapping(source_col)
            func = mapping.get('mapping_function', 'None') if mapping else 'None'
            summary += f"{omop_field:20} <- {source_col:25} [Function: {func}]\n"
        
        return summary
    
# Example Usage
'''
if __name__=='__main__':
    current=os.path.dirname(os.path.abspath(__file__))
    config_path=os.path.join(current,'config.json')
    con=ConfigManager(config_path)
    columns=con.get_all_columns('person_id')
    print(columns)'''