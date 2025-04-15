import pandas as pd
import numpy as np
import sqlite3
import re
from datetime import datetime

class DataValidator:
    """
    A class to validate and process data for the Teaching Organization Analytics application.
    Provides comprehensive validation for user-imported data before database insertion.
    """
    
    def __init__(self, db_path='data/teaching_analytics.db'):
        """Initialize the validator with database connection"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
        # Define validation rules for each table
        self.validation_rules = {
            'clients': {
                'required_fields': ['name'],
                'data_types': {
                    'client_id': 'integer',
                    'name': 'string',
                    'industry': 'string',
                    'size': 'string',
                    'region': 'string',
                    'contact_person': 'string',
                    'email': 'email',
                    'phone': 'phone',
                    'first_engagement_date': 'date',
                    'last_engagement_date': 'date',
                    'total_spend': 'float',
                    'notes': 'string'
                },
                'value_constraints': {
                    'size': ['Small', 'Medium', 'Large', 'Enterprise']
                }
            },
            'programs': {
                'required_fields': ['name'],
                'data_types': {
                    'program_id': 'integer',
                    'name': 'string',
                    'description': 'string',
                    'category': 'string',
                    'delivery_mode': 'string',
                    'duration': 'integer',
                    'base_price': 'float',
                    'min_participants': 'integer',
                    'max_participants': 'integer',
                    'trainer_cost_per_session': 'float',
                    'materials_cost_per_participant': 'float',
                    'active': 'boolean',
                    'creation_date': 'date',
                    'last_updated': 'date'
                },
                'value_constraints': {
                    'delivery_mode': ['In-Person', 'Virtual', 'Hybrid'],
                    'active': [0, 1]
                }
            },
            'enrollments': {
                'required_fields': ['program_id', 'client_id'],
                'data_types': {
                    'enrollment_id': 'integer',
                    'program_id': 'integer',
                    'client_id': 'integer',
                    'start_date': 'date',
                    'end_date': 'date',
                    'location': 'string',
                    'delivery_mode': 'string',
                    'num_participants': 'integer',
                    'revenue': 'float',
                    'trainer_cost': 'float',
                    'logistics_cost': 'float',
                    'venue_cost': 'float',
                    'utilities_cost': 'float',
                    'materials_cost': 'float',
                    'status': 'string',
                    'feedback_score': 'float',
                    'notes': 'string'
                },
                'value_constraints': {
                    'delivery_mode': ['In-Person', 'Virtual', 'Hybrid'],
                    'status': ['Scheduled', 'Completed', 'Cancelled'],
                    'feedback_score': {'min': 0, 'max': 5}
                },
                'foreign_keys': {
                    'program_id': {'table': 'programs', 'column': 'program_id'},
                    'client_id': {'table': 'clients', 'column': 'client_id'}
                }
            },
            'opportunities': {
                'required_fields': ['client_id', 'program_id'],
                'data_types': {
                    'opportunity_id': 'integer',
                    'client_id': 'integer',
                    'program_id': 'integer',
                    'potential_revenue': 'float',
                    'estimated_participants': 'integer',
                    'stage': 'string',
                    'probability': 'float',
                    'expected_close_date': 'date',
                    'actual_close_date': 'date',
                    'created_date': 'date',
                    'last_updated': 'date',
                    'owner': 'string',
                    'notes': 'string'
                },
                'value_constraints': {
                    'stage': ['Lead', 'Prospect', 'Proposal', 'Negotiation', 'Closed Won', 'Closed Lost'],
                    'probability': {'min': 0, 'max': 100}
                },
                'foreign_keys': {
                    'program_id': {'table': 'programs', 'column': 'program_id'},
                    'client_id': {'table': 'clients', 'column': 'client_id'}
                }
            }
        }
    
    def validate_dataframe(self, df, table_name):
        """
        Validate a DataFrame against the rules for a specific table
        
        Args:
            df: pandas DataFrame to validate
            table_name: name of the table to validate against
            
        Returns:
            tuple: (is_valid, errors, warnings, processed_df)
        """
        if table_name not in self.validation_rules:
            return False, [f"Unknown table: {table_name}"], [], df
        
        rules = self.validation_rules[table_name]
        errors = []
        warnings = []
        processed_df = df.copy()
        
        # Check required fields
        for field in rules['required_fields']:
            if field not in df.columns:
                errors.append(f"Missing required field: {field}")
            elif df[field].isnull().any():
                null_count = df[field].isnull().sum()
                errors.append(f"Field '{field}' contains {null_count} null values")
        
        if errors:
            return False, errors, warnings, df
        
        # Process and validate data types
        for column in df.columns:
            if column in rules['data_types']:
                expected_type = rules['data_types'][column]
                try:
                    processed_df = self._validate_and_convert_column(processed_df, column, expected_type, errors, warnings)
                except Exception as e:
                    errors.append(f"Error processing column '{column}': {str(e)}")
        
        # Validate value constraints
        for column, allowed_values in rules.get('value_constraints', {}).items():
            if column in df.columns:
                if isinstance(allowed_values, list):
                    invalid_values = df[~df[column].isin(allowed_values) & ~df[column].isnull()][column].unique()
                    if len(invalid_values) > 0:
                        errors.append(f"Column '{column}' contains invalid values: {', '.join(map(str, invalid_values))}. Allowed values: {', '.join(map(str, allowed_values))}")
                elif isinstance(allowed_values, dict) and 'min' in allowed_values and 'max' in allowed_values:
                    min_val = allowed_values['min']
                    max_val = allowed_values['max']
                    invalid_mask = ((df[column] < min_val) | (df[column] > max_val)) & ~df[column].isnull()
                    if invalid_mask.any():
                        invalid_count = invalid_mask.sum()
                        errors.append(f"Column '{column}' contains {invalid_count} values outside allowed range ({min_val} to {max_val})")
        
        # Validate foreign keys
        for column, fk_info in rules.get('foreign_keys', {}).items():
            if column in df.columns:
                foreign_table = fk_info['table']
                foreign_column = fk_info['column']
                
                # Get all valid foreign keys from the referenced table
                self.cursor.execute(f"SELECT {foreign_column} FROM {foreign_table}")
                valid_keys = set([row[0] for row in self.cursor.fetchall()])
                
                # Check if all values in the column exist in the referenced table
                invalid_keys = set(df[column].dropna().unique()) - valid_keys
                if invalid_keys:
                    errors.append(f"Column '{column}' contains {len(invalid_keys)} values that don't exist in {foreign_table}.{foreign_column}: {', '.join(map(str, list(invalid_keys)[:5]))}{' and more...' if len(invalid_keys) > 5 else ''}")
        
        return len(errors) == 0, errors, warnings, processed_df
    
    def _validate_and_convert_column(self, df, column, expected_type, errors, warnings):
        """Validate and convert a column to the expected data type"""
        if column not in df.columns:
            return df
        
        # Make a copy to avoid SettingWithCopyWarning
        result_df = df.copy()
        
        if expected_type == 'integer':
            # Try to convert to integer, handling non-numeric values
            try:
                # First convert to float to handle NaN values
                result_df[column] = pd.to_numeric(result_df[column], errors='coerce')
                # Then convert to integer, but keep NaN as NaN
                mask = result_df[column].notna()
                result_df.loc[mask, column] = result_df.loc[mask, column].astype(int)
                
                # Check how many values were coerced to NaN
                if result_df[column].isnull().sum() > df[column].isnull().sum():
                    invalid_count = result_df[column].isnull().sum() - df[column].isnull().sum()
                    warnings.append(f"Column '{column}' had {invalid_count} non-integer values that were converted to null")
            except Exception as e:
                errors.append(f"Failed to convert column '{column}' to integer: {str(e)}")
        
        elif expected_type == 'float':
            # Try to convert to float, handling non-numeric values
            try:
                result_df[column] = pd.to_numeric(result_df[column], errors='coerce')
                
                # Check how many values were coerced to NaN
                if result_df[column].isnull().sum() > df[column].isnull().sum():
                    invalid_count = result_df[column].isnull().sum() - df[column].isnull().sum()
                    warnings.append(f"Column '{column}' had {invalid_count} non-numeric values that were converted to null")
            except Exception as e:
                errors.append(f"Failed to convert column '{column}' to float: {str(e)}")
        
        elif expected_type == 'boolean':
            # Convert various boolean representations to 0/1
            try:
                # Handle common boolean representations
                bool_map = {
                    True: 1, 'True': 1, 'true': 1, 't': 1, 'yes': 1, 'y': 1, '1': 1, 1: 1,
                    False: 0, 'False': 0, 'false': 0, 'f': 0, 'no': 0, 'n': 0, '0': 0, 0: 0
                }
                
                # Apply mapping and set anything not in the map to null
                result_df[column] = result_df[column].map(bool_map)
                
                # Check how many values were converted to null
                if result_df[column].isnull().sum() > df[column].isnull().sum():
                    invalid_count = result_df[column].isnull().sum() - df[column].isnull().sum()
                    warnings.append(f"Column '{column}' had {invalid_count} values that couldn't be interpreted as boolean")
            except Exception as e:
                errors.append(f"Failed to convert column '{column}' to boolean: {str(e)}")
        
        elif expected_type == 'date':
            # Try to convert to datetime, handling various date formats
            try:
                result_df[column] = pd.to_datetime(result_df[column], errors='coerce')
                
                # Convert datetime to string in YYYY-MM-DD format
                mask = result_df[column].notna()
                result_df.loc[mask, column] = result_df.loc[mask, column].dt.strftime('%Y-%m-%d')
                
                # Check how many values were coerced to NaN
                if result_df[column].isnull().sum() > df[column].isnull().sum():
                    invalid_count = result_df[column].isnull().sum() - df[column].isnull().sum()
                    warnings.append(f"Column '{column}' had {invalid_count} values that couldn't be interpreted as dates")
            except Exception as e:
                errors.append(f"Failed to convert column '{column}' to date: {str(e)}")
        
        elif expected_type == 'email':
            # Validate email format
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            
            # Check each non-null value against the pattern
            invalid_emails = []
            for idx, value in result_df[column].dropna().items():
                if not re.match(email_pattern, str(value)):
                    invalid_emails.append(value)
                    
            if invalid_emails:
                warnings.append(f"Column '{column}' contains {len(invalid_emails)} invalid email addresses")
        
        elif expected_type == 'phone':
            # Basic phone number validation and formatting
            # This is a simplified version - real phone validation is complex
            phone_pattern = r'^\+?[0-9\-\(\)\s\.]{7,20}$'
            
            # Check each non-null value against the pattern
            invalid_phones = []
            for idx, value in result_df[column].dropna().items():
                if not re.match(phone_pattern, str(value)):
                    invalid_phones.append(value)
                    
            if invalid_phones:
                warnings.append(f"Column '{column}' contains {len(invalid_phones)} potentially invalid phone numbers")
        
        return result_df
    
    def process_and_clean_data(self, df, table_name):
        """
        Process and clean data for a specific table
        
        Args:
            df: pandas DataFrame to process
            table_name: name of the table
            
        Returns:
            tuple: (processed_df, errors, warnings)
        """
        # Validate the data first
        is_valid, errors, warnings, processed_df = self.validate_dataframe(df, table_name)
        
        if not is_valid:
            return processed_df, errors, warnings
        
        # Additional processing based on table type
        if table_name == 'clients':
            # Calculate total_spend if not provided
            if 'total_spend' not in processed_df.columns:
                warnings.append("'total_spend' column not provided, will be calculated from enrollments if available")
                processed_df['total_spend'] = 0.0
                
                # Try to calculate from enrollments if client_id exists
                if 'client_id' in processed_df.columns:
                    for client_id in processed_df['client_id'].dropna().unique():
                        try:
                            self.cursor.execute(
                                "SELECT SUM(revenue) FROM enrollments WHERE client_id = ?", 
                                (client_id,)
                            )
                            total_spend = self.cursor.fetchone()[0]
                            if total_spend is not None:
                                processed_df.loc[processed_df['client_id'] == client_id, 'total_spend'] = total_spend
                        except Exception as e:
                            warnings.append(f"Failed to calculate total_spend for client_id {client_id}: {str(e)}")
        
        elif table_name == 'enrollments':
            # Calculate profit fields if not provided
            if 'revenue' in processed_df.columns:
                # Ensure cost columns exist
                for cost_column in ['trainer_cost', 'logistics_cost', 'venue_cost', 'utilities_cost', 'materials_cost']:
                    if cost_column not in processed_df.columns:
                        processed_df[cost_column] = 0.0
                        warnings.append(f"'{cost_column}' column not provided, using 0.0 as default")
                
                # Calculate profit
                processed_df['profit'] = (
                    processed_df['revenue'] - 
                    processed_df['trainer_cost'] - 
                    processed_df['logistics_cost'] - 
                    processed_df['venue_cost'] - 
                    processed_df['utilities_cost'] - 
                    processed_df['materials_cost']
                )
                
                # Calculate profit margin
                processed_df['profit_margin'] = 0.0
                mask = processed_df['revenue'] > 0
                processed_df.loc[mask, 'profit_margin'] = (processed_df.loc[mask, 'profit'] / processed_df.loc[mask, 'revenue']) * 100
        
        elif table_name == 'opportunities':
            # Set default probability based on stage if not provided
            if 'stage' in processed_df.columns and ('probability' not in processed_df.columns or processed_df['probability'].isnull().any()):
                stage_probabilities = {
                    'Lead': 10,
                    'Prospect': 25,
                    'Proposal': 50,
                    'Negotiation': 75,
                    'Closed Won': 100,
                    'Closed Lost': 0
                }
                
                if 'probability' not in processed_df.columns:
                    processed_df['probability'] = 0.0
                    warnings.append("'probability' column not provided, using stage-based defaults")
                
                for stage, prob in stage_probabilities.items():
                    mask = (processed_df['stage'] == stage) & (processed_df['probability'].isnull())
                    if mask.any():
                        processed_df.loc[mask, 'probability'] = prob
                        warnings.append(f"Set default probability of {prob}% for {mask.sum()} opportunities in '{stage}' stage")
        
        return processed_df, errors, warnings
    
    def get_table_summary(self, table_name):
        """
        Get a summary of a table's data
        
        Args:
            table_name: name of the table
            
        Returns:
            dict: summary statistics
        """
        try:
            # Get row count
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = self.cursor.fetchone()[0]
            
            # Get column names
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in self.cursor.fetchall()]
            
            # Get basic stats for each column
            column_stats = {}
            for column in columns:
                self.cursor.execute(f"SELECT COUNT(DISTINCT {column}) FROM {table_name}")
                distinct_count = self.cursor.fetchone()[0]
                
                self.cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column} IS NULL")
                null_count = self.cursor.fetchone()[0]
                
                column_stats[column] = {
                    'distinct_values': distinct_count,
                    'null_count': null_count,
                    'null_percentage': (null_count / row_count * 100) if row_count > 0 else 0
                }
            
            return {
                'row_count': row_count,
                'column_count': len(columns),
                'columns': columns,
                'column_stats': column_stats
            }
        except Exception as e:
            return {'error': str(e)}
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()


# Example usage:
if __name__ == "__main__":
    validator = DataValidator()
    
    # Example: Get table summary
    for table in ['clients', 'programs', 'enrollments', 'opportunities']:
        summary = validator.get_table_summary(table)
        print(f"\n{table.upper()} TABLE SUMMARY:")
        print(f"Row count: {summary.get('row_count', 0)}")
        print(f"Column count: {summary.get('column_count', 0)}")
        
        if 'column_stats' in summary:
            print("\nColumn statistics:")
            for col, stats in summary['column_stats'].items():
                print(f"  {col}: {stats['distinct_values']} distinct values, {stats['null_count']} nulls ({stats['null_percentage']:.1f}%)")
    
    validator.close()
