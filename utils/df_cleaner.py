'''
https://github.com/LoloCG/Lolos_Packages/blob/main/Data_Analysis/Data_Cleaning/data_cleaning_utils.py
'''
import pandas as pd

class DFCleaner:
    def __init__(self, dataframe: pd.DataFrame):
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError("Expected input to be a pandas DataFrame.")
        else: self.dataframe = dataframe

    def column_exists(self, column):
        df = self.dataframe
        if column not in df.columns:
            raise KeyError(f"Column '{column}' not found in the DataFrame.")
        
        return

    def convert_df_dates(self, date_column, single_col=True, keep_original=True):

        df = self.dataframe
        
        self.column_exists(date_column)

        if pd.api.types.is_datetime64_any_dtype(df[date_column]):
            # raise Exception(f"Column {date_column} is already datetime type...")
            print(f"Warning: Column {date_column} is already datetime type.")
            return self

        # TODO add errorhandling for invalid date types

        new_datecol = date_column if keep_original == True else 'date'
        df[new_datecol] = pd.to_datetime(df[date_column], errors='coerce').dt.date # , , format="%d/%m/%Y"
        if not keep_original: df.drop(date_column, axis=1, inplace=True)

        if single_col == False:
            # TODO: add error handling to fill NaN rows
            df['year'] = df[new_datecol].dt.year 
            df['month'] = df[new_datecol].dt.month 
            df['day'] = df[new_datecol].dt.day
        
        if keep_original == False: df.drop(date_column, axis=1, inplace=True)

        self.dataframe = df

        return self

    def convert_df_times(self, time_column, single_col=True, keep_original=True, time_format='%H:%M'):
        '''
        Converts a column with time in HH:MM format into datetime or separate columns for hours and minutes.
        '''
        df = self.dataframe

        self.column_exists(time_column)
        
        if pd.api.types.is_datetime64_any_dtype(df[time_column]):
            print(f"Warning: Column {time_column} is already in datetime type.")
            return self

        if not pd.api.types.is_string_dtype(df[time_column]):
            df[time_column] = df[time_column].astype(str)

        new_timecol = str(time_column) if keep_original == True else 'time'

        df[new_timecol] = pd.to_datetime(df[time_column], errors='coerce', format=time_format)
         
        if df[new_timecol].isnull().any():
            print(f"Warning: Some values in column '{time_column}' could not be converted and are NaT.")

        df[new_timecol] = df[new_timecol].dt.strftime(time_format)

        # If single_col is False, split into hour, minute, and second (if applicable)
        if single_col == False:
            df['hour'] = pd.to_datetime(df[new_timecol], format=time_format).dt.hour
            df['minute'] = pd.to_datetime(df[new_timecol], format=time_format).dt.minute
            if '%S' in time_format:
                df['second'] = pd.to_datetime(df[new_timecol], format=time_format).dt.second


        if keep_original == False: df.drop(time_column, axis=1, inplace=True)

        self.dataframe = df
        return self

    def replace_comma_to_dot(self, column):
        df = self.dataframe
        
        self.column_exists(column)

        if not pd.api.types.is_string_dtype(df[column]):
            raise TypeError(f"Expected a string column for {column}.")

        df[column] = df[column].astype(str).str.replace(',', '.').astype(float)

        self.dataframe = df

        return self

    def normalize_column_strings(self, column, headers=True, items=True):
        df = self.dataframe
        self.column_exists(column)

        if items: df[column] = df[column].str.strip().str.lower().str.title()
        if headers: df.columns = df.columns.str.strip().str.lower().str.title()

        self.dataframe = df

        return self

    def split_column(self, column, separator, new_columns, expand=True, drop_old=True):
        '''
            Splits a column into multiple based on a separator.
        '''
        df = self.dataframe
        self.column_exists(column)
        
        if not pd.api.types.is_string_dtype(df[column]): # Validate if column is not str...
            df[column] = df[column].astype(str)

        df[column] = df[column].fillna('')  # Fill NaNs with empty strings before splitting

        n_separation = len(new_columns)-1
        df[new_columns] = df[column].str.split(separator, n=n_separation, expand=expand)

        if drop_old: df.drop(column, axis=1, inplace=True)
        
        self.dataframe = df
        return self