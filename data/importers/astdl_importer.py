from utils.logger import LoggerSingleton
log = LoggerSingleton().get_logger()

from utils.excel_importer import ExcelImporter
from pathlib import Path
from data.sqlalchemy import DBManager
import pandas as pd


class AbstractSpoonTDLImporter:
    @classmethod
    def csv_to_df(cls, csv_path:Path|str, raw:bool=True,
                  csv_config_dict:dict=None,
        ):
        if type(csv_path) == str:
            csv_path = Path(csv_path)
        if not csv_path.exists():
            raise(f"Import file path does not exist: {csv_path}")

        csv_kwargs={
            "skiprows": 1,
            "decimal": ",",
            "sep": "\t",
            "encoding": "utf-16",
        }
        raw_df = ExcelImporter(csv_path).get_df_from_file(csv_kwargs=csv_kwargs)
        if raw == True:
            return raw_df
        
        if csv_config_dict == None:
            raise(f"CSV config not given during import")
            return None
        
        # log.debug(f"raw df before cleaning:\n{raw_df}")
        # log.debug(f"periods={csv_config_dict["periods"]}")

        df = cls.perform_basic_cleaning(
            df_raw=raw_df,
            new_course_name=csv_config_dict["course_name"],
            period_mappings=csv_config_dict["periods"]
        )

        # log.debug(f"cleaned df={df}")
        return df

    @classmethod
    def perform_basic_cleaning(cls,
            df_raw:pd.DataFrame, 
            new_course_name: str,
            period_mappings
        ):
        from utils.df_cleaner import DFCleaner
        '''
        - Removes irrelevant columns
        - Splits "Path" column into Period, subject and task
        - consolidates date and time into a single DateTime for start and end times
        - parses Time Spent to 
        - Uses period mapping config to apply certain changes:
            {
                "csv_filename": "1ero Farmacia Tasklist_Log",
                "course_name": "21-22 PharmaNutri",
                "periods": [
                    {
                        "csv_period_name": "1st semester",
                        "start_date": "13-9-2021",
                        "edited_period_name": "1st Semester"
                    },
                    {
                        "csv_period_name": "2nd semester",
                        "start_date": "31-01-2022",
                        "edited_period_name": "2nd Semester"
                    }
                ]
            },
        '''
        def delete_negative_times(df, margin = 0.008, time_threshold = 0.5, date_threshold = pd.Timedelta(days=2)):

            negval_condition = (df['Time Spent (Hrs)'] < 0) & (df['Type'] == 'Adjusted')
            pos_rows = df[~negval_condition]
            neg_rows = df[negval_condition]

            for _, neg_row in neg_rows.iterrows():
                close_condition = (
                    (abs(pos_rows['Time Spent (Hrs)'] + neg_row['Time Spent (Hrs)']) < time_threshold) & 
                    (abs(pos_rows['End Date'] - neg_row['End Date']) < date_threshold)
                )
                pos_rows = pos_rows[~close_condition]

            pos_rows = pos_rows[~(pos_rows['Time Spent (Hrs)'] <= margin)]

            ini_neg = len(df[df['Time Spent (Hrs)'] < 0])
            post_neg = len(pos_rows[pos_rows['Time Spent (Hrs)'] < 0])
            log.debug(f"Deleted {post_neg-ini_neg} negative values and removed {len(pos_rows)-len(df)} total rows")

            return pos_rows
        
        def join_dates_times(df):
            # combine date + time into one Timestamp column
            df['Start Date'] = pd.to_datetime(df['Start Date'], errors='coerce')
            df['End Date']   = pd.to_datetime(df['End Date'],   errors='coerce')

            df['Start Time'] = df['Start Time'].astype(str).str.strip()
            df['End Time']   = df['End Time'].astype(str).str.strip()

            df['Start DateTime'] = pd.to_datetime(
                df['Start Date'].dt.strftime('%Y-%m-%d') + ' ' + df['Start Time'],
                format='%Y-%m-%d %H:%M',
                errors='coerce'
            )
            df['End DateTime'] = pd.to_datetime(
                df['End Date'].dt.strftime('%Y-%m-%d')   + ' ' + df['End Time'],
                format='%Y-%m-%d %H:%M',
                errors='coerce'
            )
            return df
        
        def rename_course_and_periods(df):
            '''
            Applies changes to course name and period names based on mapping config 
            '''
            df['Course'] = new_course_name
            
            mapping = {
                m["csv_period_name"]: m["edited_period_name"]
                for m in period_mappings
            }
            df = df[df["Period"].isin(mapping)]
            df.loc[:, "Period"] = df["Period"].map(mapping)

            return df

        df_raw = df_raw.reindex(columns=['Start Date', 'Start Time', 'End Date', 'End Time', 'Time Spent (Hrs)', 'Path', 'Title', 'Type'])
        cleaner = DFCleaner(df_raw)

        new_columns = ['Period', 'Subject', 'pathinfo']
        cleaner.split_column(column='Path', separator='\\', new_columns=new_columns, expand=True, drop_old=True)

        cleaner.normalize_column_strings(column='Subject')

        cleaner.convert_df_dates(date_column='Start Date', single_col=True)
        cleaner.convert_df_dates(date_column='End Date', single_col=True)
        cleaner.convert_df_times(time_column='Start Time', single_col=True)
        cleaner.convert_df_times(time_column='End Time', single_col=True)

        cleaner.dataframe = join_dates_times(cleaner.dataframe)  
        
        cleaner.replace_comma_to_dot(column='Time Spent (Hrs)')

        cleaner.dataframe = delete_negative_times(cleaner.dataframe)
        
        df_raw = cleaner.dataframe
        
        df_raw = rename_course_and_periods(df_raw)

        df_raw = df_raw.drop(columns=[
            'Start Date','Start Time',
            'End Date','End Time',
            'Type', 'Pathinfo'
        ])
        
        df_clean = df_raw.rename(columns={
            'Course':           'course',
            'Title':            'task_name',
            'Period':           'period',
            'Subject':          'subject',
            'Start DateTime':   'start_time',
            'End DateTime':     'end_time',
            'Time Spent (Hrs)': 'time_spent_hrs'
        })

        return df_clean

