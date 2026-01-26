import pandas as pd
from core.data_transformers import DFTransformers

from utils.logger import LoggerSingleton
log = LoggerSingleton().get_logger()


class AbstractSpoonTDLImporter:
    '''
    Legacy code for importing Abstractspoon's todolist logs from courses of 2021 to 2024.
    Used to import past courses from the CSV files.

    Acts as orchestrator with .import_pastcourses(), with its own helper .perform_basic_cleaning(),
    later using the DFCleaner class to convert the basic data to daily and weekly data.
    '''
    @classmethod
    def import_pastcourses(cls):
        from utils.excel_importer import ExcelImporter
        from pathlib import Path
        import json
        from data.sqlalchemy import DBManager

        example_input_folder_path = Path(r'.\data_example')
        
        eimp = ExcelImporter().select_folder(example_input_folder_path)
        files = eimp.list_folder_excel_files()

        with open(r'.\data_example\past_courses_config.json') as json_file: 
            config = json.load(json_file) 
        
        for file in files:
            log.info(f'Importing CSV file "{file}"')

            entry = next((item for item in config 
                        if (
                            item["csv_filename"] == Path(file).stem or 
                            item["csv_filename"] == Path(file))),
                        None)        
            if entry is None:
                log.warning(f"No config for '{file}', skipping.")
                continue
                
            course_name         = entry["course_name"]
            period_mappings     = entry["periods"]

            file_df = eimp.get_df_from_file(file)

            df_clean = cls.perform_basic_cleaning(file_df, new_course_name=course_name, period_mappings=period_mappings)
            DBManager.insert_to_main_data(df_clean)

            periods_start = {}
            for period in period_mappings:
                start_date = pd.to_datetime(period['start_date'], format="%d-%m-%Y")
       
                DBManager.insert_period_data(
                    course=course_name,
                    period=period['edited_period_name'],
                    start_date=start_date,
                    finished=True
                )
                periods_start[period["edited_period_name"]] = start_date

            df_daily = DFTransformers.basic_to_daily_clean(df_basic=df_clean, periods_start=periods_start)
            DBManager.insert_daily_data(df_daily)

            # df_weekly = DFTransformers.basic_to_weekly_clean(df_daily=df_daily)
            # DBManager.insert_weekly_data(df_weekly)
            
            log.info(f"Imported file {files.index(file)+1}/{len(files)}: {file} ")
        
        return

    @classmethod
    def perform_basic_cleaning(cls,
            df_raw, 
            new_course_name: str,
            period_mappings
        ):
        from utils.df_cleaner import DFCleaner
        '''
        - Removes irrelevant columns
        - Splits "Path" column into Period, subject and task
        - consolidates date and time into a single DateTime for start and end times
        - parses Time Spent to 
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
            df['Course'] = new_course_name
            
            mapping = {
                m["csv_period_name"]: m["edited_period_name"]
                for m in period_mappings
            }
            df = df[df["Period"].isin(mapping)]             # keep only the allowed Period
            df.loc[:, "Period"] = df["Period"].map(mapping) # rename them to the edited names

            return df

        df_raw = df_raw.reindex(columns=['Start Date', 'Start Time', 'End Date', 'End Time', 'Time Spent (Hrs)', 'Path', 'Type'])

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
            'Period':           'period',
            'Subject':          'subject',
            'Start DateTime':   'start_time',
            'End DateTime':     'end_time',
            'Time Spent (Hrs)': 'time_spent_hrs'
        })

        return df_clean


CSV_IMPORT_NUM = 1
csv_paths_folder ="past_data"
json_config_path=r"past_data\past_courses_csv_data.json"

def main():
    from data.file_handler import convert_csv_to_df
    from utils.excel_importer import ExcelImporter
    import json, os #, re


    
    eimp = ExcelImporter().select_folder(csv_paths_folder)
    files = eimp.list_folder_excel_files()
    log.info(f"Past courses .csv files: {files}")

    file = files[CSV_IMPORT_NUM]
    log.info(f"Selected to import {CSV_IMPORT_NUM}")
    raw_df = ExcelImporter(csv_paths_folder).get_df_from_file(filename=file)
    log.debug(f"Raw df:\n{raw_df}")

    config = None
    with open(json_config_path) as json_file: 
        config = json.load(json_file)
        log.info(f"Imported json config in {json_config_path}")

    base_filename = os.path.splitext(file)[0].strip()
    entry = next(
        cfg for cfg in config
        if cfg["csv_filename"].strip() == base_filename
    )
        
    course_name         = entry["course_name"]
    period_mappings     = entry["periods"]


    df_clean = convert_csv_to_df(n=1)

    print(df_clean)

if __name__ == "__past_importer__":
    main()

