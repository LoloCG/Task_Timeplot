import pandas as pd

from utils.logger import LoggerSingleton
log = LoggerSingleton().get_logger()


class SPImportTransformer:
    '''
    Manages the transformation of Super Productivity data into the format required.
    '''

class DFTransformers:
    @staticmethod
    def basic_to_daily_clean(df_basic, periods_start: dict[str,str] | None = None):
        '''
        example of periods_start = {
            '1st Semester':'13-9-2021',
            '2nd Semester':'31-01-2022'
        }
        '''
        def fill_missing_days(df, period):
            '''
                Used along the iteration. It selects the period values from the dataframe, 
                    obtains the range of days from start to finish, merges it with the period df, 
                    and fills the missing values (time spent = 0, periods with those that belong to it).
            '''
            df_period = df[df['period'] == period]
            course_name = df_period['course'].iloc[0]
            

            if periods_start is not None:
                period_min = pd.Timestamp(periods_start[period])
            else:
                period_min = pd.Timestamp(df_period['start_time'].min())
            period_max = pd.Timestamp(df_period['start_time'].max())
            
            log.debug(f"Range of period {period}: "
                f"{period_min.strftime('%d-%m-%Y')} to {period_max.strftime('%d-%m-%Y')} "
                f"({((period_max - period_min) + pd.Timedelta(days=1)).days})."
            )

            full_range = pd.DataFrame({'start_time': pd.date_range(start=period_min, end=period_max)})

            df_period = df[df['period'] == period].copy()
            df_period['start_time'] = pd.to_datetime(df_period['start_time'])
            full_range['start_time'] = pd.to_datetime(full_range['start_time'])
            
            df_merged = pd.merge(full_range, df_period, on='start_time', how='left')
            
            df_merged['time_spent_hrs'] = df_merged['time_spent_hrs'].fillna(0)
            df_merged['period'] = df_merged['period'].ffill()

            df_merged['date'] = df_merged['start_time'].apply(lambda x: (pd.Timestamp(x) - period_min).days)
            
            df_merged['period'] = df_merged['period'].ffill().fillna(period)

            df_merged['course'] = course_name
            log.debug(f"Consolidated from {len(df)} to {len(df_merged)} rows.")
    
            return df_merged
        
        log.debug(f"Converting basic data to daily data.")
        
        wanted_cols = ['course', 'period', 'subject', 'time_spent_hrs', 'start_time']
        df = df_basic[wanted_cols]

        df.loc[:, 'start_time'] = pd.to_datetime(df['start_time'], errors='coerce')# .dt.date

        df = df.groupby(['course','period','subject', 'start_time'], as_index=False,dropna=False).sum()

        df_list = []
        for period in df['period'].unique():
            df_filled = fill_missing_days(df=df, period=period)
            df_list.append(df_filled)

        df = pd.concat(df_list, ignore_index=True)
        
        df['date'] = pd.to_datetime(df['start_time']).dt.date
        df = df.drop(columns=['start_time'])

        df['course'] = df['course'].ffill()
        # df['subject'] = df['subject'].fillna('_missing_')

        return df

    @staticmethod
    def daily_to_weekly_clean(df_daily):
        ''' (generated w/ gpt o4-mini)
        Build a Mon-Sun weekly summary (including zero-hour weeks) from a daily hours DataFrame.

        Parameters
        ----------
        df_daily : pd.DataFrame
            A daily-granularity table already zero-filled per period, with columns:
            - course : identifier for the course
            - period : academic period name
            - subject : subject name
            - date : datetime.date for each day
            - time_spent_hrs : hours logged on that day

        Returns
        -------
        pd.DataFrame
            A weekly summary DataFrame containing:
            - course, period, subject
            - week : a pandas Period (W-SUN) labeling each Mon-Sun week
            - time_spent_hrs : total hours per week (zeros where no activity)
            - week_number : sequential week index within each period
        '''
        log.debug(f"Generating weekly hours")

        df = df_daily.copy()
        
        df['date'] = pd.to_datetime(df['date'])
        df['week'] = df['date'].dt.to_period('W-SUN') # .astype(str)
        
        weekly = (df
            .groupby(['course','period','subject','week'], as_index=False)
            ['time_spent_hrs']
            .sum()
        )
        
        # For each period, generate the full list of weeks and left-merge
        out = []
        for period in weekly['period'].unique():
            part = weekly[weekly['period'] == period].copy()
            
            # derive exact calendar bounds from daily data
            days = df[df['period'] == period]
            start, end = days['date'].min(), days['date'].max()
            
            # every Mon–Sun period covering [start, end]
            full_weeks = pd.period_range(
                start=start.to_period('W-SUN'),
                end  =end.to_period('W-SUN'),
                freq ='W-SUN'
            )
            full   = pd.DataFrame({'week': full_weeks})
            
            # left-merge and zero-fill
            merged = (
                full
                .merge(part, on='week', how='left')
                .assign(
                    time_spent_hrs = lambda d: d['time_spent_hrs'].ffill(),
                    course         = part['course'].iloc[0],
                    period         = period,
                    subject        = lambda d: d['subject'].ffill()
                )
            )
            
            # sequential numbering from week 1 → N
            merged['week_number'] = range(1, len(merged) + 1)
            
            out.append(merged)
        
        result = pd.concat(out, ignore_index=True)
        result['week'] = result['week'].astype(str)
        log.debug(f"Produced {len(result)} weekly rows over {len(out)} periods")
        return result




# import Excel_Tools.import_export_utils as imex 
# import Data_Cleaning.data_cleaning_utils as dclean
# import pandas as pd
# from pathlib import Path
# import CLI_native_tools as clin # TODO disconnect CLI tools from data import
# from data.json_handler import json_upsert # absolute import example
# from PyLogger.basic_logger import LoggerSingleton

# log = LoggerSingleton().get_logger()

# input_folder_path = Path(r'C:\Users\Lolo\Desktop\Programming\GITRepo\StudyHoursAnalytics\data_example') 

# def get_files_from_input_path():
#     file_paths = []
#     for item in input_folder_path.iterdir():
#         file_paths.append(item)
#     return file_paths

# def select_current_year_file():
#     def ask_current_year_path():
#         log.info("Current year's .csv file not found.")
        
#         csv_folder_path = ''
#         while True:
#             folder_dir = input("Enter the directory path to the file: ")
#             csv_folder_path = Path(folder_dir)
#             if csv_folder_path.exists() and csv_folder_path.is_dir():
#                 break
#             print("Invalid directory. Please try again.")
        
#         new_data = {}
#         new_data['Current year'] = {}
#         new_data['Current year']['folder path'] = str(csv_folder_path)
        
#         json_upsert(config_file, new_data)

#         return str(csv_folder_path)
        
#     def ask_current_year_csv():
#         config = None
#         with open(config_file, 'r') as file:
#             config = json.load(file)
        
#         current_year_path = Path(config['Current year']['folder path'])
        
#         files = []
#         for file in current_year_path.iterdir():
#             if file.suffix == '.csv':
#                 files.append(file.name)
#             else: continue

#         if not files:
#             log.info("No CSV files found in the directory.")
#             return None

#         choice = clin.show_and_select_options(str_list=files)

#         new_data = {}
#         new_data['Current year'] = {}
#         # this shouldnt be necesary... TODO: refactor upsert function
#         new_data['Current year']['folder path'] = config['Current year']['folder path'] 
#         new_data['Current year']['csv name'] = str(files[choice-1])

#         csv_folder_path = json_upsert(config_file, new_data)

#         return files[choice-1]

#     if not config_file.exists(): 
#         folder_path = ask_current_year_path()
#         file_name = ask_current_year_csv()

#         return folder_path, file_name

#     config = None
#     with open(config_file, 'r') as file:
#         config = json.load(file)

#     folder_path = ask_current_year_path() if 'Current year' not in config else config['Current year']['folder path']
#     file_name = ask_current_year_csv() if 'csv name' not in config['Current year'] else config['Current year']['csv name']
#     # log.debug(f"Selected current year file path:\n{folder_path}/{file_name}")

#     return folder_path, file_name


# def edit_course_params(df, file=None):
#     def update_df_with_json_config(df, config, file):
#         log.debug(f"{file} found in config JSON") 
        
#         df['Course'] = config[file]["Course Name"]
#         df = df[df['Period'].isin(config[file]["Periods maintained"])]

#         for json_period in config[file]["Periods maintained"]:
#             period_name = config[file][json_period]["Period name"]
#             df.loc[df['Period'] == json_period, 'Period'] = period_name

#             new_start_date = pd.to_datetime(config[file][json_period]["Start date"]).date()
#             new_row = {
#                 'Period':           period_name, 
#                 'Start Date':       new_start_date,
#                 'Start Time':       '00:00',
#                 'Time Spent (Hrs)': 0,
#                 'End Date':	        new_start_date,
#                 'End Time':         '00:00',
#             } 

#             df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        
#         df.loc[:, 'Course'] = df['Course'].ffill()

#         log.debug(f"course parameters added from json correctly.")  
#         return df

#     if file is not None and config_file.exists():
#         config = None
#         log.debug(f"Searching for {file} in JSON")
#         with open(config_file, 'r') as j_file:
#             config = json.load(j_file)

#         if file in config:
#             df = update_df_with_json_config(df, config, file)
#             return df

#         else:  
#             log.error(f"{file} not found in JSON")
#     else: 
#         log.error(f"Json file does not exist")


# def basic_to_weekly_df(df_clean):
#     ''' '''
#     log.debug(f"generating weekly hours")
#     wanted_cols = ['Course', 'Period', 'Subject', 'Time Spent (Hrs)', 'Start Date', 'End Date', 'Start Time', 'End Time']
    
#     df = df_clean[wanted_cols].copy()

#     df['Start Date'] = pd.to_datetime(df['Start Date'])
#     df = df.dropna(subset=['Start Date'])

#     df['Week'] = df['Start Date'].dt.to_period('W-SUN').astype(str)

#     df = df.groupby(['Course', 'Period', 'Subject', 'Week'])['Time Spent (Hrs)'].sum().reset_index()
    
#     df = df.sort_values(by='Week').reset_index(drop=True)
    
#     df_list = []
#     for period in df['Period'].unique():
#         period_df = df[df['Period'] == period].copy()  # Explicitly create a copy
#         min_week = pd.Period(period_df['Week'].min(), freq='W-SUN')
#         max_week = pd.Period(period_df['Week'].max(), freq='W-SUN')
#         week_range = pd.period_range(start=min_week, end=max_week, freq='W-SUN')

#         week_enum = {str(week): i+1 for i, week in enumerate(week_range)}
#         period_df['Week Number'] = period_df['Week'].map(week_enum)
#         df_list.append(period_df)
    
#     df = pd.concat(df_list, ignore_index=True)

#     return df

