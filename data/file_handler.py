import json, re, os
import ijson
import warnings
from ijson.common import ObjectBuilder
from pathlib import Path
import pandas as pd
from core.data_transformers import DFTransformers
from enum import Enum
from datetime import datetime, timezone,  timedelta, date

from utils.logger import LoggerSingleton
log = LoggerSingleton().get_logger()

def stream_json_file(file_path: Path, chunk_size:int=64, limit=None):
    """
    A generator over (prefix, event, value) for every JSON token
    in the SuperProductivity json dump.
    Reads in small chunks of 64 bits until "{" character is seen,
    indicating json start(?).

    """
    with open(file_path, "rb") as f:
        header_buf = b""
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                raise ValueError("No JSON start found!")
            
            header_buf += chunk     # append these bytes to our buffer
            
            idx = header_buf.find(b"{")
            # If it returns -1, there’s no { yet, so we loop again and read another 64 bytes.
            if idx != -1: 
                # Indicates the first '{' at position idx within header_buf
                f.seek(- (len(header_buf) - idx), 1)
                break

        count = 0
        for prefix, event, value in ijson.parse(f):
            yield prefix, event, value
            
            count += 1
            if limit and count >= limit:
                break                 

class SPImportManager:
    def __init__(self, sp_path: Path):
        if not sp_path.exists():
            log.error(f'Error, sync path does not exist ({str(sp_path)})')
        self.sp_path     = sp_path
    
    def get_last_update_nums(self) -> dict:
        lastUpdate = None 
        archiveYoung = None
        archiveOld = None
        e_types = ["number", "string", "null"]

        for prefix, event, value in stream_json_file(file_path=self.sp_path):
            parts = prefix.split(".")

            if (parts[0] == "lastUpdate" and event in e_types):
                lastUpdate = value
            elif (parts[:2] == ["revMap","archiveYoung"] and event in e_types):
                archiveYoung = value
            elif (parts[:2] == ["revMap","archiveOld"] and event in e_types):
                archiveOld = value

            if (lastUpdate is not None and
                archiveYoung is not None and
                archiveOld is not None):
                break
        
        return {
            "lastUpdate":   lastUpdate,
            "archiveYoung": int(archiveYoung),
            "archiveOld":   int(archiveOld)
        }

    def get_sp_data(self, filter_date: date = None):
        '''
        Retrieve and parse SuperProductivity JSON data, optionally filtering tasks by date of time entries.

        Parameters:
            filter_date (datetime.date, optional): If provided, only tasks whose latest
                "timeSpentOnDay" key is on or after this date are included. Tasks with all
                entries before `filter_date` are dropped.

        Returns:
            tuple[dict, dict]:
                - tasks: A mapping from task IDs to task objects that passed the date filter.
                - projects: A mapping from project IDs to project objects (unfiltered).

        Behavior:
            - Streams the JSON blob with ijson, building one task/project at a time.
            - Tracks the maximum day seen in each task's "timeSpentOnDay" map.
            - Filters out tasks whose max day < `filter_date` (if filtering is enabled).
            - Optionally updates the configuration's "last_update" to the highest date seen
              across all processed tasks, for incremental sync.

        Generated with help of o4-mini

        Example map generated:
            [{
                "1XHjOj3cxM7WTJXXclGbi": {
                    "id": "1XHjOj3cxM7WTJXXclGbi",
                    "subTaskIds": [],
                    "timeSpentOnDay": {
                        "2025-06-22": 15819999,
                        "2025-06-23": 16080158,
                        "2025-06-24": 336000
                    },
                    "timeSpent": 32236157,
                    "timeEstimate": 0,
                    "isDone": false,
                    "title": "Connection to Superproductivity data",
                    "tagIds": [],
                    "created": 1750581369404,
                    "attachments": [],
                    "projectId": "rjbQzJIKXGrITOQ0ssVf-",
                    "dueDay": "2025-06-24"
                },
            }]
        '''     

        if filter_date is not None:
            cutoff: date = filter_date 
        else:
            cutoff = None

        max_day_seen = date.min

        tasks = {}
        projects = {}
        
        task_builder = None
        proj_builder = None
        current_task = None
        current_proj = None        

        for prefix, event, value in stream_json_file(file_path=self.sp_path):
            parts = prefix.split(".")
            lparts = len(parts)

            # --------------- PROJECTS --------------- #
            if (
                event == "start_map"
                and lparts == 4
                and parts[:3] == ["mainModelData", "project", "entities"]
                and parts[3] != "INBOX_PROJECT"
            ):
                current_proj = parts[lparts-1]
                proj_builder = ObjectBuilder()
                proj_builder.event(event, value)
                continue

            # B) If we’re inside a project, feed every event
            if proj_builder is not None:
                proj_builder.event(event, value)

                # C) On the matching end_map, finalize
                if (
                    event == "end_map"
                    and len(parts) == 4
                    and parts[:3] == ["mainModelData", "project", "entities"]
                ):
                    proj = proj_builder.value
                    # prune the unwanted nested keys:
                    proj.pop("advancedCfg", None)
                    proj.pop("theme",       None)
                    proj.pop("icon", None)

                    projects[current_proj] = proj

                    # Reset
                    proj_builder = None
                    current_proj = None

                    # Don’t fall through into task logic
                    continue


            # --------------- Current Tasks --------------- #
            # Detect start of a task object
            # mainModelData.archiveYoung.task.entities.id
            # mainModelData.archiveOld.task.entities.id
            # mainModelData.task.entities.NCEaP5ZYh4lVVPUsy1BLG
            if (
                event == "start_map"
                and lparts >= 4
                and parts[lparts-3:lparts-1] == ["task", "entities"]
            ):
                
                current_task = parts[lparts-1]
                
                task_builder = ObjectBuilder()
                task_builder.event(event, value)

                if filter_date is not None:
                    # we're filtering: start each task with the oldest possible day
                    max_day_seen = date.min
                else:
                    # not filtering: it doesn’t matter, but set it so comparisons never fail
                    max_day_seen = date.min
                    
                continue

            if task_builder is not None:
                
                if (
                    cutoff is not None 
                    and prefix.endswith(".timeSpentOnDay") 
                    and event == "map_key"
                ):
                    day = datetime.fromisoformat(value).date()

                    if max_day_seen == None or day > max_day_seen:
                        max_day_seen = day
                                    
                task_builder.event(event, value)

                if (
                    event == "end_map"
                    and lparts >= 4 
                    and parts[lparts-3:lparts-1] == ["task", "entities"]
                ):

                    if cutoff is None or max_day_seen >= cutoff:
                        tasks[current_task] = task_builder.value
                    
                    # reset for next task
                    task_builder = None
                    current_task = None
                    max_day_seen = None

        # DEBUG PURPOSES
        # JsonConfigManager(Path('raw_tasks.json')).save_dict_to_config(data=tasks)

        return tasks, projects

    @staticmethod
    def clean_sp_tasks(tasks:dict, projects:dict, ccourse:str, cperiod:str, filter_date: date = None, cstart=None):
        def remove_child_tasks(tasks: dict[str, dict]) -> dict[str, dict]:
            ignore_subtask_id = []
            parent_tasks = {}

            for task_id, task_dict in tasks.items():

                if task_id in ignore_subtask_id: continue
                if len(task_dict["subTaskIds"]) > 0:
                    for subtask_id in task_dict["subTaskIds"]:
                        ignore_subtask_id.append(subtask_id)
                
                parent_tasks[task_id] = task_dict

            return parent_tasks

        tasks = remove_child_tasks(tasks)

        # map of project_id:project_title
        proj_titles = {
            pid: proj["title"].strip()
            for pid, proj in projects.items()
        }
        
        flat_tasks = []
    
        for task_id, task_dict in tasks.items():
            
            proj_id = task_dict["projectId"] # fall back to pid if we don't know this project
            subject_title = proj_titles.get(proj_id, proj_id)

            for time_day, time_spent in task_dict['timeSpentOnDay'].items():
                day = datetime.fromisoformat(time_day).date()

                if filter_date is not None and day < filter_date: 
                    continue

                hours = time_spent / 3_600_000

                day_start = day # Due to SP data structure, tasks start 00:00
                end_start   = day_start + timedelta(hours=hours)
                
                flat_tasks.append({
                    'course': ccourse,
                    'period': cperiod,
                    'subject': subject_title,
                    'task_name': task_dict['title'].strip(),
                    'start_time': day_start.isoformat(),
                    'end_time': end_start.isoformat(),
                    'time_spent_hrs': hours,
                    'finished': task_dict.get("isDone", False),
                })

        log.debug(f"Generated a total of {len(flat_tasks)} tasks.")

        return flat_tasks
    
    @staticmethod
    def convert_tasks_to_df(tasks_list: list[dict], cstart=None) -> pd.DataFrame:
        if cstart != None:
            log.warning(f"Current start feature not yet added.")

        df = pd.DataFrame(tasks_list)
        df["start_time"] = pd.to_datetime(df["start_time"])
        df["end_time"]   = pd.to_datetime(df["end_time"],
                                format="ISO8601",
                                errors="raise")
        return df

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

            df_weekly = DFTransformers.basic_to_weekly_clean(df_daily=df_daily)
            DBManager.insert_weekly_data(df_weekly)
            
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

class JsonConfigManager:
    def __init__(self, path: Path = Path("config.json")):
        self.path = path

    def save_dict_to_config(self, data, path=None):
        if not path: path = self.path
        log.info(f"Saving data to {path}")

        with open(path, 'w') as json_file:
            json.dump(data, json_file, indent=2)

    def load_json_config(self) -> dict:
        """
        Read and return the JSON config, or {} if the file doesn't exist.
        """
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def json_upsert(self, new_data, ):
        """update or insert, and save the config data."""
        cfg_file = Path(self.path)
        if cfg_file.exists():
            with open(cfg_file, 'r') as file:
                try:
                    config = json.load(file)
                except json.JSONDecodeError:
                    config = {}
        else:
            config = {}

        config.update(new_data)

        with open(cfg_file, 'w') as file:
            json.dump(config, file, indent=4)

        return config

class SyncStatus(Enum):
    FIRST_RUN           = 0
    UP_TO_DATE          = 1
    UPDATE_AVAILABLE    = 2

def check_local_archives_ver(headers:dict) -> SyncStatus:
    '''
    Return int value indicating:
    '''
    last_young  = headers["archiveYoung"]
    last_old    = headers["archiveOld"]

    config      = JsonConfigManager().load_json_config()
    local_young = int(config.get("local_archive_young", 0))
    local_old   = int(config.get("local_archive_old", 0))

    if local_young == last_young and local_old == last_old:
        log.debug(f"Archived tasks seem in sync.\nyoung={local_young}={last_young} {local_young==last_young}, old={local_old}{last_old}{local_old==last_old}")
        return SyncStatus.UP_TO_DATE
    elif local_young < local_young:
        return SyncStatus.ARCHIVE_YOUNG
    else:
        return SyncStatus.ARCHIVE_OLD