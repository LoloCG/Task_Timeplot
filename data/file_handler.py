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
    def __init__(self, path_str: str):
        sp_path = Path(path_str)
        if not sp_path.exists():
            log.error(f'Error, sync path does not exist ({path_str})')
        self.sp_path     = sp_path

        self.project_prefixes = ["state", "project"]
    
    def get_last_update_nums(self) -> dict:
        lastModified = None 
        archiveYoung = None
        archiveOld = None
        e_types = ["number", "string", "null"]

        for prefix, event, value in stream_json_file(file_path=self.sp_path):
            parts = prefix.split(".")

            if (parts[0] == "lastModified" and event in e_types):
                lastModified = value
            elif (parts[:2] == ["revMap","archiveYoung"] and event in e_types):
                archiveYoung = value
            elif (parts[:2] == ["revMap","archiveOld"] and event in e_types):
                archiveOld = value

            if (lastModified is not None and
                archiveYoung is not None and
                archiveOld is not None):
                log.info("No sync update numbers found.")
                break

        data = {
            "lastUpdate":   lastModified,
            "archiveYoung": archiveYoung if archiveYoung is not isinstance(archiveYoung, str) else 0,
            "archiveOld":   archiveOld if archiveOld is not isinstance(archiveOld, str) else 0,
        }

        return data

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
            log.debug(f"Cutoff date={cutoff}") 
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
                and parts[:3] == ["state", "project", "entities"]
                and parts[3] != "INBOX_PROJECT"
            ):
                current_proj = parts[lparts-1]
                proj_builder = ObjectBuilder()
                proj_builder.event(event, value)
                continue

            # If we’re inside a project, feed events to the builder
            if proj_builder is not None:
                proj_builder.event(event, value)

                # On the matching end_map, finalize
                if (
                    event == "end_map"
                    and len(parts) == 4
                    and parts[:3] == ["state", "project", "entities"]
                ):
                    proj = proj_builder.value
                    # prune the unwanted nested keys:
                    proj.pop("advancedCfg", None)
                    proj.pop("theme", None)
                    proj.pop("icon", None)
                    proj.pop("isHiddenFromMenu", None)
                    proj.pop("isEnableBacklog", None)

                    projects[current_proj] = proj

                    # Reset
                    proj_builder = None
                    current_proj = None

                    # Don’t fall through into task logic
                    continue

            # --------------- Current Tasks --------------- #
            # Detect start of a task object
            # state.task.task.entities.id
            # archiveYoung.archiveOld.task.entities.id
            # archiveOld.task.entities
            TASK_BASES = [
                ["state", "task", "entities"],
                ["archiveYoung", "task", "entities"],
                ["archiveOld", "task", "entities"],
            ]

            if (
                event == "start_map"
                and lparts == 4 
                and any(parts[:3] == b for b in TASK_BASES)
                # and parts[lparts-3:lparts-1] == ["task", "entities"]
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
            # ignore_subtask_id = []
            ignore_subtask_id = set()
            parent_tasks = {}

            for task_id, task_dict in tasks.items():

                if task_id in ignore_subtask_id: continue
                if len(task_dict["subTaskIds"]) > 0:
                    for subtask_id in task_dict["subTaskIds"]:
                        # ignore_subtask_id.append(subtask_id)
                        ignore_subtask_id.add(subtask_id)
                
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

            # for time_day, time_spent in task_dict['timeSpentOnDay'].items():
            tsod = task_dict.get("timeSpentOnDay") or {}
            for time_day, time_spent in tsod.items():
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
        if len(df) == 0:
            log.warning(f"task list empty")
            return None

        df["start_time"] = pd.to_datetime(df["start_time"])
        df["end_time"]   = pd.to_datetime(df["end_time"],
                                format="ISO8601",
                                errors="raise")
        return df

