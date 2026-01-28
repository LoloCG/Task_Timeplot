from datetime import datetime, timezone,  timedelta
# from sqlite3 import Date
# from sys import exception
# from tracemalloc import start

from pandas import DataFrame
from data.sqlalchemy import DBManager
from data.file_handler import *
from core.charts import Charts

from utils.logger import LoggerSingleton
log = LoggerSingleton().get_logger()

SP_FILE = Path(r"C:\Users\Lolo\Nextcloud\Super Productivity\__meta_")

class StartSequence:
    @staticmethod
    def check_local_data_exists() -> bool:
        DBManager().createTables()

        config_mng = JsonConfigManager()
        config = config_mng.load_json_config()
        
        if config == {}:
            log.info("Config file does not exist.")
            return False

        else:
            Orchestrators.check_sp_sync()
        return True

    @staticmethod
    def generate_from_start(ccourse:str, cperiod:str, period_start):
        log.debug(f"Sync file path set in:\n\t{str(SP_FILE)}")

        log.info(f'Importing all data for SP Course '
            f'{ccourse} - {cperiod} '
            f'starting on {period_start}')

        importer = SPImportManager(
            path_str=str(SP_FILE), 
        )
        tasks, projects = importer.get_sp_data()
        flat_tasks = importer.clean_sp_tasks(
            tasks=tasks,
            projects=projects, 
            ccourse=ccourse, 
            cperiod=cperiod
        )
        df = importer.convert_tasks_to_df(flat_tasks, cstart=None)
        Orchestrators.upsert_df_to_db(df)

        sync_headers = importer.get_last_update_nums()

        data={
            "sync_data":{
                "sync_file_path":str(SP_FILE),
                "last_update":sync_headers["lastUpdate"],
                "archive_young":sync_headers["archiveYoung"],
                "archive_old":sync_headers["archiveOld"],
                "update_date":str(datetime.now(timezone.utc)),
            },
            "current_period_data":{
                "current_course":ccourse,
                "current_period":cperiod,
                "period_start_date":period_start
            }
        }
        JsonConfigManager().save_dict_to_config(data)
        log.debug(f"saving config:\n{data}")

        DBManager().insert_period_data(
            course=ccourse,
            period=cperiod,
            start_date=datetime.strptime(period_start, '%d-%m-%Y').date(),
            finished=False
        )

class Orchestrators: 
    @staticmethod
    def plot_weekly_hours_bars(*_, course:str=None, period:str=None):
        # TODO: Will use daily data for the time being until weekly data is added to db.

        if course or period is None: 
            config = get_current_period_config()
            course=config["current_course"]
            period=config["current_period"]

        log.debug(f"Plotting weekly data for {course}, {period}")
        df = DBManager().get_daily_data(course, period)
        df = filter_df_excluded(df)
        df = add_start_date_df(df)

        weekly_df = DFTransformers.temp_daily_to_weekly_clean(df)
        # log.debug(f"Weekly df tail:\n{weekly_df.tail()}")

        Charts.plot_weekly_stack_bar(weekly_df)
        return
    
    @staticmethod
    def plot_daily_hours_bars(*_, course:str=None, period:str=None):
        if course or period is None: 
            config = get_current_period_config()
            course=config["current_course"]
            period=config["current_period"]

        log.debug(f"Plotting daily data for {course}, {period}")
        df = DBManager().get_daily_data(course, period)
        df = filter_df_excluded(df)
        
        full_df = add_start_date_df(df)

        Charts.plot_daily_stack_bar(full_df)
    
    @staticmethod
    def plot_total_horus_bars(*_, course:str=None, period:str=None):

        if course or period is None: 
            config = get_current_period_config()
            course=config["current_course"]
            period=config["current_period"]

        log.debug(f"Plotting total hours data for {course}, {period}")
        df = DBManager().get_daily_data(course, period)
        df = filter_df_excluded(df)

        Charts.plot_total_period_hours_bars(df)

    @staticmethod
    def plot_week_avg_line(*_, course:str=None, period:str=None):
        if course or period is None: 
            config = get_current_period_config()
            course=config["current_course"]
            period=config["current_period"]

        log.debug(f"Plotting 7 day average data for {course}, {period}")
        df = DBManager().get_daily_data(course, period)
        df = filter_df_excluded(df)
        df = add_start_date_df(df)

        Charts.plot_rolling_7d_average(df=df, window=7)
        return None

    @staticmethod
    def insert_df_to_db(df, ccourse, cperiod, cstart):
        db = DBManager()
        db.insert_to_main_data(df=df)

        period_start = {cperiod:cstart}
        daily_df = DFTransformers.basic_to_daily_clean(df, period_start)
        db.insert_daily_data(daily_df)

        # weekly_df = DFTransformers.daily_to_weekly_clean(daily_df)
        # db.insert_weekly_data(weekly_df)

        db.insert_period_data(
            course=ccourse, 
            period=cperiod, 
            start_date= datetime.strptime(cstart, '%d-%m-%Y'), 
            finished = False
        )

    @staticmethod
    def upsert_df_to_db(df):
        db = DBManager()
        db.upsert_to_tables(table='main', df=df)

        # period_start = {CURRENT_PERIOD:CURRENT_PERIOD_START}

        daily_df = DFTransformers.basic_to_daily_clean(df)
        db.upsert_to_tables(table='daily', df=daily_df)

        # weekly_df = DFTransformers.daily_to_weekly_clean(daily_df)
        # db.upsert_to_tables(table='weekly', df=weekly_df)

    @staticmethod
    def check_sp_sync():
        config_mng = JsonConfigManager()
        config = config_mng.load_json_config()

        sync_config = config["sync_data"]
        sync_path = sync_config["sync_file_path"]
        log.debug(f"Checking sync data from: {sync_path}")

        importer = SPImportManager(sync_path)
        sync_headers = importer.get_last_update_nums()
        log.debug(f"sync headers = {sync_headers}")

        update_needed = (sync_headers["lastUpdate"] > sync_config.get("last_update",0))

        if not update_needed:
            log.info(f"No update required.")
            return
        
        log.info(f"Update required. Checking archived tasks.")
        
        # FIXME: seems that if headers dont have data, it shows "UPDATE_ALL_REV" 
        # rather than a single integer.
        # local_young = int(sync_config.get("archive_young", 0))
        # local_old   = int(sync_config.get("archive_old", 0))
        # if local_young < sync_headers["archiveYoung"]:
        #     log.info(f"Update of young archive required ({local_young} vs {sync_headers["archiveYoung"]})")
        # if local_old < sync_headers["archiveOld"]:
        #     log.info(f"Update of old archive required ({local_old} vs {sync_headers["archiveOld"]})")
            
        last_sync_date = datetime.fromtimestamp(sync_config["last_update"]/1000, tz=timezone.utc).date()
        log.info(f"Updating to latest SP data with active tasks after {last_sync_date}.")
        tasks, projects = importer.get_sp_data(filter_date=last_sync_date)
        log.info(f"Found {len(tasks)} tasks to update.")

        ccourse_config=config["current_period_data"]
        flat_tasks = importer.clean_sp_tasks(
            tasks=tasks, projects=projects, 
            ccourse=ccourse_config["current_course"], 
            cperiod=ccourse_config["current_period"], 
            filter_date=last_sync_date
        )
        # FIXME: seems that if it needs to update but does not find any task, it generates empty 
        #   `flat_tasks`, crashing the importer.convert_tasks_to_df
        df = importer.convert_tasks_to_df(flat_tasks, cstart=None)
        Orchestrators.upsert_df_to_db(df)

        # json_upsert does a shallow update of data. So data specific to config is updated here instead.        
        sync_config["last_update"] = sync_headers["lastUpdate"]
        sync_config["update_date"] = str(datetime.now(timezone.utc))
        JsonConfigManager().json_upsert({"sync_data": sync_config})

    @staticmethod
    def get_basic_stats(*_) -> dict:
        log.debug(f"Getting basic stats")
        config = JsonConfigManager().load_json_config()
        config_sync = config["sync_data"]
        last_dt_sync = datetime.fromisoformat(config_sync['update_date'])

        df = DBManager().get_daily_data()
        df = filter_df_excluded(df, config.get("current_period_data", {}).get("default_exclude", None))

        last_db_day = df['date'].max()

        log.debug(f"last db day={last_db_day}")
        total_hours_last_day = df.loc[df['date'] == last_db_day, 'time_spent_hrs'].sum()
        
        week_df = get_week_df(df)
        # log.debug(f"Week data:\n{week_df}")

        total_week_hours = week_df['time_spent_hrs'].sum()
                
        days_elapsed = datetime.now().date().weekday() + 1
        avg_week_daily = total_week_hours / days_elapsed

        return {
            "last_sync":last_dt_sync.date(),
            "last_db_day": last_db_day.date(),
            "last_db_hrs":total_hours_last_day,
            'avg_week_daily':avg_week_daily,
            'total_week_hours':total_week_hours
        }

    @staticmethod
    def import_past_data(*_):
        log.error("Feature not added")
        return

def pivot_time_by_subject_period(
    df: pd.DataFrame,
    time_col: str = "time_spent_hrs",
    subject_col: str = "subject",
    period_col: str = "period",
    course_col: str | None = "course",
    fill_value: float = 0.0,
    add_margins: bool = True,
) -> pd.DataFrame:
    """
    Returns a pivot table with total accumulated hours per subject per period.
    Rows: subject
    Columns: period
    Values: sum(time_spent_hrs)
    Optionally filters by course if df contains multiple courses and course_col is provided.
    """

    if course_col and course_col in df.columns:
        # If multiple courses exist, keep them separated by adding course to the index.
        index = [course_col, subject_col]
    else:
        index = [subject_col]

    piv = pd.pivot_table(
        df,
        index=index,
        columns=period_col,
        values=time_col,
        aggfunc="sum",
        fill_value=fill_value,
        margins=add_margins,
        margins_name="Total",
    )

 
    piv.columns = piv.columns.astype(str)
    return piv.sort_index()

def get_week_df(
    df: pd.DataFrame,
    anchor_date: str | pd.Timestamp | None = None,
    week_start: str = "MON",
    ) -> dict:

    data = df.copy()
    data['date'] = pd.to_datetime(data['date'], errors="coerce")
    if data['date'].isna().any():
        raise ValueError(f"Some values in {'date'} could not be parsed to datetime.")
    
    if anchor_date is None:
        anchor_ts = pd.Timestamp.today()
    else:
        anchor_ts = pd.Timestamp(anchor_date)
    wk_start, wk_end_inclusive = _week_bounds(anchor_ts, week_start=week_start)

    wk_end_exclusive = wk_start + pd.Timedelta(days=7)
    mask = (data['date'] >= wk_start) & (data['date'] < wk_end_exclusive)
    window_desc = f"[{wk_start} … {wk_end_exclusive}) ({week_start}-based)" # unsure why this exists..
    
    log.debug(f"Selecting weekly data from {wk_start} to {wk_end_inclusive}: window_desc={window_desc}")

    week_df = data.loc[mask].sort_values('date')
    
    return week_df

def _week_bounds(
        anchor_ts: pd.Timestamp, 
        week_start: str = "MON"
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Returns the start and end of a week, given an anchor timestamp. 
    Accepts day of start as MON or SUN"""
    
    WEEK_START_IDX = {"MON": 0, "SUN": 6}
    start_idx = WEEK_START_IDX[week_start.upper()]

    anchor_norm = anchor_ts.normalize()
    delta_days = (anchor_norm.weekday() - start_idx) % 7
    wk_start = anchor_norm - pd.Timedelta(days=delta_days)
    wk_end_inclusive = wk_start + pd.Timedelta(days=6)
    return wk_start, wk_end_inclusive

def get_current_period_config()-> dict:
    return JsonConfigManager().load_json_config()["current_period_data"]

def filter_df_excluded(
    df:pd.DataFrame,
    excluded_list:list|None=None,
    ) -> DataFrame:

    if excluded_list is None:
        excluded_list = JsonConfigManager().load_json_config().get("current_period_data", {}).get("default_exclude", None)
        if excluded_list is None: return df
    
    log.debug(f"Filtering df to exclude {excluded_list}")
    filter_df = df[~df["subject"].isin(excluded_list)] 
    return filter_df

def add_start_date_df(df_in, start_date=None, course=None, period=None)->pd.DataFrame:
    '''
    Backfills a dataframe with date column with empty data on "subject" and "time_spent_hrs".
    Allows giving it a start_date, a course and period to search the db (TO ADD IN FUTURE), or
    nothing at all thus using the current period start_date by default.
    '''
    def get_period_start_date()-> datetime:
        period_config = get_current_period_config()
        raw = period_config.get("period_start_date", None)
        if raw is None:
            raise ValueError("period_start_date missing in current period config")
        return datetime.strptime(raw, '%d-%m-%Y') # expects "15-9-2025"

    df_date_start = pd.Timestamp(df_in['date'].min())

    if (start_date is None and (course is None or period is None)):
        period_init = get_period_start_date()
    # elif (course is not None and period is not None):
    #     #TODO in future. This should search course and period data in db and retrieve the start date. 
    #     return
    else:
        period_init = start_date
    
    if period_init == df_date_start:
        log.debug(f"df has the same initial date as period init.")
        return df_in

    df = df_in.copy()

    # missing_df = pd.DataFrame({'date': pd.date_range(start=period_init, end=df_date_start)})
    missing_days = pd.date_range(
        start=period_init,
        end=(df_date_start - pd.Timedelta(days=1)),
        freq='D'
    )
    missing_df = pd.DataFrame({'date': missing_days})

    if course and period is None:
        course_name = df['course'].iloc[0]
        period_name = df['period'].iloc[0]
    else:
        course_name = course
        period_name = period

    missing_df['course'] = course_name
    missing_df['period'] = period_name
    missing_df['subject'] = None
    missing_df['time_spent_hrs'] = 0.0

    full_df = pd.concat([missing_df, df], ignore_index=True).sort_values('date', kind='stable')

    return full_df
    
