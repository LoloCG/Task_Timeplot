from datetime import datetime, timezone
from warnings import deprecated
from numpy import full
from pandas import DataFrame
from sqlalchemy import true
from data.sqlalchemy import DBManager
from data.file_handler import SPImportManager, Path, DFTransformers, pd
from data.config_handler import ConfigManager
from core.charts import Charts

from utils.logger import LoggerSingleton
log = LoggerSingleton().get_logger()

SP_FILE = Path(r"C:\Users\Lolo\Nextcloud\Super Productivity\sync-data.json")

class StartSequence:
    @staticmethod
    def start_sequence():
        config_mng = ConfigManager()
        c_status = config_mng.inspect()

        # TODO: add recovery/migrate if exists but invalid
        log.debug(f"config status: exists={c_status.exists}, valid={c_status.valid}")
        if c_status.data == None or c_status.exists == False:
            config_mng.generate_new(sync_file_path=SP_FILE) # FIXME: at the moment whole code assumes hardcoded syncpath
            c_status = config_mng.inspect()
        if not c_status.valid:
            raise RuntimeError("Config exists but is invalid JSON.")
        
        db_mngr = DBManager()
        db_status = db_mngr.inspect_status()
        log.debug(f"db status={db_status}")
        if not db_status.can_connect: raise RuntimeError(f"ERROR: cannot connect to database.")
        if not db_status.exists or not db_status.tables_ok:
            log.debug(f"Generating db tables")
            db_mngr.createTables()


        mode = "NORMAL"
        current_period_req = False
        sync_path_req = True
        if c_status.data["sync_data"]["sync_file_path"]: sync_path_req = False

        if not db_status.has_data:
            log.debug(f"No data in db.")
            if StartSequence.config_cperiod_has_data(c_status.data) and not sync_path_req:
                log.debug(f"Using config data to import from sync source")
                StartSequence.first_import_from_config(
                    cfg_data=c_status.data,
                    cfg_mngr=config_mng,
                    db_mngr=db_mngr
                )
                mode = "IMPORTED"

            else: 
                log.debug(f"Cannot import without period data in config")
                mode = "FIRST_RUN"
                current_period_req = True
        
        return {
            "mode": mode,
            "ask_sync_path": sync_path_req,
            "ask_current_period": current_period_req
        }
    
    @staticmethod
    def first_import_from_config(cfg_data, cfg_mngr:ConfigManager=ConfigManager(), db_mngr:DBManager=DBManager()):
        '''
        ! - Assumes all necessary data required to import from superproductivity app is already present in config 
            (sync_file_path, period_start_date, current_course, current_period).
        Also updates config file with last update numbers. 
        '''
        cperiod_data = cfg_data["current_period_data"]
        sync_source = cfg_data["sync_data"]["sync_file_path"]
        
        # FIXME: this will fail with an empty date...
        period_start = datetime.strptime(cperiod_data["period_start_date"], '%d-%m-%Y').date()
        
        importer = SPImportManager(path_str=sync_source)
        tasks, projects = importer.get_sp_data(filter_date=period_start)
        flat_tasks = importer.clean_sp_tasks(
            tasks=tasks,
            projects=projects, 
            ccourse=cperiod_data["current_course"], 
            cperiod=cperiod_data["current_period"]
        )
        df = importer.convert_tasks_to_df(flat_tasks, cstart=None)

        db_mngr.upsert_to_tables(table='main', df=df)
        daily_df = DFTransformers.basic_to_daily_clean_new(df)
        db_mngr.upsert_to_tables(table='daily', df=daily_df)

        db_mngr.insert_period_data(
            course=cperiod_data["current_course"],
            period=cperiod_data["current_period"],
            start_date=period_start,
            finished=False,
            auto_exclude=cperiod_data['default_exclude']
        )
        
        sync_headers = importer.get_last_update_nums()
        data = {
            "sync_data": {
                "last_update": sync_headers["lastUpdate"],
                "archive_young": sync_headers["archiveYoung"],
                "archive_old": sync_headers["archiveOld"],
                "update_date": int(datetime.now(timezone.utc).timestamp() * 1000),
            }
        }
        cfg_mngr.json_upsert(data)

    @staticmethod
    def config_cperiod_has_data(cfg_data:dict)->bool:
        cperiod_data = cfg_data["current_period_data"]
        if any(value is None for value in cperiod_data.values()):
            return False
        return True 

class Orchestrators: 
    @staticmethod
    def plot_weekly_hours_bars(*_, course:str=None, period:str=None):
        # TODO: Will use daily data for the time being until weekly data is added to db.

        excluded = []
        start_date = None
        if course or period is None: 
            config      = get_current_period_config()
            course      = config["current_course"]
            period      = config["current_period"]
            excluded    = config['default_exclude']
            start_date  = config['period_start_date']

        log.debug(f"Plotting weekly data for {course}, {period}")
        df = DBManager().get_daily_data(course, period)
        df = filter_df_excluded(df, excluded_list=excluded)

        # df = add_start_date_df(df)
        df = fill_daily_missing_dates(
            daily_df=df,
            start_date=start_date
        )

        weekly_df = DFTransformers.temp_daily_to_weekly_clean(df)

        Charts.plot_weekly_stack_bar(weekly_df)
        return
    
    @staticmethod
    def plot_daily_hours_bars(*_, course:str=None, period:str=None):
        if course or period is None: 
            config = get_current_period_config()
            course=config["current_course"]
            period=config["current_period"]

        log.debug(f"Plotting daily data for {course}, {period}")
        db_mngr = DBManager()
        
        df = db_mngr.get_daily_data(course, period)
        period_data  = db_mngr.get_period_data(course, period)

        df = filter_df_excluded(df, excluded_list=period_data['auto_exclude'])
        
        # log.debug(f"period data start date={period_data['start_date']}")
        df = fill_daily_missing_dates(
            daily_df=df,
            start_date=period_data['start_date']
        )

        log.debug(f"full_df:\n{df}")
        Charts.plot_daily_stack_bar(df)
    
    @staticmethod
    def plot_total_horus_bars(*_, course:str=None, period:str=None):
        
        excluded = []
        if course or period is None: 
            config      = get_current_period_config()
            course      = config["current_course"]
            period      = config["current_period"]
            excluded    = config['default_exclude']

        log.debug(f"Plotting total hours data for {course}, {period}")
        df = DBManager().get_daily_data(course, period)
        df = filter_df_excluded(df, excluded_list=excluded)

        Charts.plot_total_period_hours_bars(df)

    @staticmethod
    def plot_week_avg_line_compared(*_, course:str=None, period:str=None):
        if course is None or period is None: 
            config = get_current_period_config()
            course=config["current_course"]
            period=config["current_period"]

        log.debug(f"Plotting general 7 day average data")

        db_mngr = DBManager()
        df = db_mngr.get_daily_data()

        filtered_groups = []
        for (course_i, period_i), g in df.groupby(['course', 'period']):
            # log.debug(f"in loop: {course_i}, {period_i}")
            period_data  = db_mngr.get_period_data(course_i, period_i)
                        
            g = filter_df_excluded(g, excluded_list=period_data['auto_exclude'])

            period_df = fill_daily_missing_dates(
                daily_df=g,
                start_date=period_data['start_date']
            )

            period_df = period_df.groupby(['course', 'period', 'date'], as_index=False)['time_spent_hrs'].sum()

            filtered_groups.append(period_df)
        df_filtered = pd.concat(filtered_groups, ignore_index=True)

        # log.debug(f"\n{df_filtered}")

        Charts.plot_rolling_7d_average_compared(
            df=df_filtered,  
            course_highlight=course, 
            # period_highlight=period, 
            window=7
        )
        return None
    
    @staticmethod
    def insert_df_to_db(df, ccourse, cperiod, cstart):
        db = DBManager()
        db.insert_to_main_data(df=df)

        period_start = {cperiod:cstart}
        daily_df = DFTransformers.basic_to_daily_clean_new(df, period_start)
        db.insert_daily_data(daily_df)

        # weekly_df = DFTransformers.daily_to_weekly_clean(daily_df)
        # db.insert_weekly_data(weekly_df)

        db.insert_period_data(
            course=ccourse, 
            period=cperiod, 
            start_date=datetime.strptime(cstart, '%d-%m-%Y'), 
            finished = False
        )

    @staticmethod
    def upsert_df_to_db(df):
        db = DBManager()
        db.upsert_to_tables(table='main', df=df)

        # period_start = {CURRENT_PERIOD:CURRENT_PERIOD_START}

        daily_df = DFTransformers.basic_to_daily_clean_new(df)
        db.upsert_to_tables(table='daily', df=daily_df)

        # weekly_df = DFTransformers.daily_to_weekly_clean(daily_df)
        # db.upsert_to_tables(table='weekly', df=weekly_df)

    @staticmethod
    def check_sp_sync():
        config_mng = ConfigManager()
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
        sync_config["update_date"] = int(
            datetime.now(timezone.utc).timestamp() * 1000
        )
        log.debug(f"Upserting config with {sync_config}")
        ConfigManager().json_upsert({"sync_data": sync_config})

    @staticmethod
    def get_basic_stats(*_) -> dict:
        log.debug(f"Getting basic stats")
        config = ConfigManager().load_json_config()
        config_sync = config["sync_data"]
        # last_dt_sync = config_sync['update_date']
  
        last_dt_sync = datetime.fromtimestamp(
            config_sync['update_date'] / 1000, 
            tz=timezone.utc).strftime("%Y-%m-%d %H:%M")

        df = DBManager().get_daily_data()
        df = filter_df_excluded(df, config.get("current_period_data", {}).get("default_exclude", None))

        last_db_day = df['date'].max()

        log.debug(f"last db day={last_db_day}")
        total_hours_last_day = df.loc[df['date'] == last_db_day, 'time_spent_hrs'].sum()
        
        week_df = get_week_df(df)

        total_week_hours = week_df['time_spent_hrs'].sum()
                
        days_elapsed = datetime.now().date().weekday() + 1
        avg_week_daily = total_week_hours / days_elapsed

        return {
            "last_sync":last_dt_sync,
            "last_db_day": last_db_day,
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
    return ConfigManager().load_json_config()["current_period_data"]

def filter_df_excluded(
    df:pd.DataFrame,
    excluded_list:list|str|None=None,
    ) -> DataFrame:
    if excluded_list is None or excluded_list == "[]": 
        log.debug(f"No subject filtered.")
        return df
    
    if isinstance(excluded_list, str):
        excluded_list = [x.strip() for x in excluded_list.split(",") if x.strip()]

    log.debug(f"Filtering df to exclude {excluded_list}")
    filter_df = df[~df["subject"].isin(excluded_list)] 
    return filter_df

def fill_daily_missing_dates(
        daily_df:pd.DataFrame, 
        start_date:pd.Timestamp|None=None
    ) ->pd.DataFrame:
    '''
    Fills missing dates from the daily dataframe with "time_spent_hrs"=0, "subject"=None
    Supposed to be used for only one course - period at a time, as it uses the first row to fill the rest on these. 
    '''    
    df = daily_df.copy()
    if df['course'].nunique() != 1 or df['period'].nunique() != 1:
        raise ValueError("daily_df must contain exactly one course and one period.")   
     
    # TODO: this shouldnt be necessary here as it is (or should be) handled upstream
    df['date'] = pd.to_datetime(df['date'])

    course = df['course'].iloc[0]
    period = df['period'].iloc[0]

    df_date_start = start_date if start_date else df['date'].min()
    df_date_end = df['date'].max()
    
    # full_range = pd.DataFrame({'date': pd.date_range(df_date_start, df_date_end, freq='D')})
    full_dates = pd.date_range(df_date_start, df_date_end, freq='D')

    present_dates = pd.DatetimeIndex(df['date'].dt.normalize().unique())
    missing_dates = pd.DatetimeIndex(full_dates).difference(present_dates)

    if len(missing_dates) == 0:
        return df.sort_values('date', kind='stable').reset_index(drop=True)

    filler = pd.DataFrame({
        'course': course,
        'period': period,
        'subject': None,
        'time_spent_hrs': 0.0,
        'date': missing_dates
    })

    out = (
        pd.concat([df, filler], ignore_index=True)
          .sort_values('date', kind='stable')
          .reset_index(drop=True)
    )

    # log.debug(f"Filled empty dates. Earliest date={out['date'].min()}")

    return out
    
@deprecated("...")
def add_start_date_df(df_in, start_date=None, course=None, period=None)->pd.DataFrame:
    '''
    Backfills a dataframe with date column with empty data on "subject" and "time_spent_hrs".
    Allows giving it a start_date, a course and period to search the db, or
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
    
