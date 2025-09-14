from datetime import datetime, timezone,  timedelta
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

        config_mng = JsonConfigManager()
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
        config_mng.save_dict_to_config(data)
        log.debug(f"saving config:\n{data}")

        DBManager().insert_period_data(
            course=ccourse,
            period=cperiod,
            start_date=datetime.strptime(period_start, '%d-%m-%Y').date(),
            finished=False
        )

class Orchestrators:        
    @staticmethod
    def plot_daily_hours_bars(*_, course:str=None, period:str=None):
        if course or period is None:
            config = JsonConfigManager().load_json_config()["current_period_data"]
            course=config["current_course"]
            period=config["current_period"]

        log.debug(f"Plotting daily data for {course}, {period}")
        df = DBManager().get_daily_data(course, period)
        Charts.plot_daily_stack_bar(df)

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
        
        importer = SPImportManager(sync_config["sync_file_path"])
        sync_headers = importer.get_last_update_nums()
        log.debug(f"sync headers = {sync_headers}")

        update_needed = (sync_headers["lastUpdate"] > sync_config.get("last_update",0))

        if not update_needed:
            log.info(f"No update required.")
            return
        
        log.info(f"Update required. Checking archived tasks.")
        
        local_young = int(sync_config.get("archive_young", 0))
        local_old   = int(sync_config.get("archive_old", 0))

        if local_young < sync_headers["archiveYoung"]:
            log.info(f"Update of young archive required ({local_young} vs {sync_headers["archiveYoung"]})")
        if local_old < sync_headers["archiveOld"]:
            log.info(f"Update of old archive required ({local_old} vs {sync_headers["archiveOld"]})")
        
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
        df = importer.convert_tasks_to_df(flat_tasks, cstart=None)
        Orchestrators.upsert_df_to_db(df)

        JsonConfigManager().json_upsert({
            "last_update":sync_headers["lastUpdate"],
            "update_date":str(datetime.now(timezone.utc))
        })

    @staticmethod
    def get_basic_stats(*_) -> dict:
        log.debug(f"Getting basic stats")
        config = JsonConfigManager().load_json_config()["sync_data"]
        last_dt_sync = datetime.fromisoformat(config['update_date'])

        df = DBManager().get_daily_data()
        last_db_day = df['date'].max()
        total_hours_last_day = df.loc[df['date'] == last_db_day, 'time_spent_hrs'].sum()

        return {
            "last_sync":last_dt_sync.date(),
            "last_db_day": last_db_day.date(),
            "last_db_hrs":total_hours_last_day
        }
        


