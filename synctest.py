from matplotlib.pylab import f
from utils.logger import LoggerSingleton
from data.file_handler import JsonConfigManager, SPImportManager, stream_json_file

from pandas import DataFrame


def main():
    from data.sync_importers import AbstractSpoonTDLImporter

    csv_path = r"C:\Users\Lolo\Desktop\programming\local_repo\Task_Timeplot\past_data\1ero Farmacia Tasklist_Log.csv"

    df = AbstractSpoonTDLImporter.csv_to_df(csv_path)
    
    log.debug(df)

logger_instance = LoggerSingleton()
logger_instance.set_logger_config(level='DEBUG')
logger_instance.set_third_party_loggers_level(level='ERROR')

log = logger_instance.get_logger()

if __name__ == "__main__":
    main()

''' the following was used to export data from the first semester:
    config = JsonConfigManager().load_json_config()
    data_path = config["sync_data"]["sync_file_path"]
    log.debug(data_path)

    importer= SPImportManager(data_path)
    tasks, projects = importer.get_sp_data()

    cconfig = config["current_period_data"]
    ccourse=cconfig["current_course"]
    cperiod=cconfig["current_period"]
    
    ctasks = importer.clean_sp_tasks(
        tasks=tasks,projects=projects,
        ccourse=ccourse,
        cperiod=cperiod
    )

    log.info(f"{len(projects)} Projects and {len(tasks)} tasks raw")
    log.info(f"{len(ctasks)} cleaned tasks")

    df = importer.convert_tasks_to_df(ctasks, cstart=cconfig["period_start_date"])

    output_name = "_".join(f"{ccourse}_{cperiod}".lower().split())+".csv"
    df.to_csv(output_name, sep=";", encoding="utf-8", decimal=",")
    log.debug(f"Generated file at {output_name}")
'''