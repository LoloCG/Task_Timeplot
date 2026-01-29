from matplotlib.pylab import f
from utils.logger import LoggerSingleton
from data.file_handler import *
from data.importers.astdl_importer import AbstractSpoonTDLImporter

from pandas import DataFrame


def main():
    log.debug("start test")
    from core.orchestrators import StartSequence

    load=StartSequence.start_sequence()
    log.debug(f"payload from start sequence={load}")

def importpast():
    csv_path = r"C:\Users\Lolo\Desktop\programming\local_repo\Task_Timeplot\past_data\1ero Farmacia Tasklist_Log.csv"
    
    course_config = select_course_config(1,csv_path)

    df = AbstractSpoonTDLImporter.csv_to_df(csv_path, raw=False, csv_config_dict=course_config)
    log.debug(df)

def select_course_config(num, csv_path):
    import json, os
    json_config_path=r"past_data\past_courses_csv_data.json"
    
    course_config = None
    with open(json_config_path) as json_file: 
        course_config = json.load(json_file)[num-1]
        csv_name = course_config["csv_filename"]
        log.info(f"Imported json config in {json_config_path} for {csv_name}")
    
    return course_config
    
    base_filename = os.path.splitext(csv_path)[0].strip()


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