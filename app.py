from utils.logger import LoggerSingleton
from interface.kivy_main import MainWindows

def main():
    MainWindows().run()

logger_instance = LoggerSingleton()
logger_instance.set_logger_config(level='DEBUG')
logger_instance.set_third_party_loggers_level(level='ERROR')

log = logger_instance.get_logger()

if __name__ == "__main__":
    main()

# Cannot be added until system works with previous data correctly...
# AbstractSpoonTDLImporter.import_pastcourses()
