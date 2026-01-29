from dataclasses import dataclass
from pathlib import Path

import json

from utils.logger import LoggerSingleton
log = LoggerSingleton().get_logger()

@dataclass
class ConfigStatus:
    exists: bool
    valid: bool
    empty: bool
    data: dict | None

class ConfigManager:
    def __init__(self, path: Path = Path("config.json")):
        self.path = path

    def inspect(self)->ConfigStatus:
        if not self.path.exists():
            return ConfigStatus(
                exists=False,
                valid=False,
                empty=True,
                data=None,
            )
        
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)

        except json.JSONDecodeError:
            log.error(f"Json decode error during config inspection")
            return ConfigStatus(
                exists=True,
                valid=False,
                empty=True,
                data=None,
            )
        
        if not isinstance(data, dict):
            return ConfigStatus(
                exists=True,
                valid=False,
                empty=True,
                data=None,
            )

        return ConfigStatus(
            exists=True,
            valid=True,
            empty=(len(data) == 0),
            data=data,
        )

    def _deep_update(self, dst: dict, src: dict) -> dict:
        for k, v in src.items():
            if isinstance(v, dict) and isinstance(dst.get(k), dict):
                self._deep_update(dst[k], v)
            else:
                dst[k] = v
        return dst

    def save_dict_to_config(self, data):
        with open(self.path, 'w') as json_file:
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

    def json_upsert(self, new_data):
        cfg_file = Path(self.path)
        if cfg_file.exists():
            with open(cfg_file, "r", encoding="utf-8") as f:
                try:
                    config = json.load(f)
                except json.JSONDecodeError:
                    config = {}
        else:
            config = {}

        self._deep_update(config, new_data)

        with open(cfg_file, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4)

        return config
    
    def change_exclude_list(self, subjects: str | list[str]) -> dict:
        cfg_file = Path(self.path)

        if cfg_file.exists():
            with open(cfg_file, 'r', encoding='utf-8') as file:
                try:
                    config = json.load(file)
                except json.JSONDecodeError:
                    config = {}
        else:
            config = {}

        current_period_data = config.get('current_period_data')
        if not isinstance(current_period_data, dict):
            current_period_data = {}
            config['current_period_data'] = current_period_data

        # Normalize the input into a clean list[str]
        if isinstance(subjects, str):
            raw_items = subjects.split(',')
        elif isinstance(subjects, list):
            raw_items = subjects
        else:
            log.error(f"subjects to be added to exclude list shown as empty: ${subjects}")
            raw_items = []

        # Trim, drop empties, and deduplicate while preserving order
        seen = set()
        normalized: list[str] = []
        for item in raw_items:
            name = (item or "").strip()
            if name and name not in seen:
                seen.add(name)
                normalized.append(name)

        current_period_data['default_exclude'] = normalized  

        with open(cfg_file, 'w', encoding='utf-8') as file:
            json.dump(config, file, indent=2, ensure_ascii=False)

        return config
    
    def generate_new(self, sync_file_path:str|Path|None = None):
        if isinstance(sync_file_path, Path):str_sync_file = str(sync_file_path) 
        else: str_sync_file = sync_file_path
        
        base_config = {
            "sync_data": {
                "sync_file_path": str_sync_file,
                "last_update": 0,
                "archive_young": 0,
                "archive_old": 0,
                "update_date": 0
            },
            "current_period_data": {
                "current_course":None,
                "current_period":None,
                "period_start_date": None
            } 
        }
        log.info(f"Generating new config at {self.path.absolute()}")
        self.save_dict_to_config(base_config)
