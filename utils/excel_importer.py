'''
from my own tools at:
https://github.com/LoloCG/Lolos_Packages/tree/main/Data_Analysis/Excel_Tools
'''
from tkinter.messagebox import NO
import pandas as pd
import os
from pathlib import Path

class ExcelImporter:
    '''
    Example usage: from a folder path, list all excel files and obtain the dataframe of one of them
        example_input_folder_path = Path(r'.\data_example')
        eimp = ExcelImporter().select_folder(example_input_folder_path)
        files = eimp.list_folder_excel_files()
        file_df = eimp.get_df_from_file(files[0])
    '''
    def __init__(self, file_path:str|Path=None, extraction_folder_dir=None):
        if file_path is not None:
            if type(file_path) == str: self.file_path = Path(file_path)
            else: self.file_path = file_path

        self.extraction_folder_dir = extraction_folder_dir

    def select_folder(self, folder_dir):
        if not os.path.exists(folder_dir):
            raise FileNotFoundError(f"Directory {folder_dir} does not exist.")
        elif not os.path.isdir(folder_dir):
            raise NotADirectoryError(f"{folder_dir} is not a directory.")
        else:
            self.extraction_folder_dir = Path(folder_dir)

        return self

    def list_folder_excel_files(self, extensions=('*.csv','*.xlsx','*.xls','*.xlsm')):
        '''
            Returns a List of strings of all files with the selected extensions located in the 
            folder path given.
        '''
        files = []
        for pattern in extensions:
            files.extend(self.extraction_folder_dir.glob(pattern))
        files = sorted(files)
        return [f.name for f in files]
    
    def get_df_from_file(self,
        filename: str | Path = None,
        folder: str|Path = None,
        import_nan: bool = False,
        csv_kwargs: dict = None,
        excel_kwargs: dict = None
    ) -> pd.DataFrame | dict[str, pd.DataFrame]:
        """
        Load a .csv or Excel file from the selected folder.
        
        - .csv -> returns a single DataFrame
        - .xls/.xlsx/.xlsm -> returns a dict of DataFrames, one per sheet
        
        import_nan=False will drop columns whose header is null/empty or
        where all values are NaN.
        Any kwargs you pass in csv_kwargs / excel_kwargs will override the defaults.
        """
        def get_from_csv():
            defaults = {"encoding": "utf-8"}
            df = pd.read_csv(path, **(defaults | csv_kwargs))

            if not import_nan:
                df = df.loc[:, df.columns.notnull()]
                df = df.loc[:, df.columns != '']
                df = df.dropna(axis=1, how='all')
            return df

        csv_kwargs   = {} if csv_kwargs   is None else csv_kwargs
        excel_kwargs = {} if excel_kwargs is None else excel_kwargs
        
        if self.file_path is not None: path = self.file_path
        elif self.file_path is None: path = folder / filename
        elif folder is None: folder = Path(self.extraction_folder_dir) 
        print(f"path={path}")
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(f"{path!r} does not exist or is not a file.")

        suffix = path.suffix.lower()
        
        if suffix == '.csv':
            return get_from_csv()

        elif suffix in ('.xls', '.xlsx', '.xlsm'):
            # map suffix -> engine
            engine_map = {'.xls': 'xlrd', '.xlsx': 'openpyxl', '.xlsm': 'openpyxl'}
            engine = engine_map[suffix]

            # read all sheets at once
            df_dict = pd.read_excel(
                path,
                sheet_name=None,
                engine=engine,
                **excel_kwargs
            )

            if not import_nan:
                cleaned = {}
                for name, sheet_df in df_dict.items():
                    tmp = sheet_df.loc[:, sheet_df.columns.notnull()]
                    tmp = tmp.loc[:, tmp.columns != '']
                    tmp = tmp.dropna(axis=1, how='all')
                    cleaned[name] = tmp
                return cleaned

            return df_dict

        else:
            raise ValueError(f"Unsupported extension {suffix!r}. "
                             "Use .csv, .xls, .xlsx or .xlsm.")

    def get_file_sheets(self, filename: str | Path): # TODO
        # the previous old function loaded the file to dataframe to obtain the sheets it contains
            # was done through "pd.ExcelFile()" function. 
            # maybe there is another way to do so?
            #  
        pass

    def add_sheets(self, excel_sheets): # TODO
        # add validation to check if the provided sheets actually exist in the Excel file. 
        for sheet in excel_sheets:
            self.sheets_to_extract.append(sheet)
        
        return self

    def _detect_delimiter(self): # TODO
        # delimiters = [',', ';', '\\', '\t'] 
        pass
    
    def _detect_encoding(self): # TODO
        # encodings = ['utf-8', 'utf-8-sig', 'utf-16', 'latin1', 'iso-8859-1', 'cp1252']
        pass

    def _detect_headers(self): # TODO
        # used for the skiprows parameter in some functions (csv_to_dataframe)
        pass