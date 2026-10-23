# About
Personal productivity tool to visualize time-tracked tasks for comparison, etc.
It is also intended for myself familiarize with the following frameworks:
- data science: pandas, numpy, matplotlib
- general: sqlalchemy, ijson
- frontend: kivy (will change to pyside in future)

This is intended for myself alone, so clean code is not intended beyond the required maintenance performed by myself alone.

## Task apps linked
App uses sync files generated from:
### [Abstract's spoon ToDoList](https://abstractspoon.com/)
Generates an output .csv file that is cleaned off and normalized to be stored in sqflite db generated.

### SuperProductivity
Generates a json blob with time tracked per task.

## DB task structure
Due to SuperProductivity allowing only the use of a minimal hierarchy of Projects>Task>Subtask, Only the first two are used, indicating the Project as the university/study subject, and task as itself.

## Build a portable Windows release

Run these commands from the repository root:

```text
py -3 -m venv .venv
```

Activate the virtual environment in PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Or activate it in Command Prompt (`cmd.exe`):

```bat
.venv\Scripts\activate.bat
```

Then install the dependencies and build the release (the following commands work in either shell):

```text
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install --upgrade pyinstaller

python -m PyInstaller --noconfirm --clean --onefile --windowed --name TaskTimeplot --distpath ".\dist\TaskTimeplot-portable" app.py

powershell.exe -NoProfile -Command "Copy-Item -LiteralPath '.\config.json','.\studyanalytics.db' -Destination '.\dist\TaskTimeplot-portable' -Force"

powershell.exe -NoProfile -Command "Compress-Archive -Path '.\dist\TaskTimeplot-portable' -DestinationPath '.\dist\TaskTimeplot-portable-windows-x64.zip' -CompressionLevel Optimal -Force"
```

The command creates both an unpacked portable release and a compressed ZIP beside it:

```text
dist/
|-- TaskTimeplot-portable/
|   |-- TaskTimeplot.exe
|   |-- config.json
|   `-- studyanalytics.db
`-- TaskTimeplot-portable-windows-x64.zip
```

The ZIP contains the complete `TaskTimeplot-portable` directory and is the recommended distribution artifact. PyInstaller temporarily extracts the Python runtime and application dependencies when the executable starts; those extracted files are removed when it exits. The build architecture matches the Python installation used to run PyInstaller.

The application must resolve `config.json` and `studyanalytics.db` relative to the directory containing `TaskTimeplot.exe` when running as a frozen executable. Do not bundle these writable files with PyInstaller's `--add-data`; files embedded in a one-file executable are extracted to a temporary directory and are not suitable for persistent application data.
