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

Before building a release, manually update the version in the executable, folder, and ZIP names below to match `APP_VERSION` in `app.py`.

Then install the dependencies and build the release (the following commands work in either shell):

```text
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install --upgrade pyinstaller

python -m PyInstaller --noconfirm --clean --onefile --windowed --name TaskTimeplot-v0.1.0 --distpath ".\dist\TaskTimeplot-v0.1.0-portable" app.py

powershell.exe -NoProfile -Command "Compress-Archive -Path '.\dist\TaskTimeplot-v0.1.0-portable' -DestinationPath '.\dist\TaskTimeplot-v0.1.0-portable-windows-x64.zip' -CompressionLevel Optimal -Force"
```

The commands package the executable into an unpacked portable release and a compressed ZIP beside it. Keep your existing `config.json` and `studyanalytics.db` beside the executable; on a fresh installation, the application creates missing files there on first run. After setup, the portable folder looks like this:

```text
dist/
|-- TaskTimeplot-v0.1.0-portable/
|   |-- TaskTimeplot-v0.1.0.exe
|   |-- config.json
|   `-- studyanalytics.db
`-- TaskTimeplot-v0.1.0-portable-windows-x64.zip
```

The ZIP contains the executable inside the versioned portable directory. To update an existing installation, replace its executable while keeping its data files. Move the whole portable folder to relocate the application and its data. PyInstaller temporarily extracts the Python runtime and application dependencies when the executable starts; those extracted files are removed when it exits. The build architecture matches the Python installation used to run PyInstaller.

The packaged application locates `config.json` and `studyanalytics.db` beside the executable, independently of the working directory or original build location. When running `app.py` from source, both files are located in the repository root. Do not bundle these writable files with PyInstaller's `--add-data`; files embedded in a one-file executable are extracted to a temporary directory and are not suitable for persistent application data.
