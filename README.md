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
