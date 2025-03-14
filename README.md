# log_analysis
Log Data for Project

#The following scripts are used in the system:
1 ) master_report_migration.py retrieves the production planned records.
2 ) migration.py is the main script responsible for migrating data from Excel to the database.
3 ) run.py executes multiple scripts based on their corresponding functions.
4 ) folder.py automates the extraction of log folders and retrieves all log.txt files within them.


#Database consists of multiple tables:
1 ) Production Data stores information about planned production records, including PSN, record date, project code, and location.
2 ) Location holds location details, including a unique location code and name.
3 ) Project contains project details with a unique project code and project name.
4 ) TL (Team Lead) associates team leads with specific locations.
5 ) Employee maintains employee details, including PSN, associate name, experience, location, team lead, manager, and project association.
6 ) Date Table records daily employee data by linking PSN with specific dates.
7 ) Session Table tracks employee work sessions, including start time, end time, location, and project details.
8 ) Duration stores details about total work duration, break times, shortcut usage, and processed data metrics.
9 ) OCR Summary records OCR-related attempts, duration, and processing metrics linked to employees, locations, and projects.
10 ) Updated Field tracks various data updates associated with employees, locations, team leads, and projects.
11 ) Shortcut logs shortcut usage by employees on specific dates.

