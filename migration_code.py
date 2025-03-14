import pandas as pd
import mysql.connector
from mysql.connector import Error
import datetime
import re
import sys
import traceback
import logging

###############################################################################
#                             LOGGING CONFIG
###############################################################################
logging.basicConfig(
    filename='debug.log',
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

###############################################################################
#                               DB CONNECTION
###############################################################################
def connect_to_mysql(host, user, password, database):
    """
    Establish connection to MySQL database
    """
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if connection.is_connected():
            db_info = connection.get_server_info()
            logging.info(f"Successfully connected to MySQL Server version {db_info}")
            logging.info(f"Connected to database '{database}'")
            return connection
    except Error as e:
        logging.error(f"Error connecting to MySQL: {e}", exc_info=True)
        return None

###############################################################################
#                           READ EXCEL
###############################################################################
def read_excel_data(file_path):
    """
    Read all sheets from the Excel file into a dictionary of DataFrames
    """
    try:
        logging.info(f"Reading Excel file: {file_path}")
        excel_data = pd.read_excel(file_path, sheet_name=None)
        logging.info(f"Successfully read {len(excel_data)} sheets: {', '.join(excel_data.keys())}")
        return excel_data
    except Exception as e:
        logging.error(f"Error reading Excel file: {e}", exc_info=True)
        return None

###############################################################################
#                           UTILITY FUNCTIONS
###############################################################################
def sanitize_column_names(df):
    """
    Convert column names to snake_case and remove spaces and special chars
    """
    df.columns = [
        re.sub(r'[^a-zA-Z0-9_]', '', col.lower().replace(' ', '_'))
        for col in df.columns
    ]
    return df

def location_exists_in_location_table(connection, location_code):
    """
    Check if location_code is in Location. Returns True if found, False otherwise.
    """
    cur = connection.cursor()
    q = "SELECT COUNT(*) FROM Location WHERE location_code=%s"
    cur.execute(q, (location_code,))
    res = cur.fetchone()
    return (res[0] if res else 0) > 0

def project_exists_in_project_table(connection, project_code):
    """
    Check if project_code is in Project. Returns True if found, False otherwise.
    """
    cur = connection.cursor()
    q = "SELECT COUNT(*) FROM Project WHERE project_code=%s"
    cur.execute(q, (project_code,))
    res = cur.fetchone()
    return (res[0] if res else 0) > 0

def tl_exists_in_tl_table(connection, tl_name):
    """
    Check if tl_name is in TL. Returns True if found, False otherwise.
    """
    cur = connection.cursor()
    q = "SELECT COUNT(*) FROM TL WHERE tl_name=%s"
    cur.execute(q, (tl_name,))
    res = cur.fetchone()
    return (res[0] if res else 0) > 0

def psn_exists_in_employee(connection, psn_value):
    """
    Check if `psn_value` is found in Employee table.
    Returns True if found, False if not.
    """
    cursor = connection.cursor()
    query = "SELECT COUNT(*) FROM Employee WHERE psn=%s"
    cursor.execute(query, (psn_value,))
    result = cursor.fetchone()
    return (result[0] if result else 0) > 0

def parse_timeval(x):
    """
    Parses an input (string, datetime.time, or pd.Timestamp) and returns a Python time object.
    Defaults to 00:00:00 if parsing fails or if x is None.
    """
    if isinstance(x, str):
        try:
            return pd.to_datetime(x).time()
        except:
            return datetime.time(0, 0, 0)
    elif isinstance(x, datetime.time):
        return x
    elif isinstance(x, pd.Timestamp):
        return x.time()
    return datetime.time(0, 0, 0)

###############################################################################
#                           TABLE CREATION
###############################################################################
def create_tables(connection):
    """
    Create all needed tables with the correct schema if they do not already exist.
    """
    cursor = connection.cursor()

    try:
        # Location table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Location (
                location_code VARCHAR(10) PRIMARY KEY,
                location_name VARCHAR(255)
            )
        """)

        # Project table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Project (
                project_code VARCHAR(10) PRIMARY KEY,
                project VARCHAR(255)
            )
        """)

        # TL table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TL (
                tl_name VARCHAR(255) PRIMARY KEY,
                location_code VARCHAR(10),
                FOREIGN KEY (location_code) REFERENCES Location(location_code)
            )
        """)

        # Employee table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Employee (
                psn VARCHAR(50) PRIMARY KEY,
                associate_name VARCHAR(255),
                experience VARCHAR(50),
                location_code VARCHAR(10),
                tl_name VARCHAR(255),
                manager VARCHAR(255),
                project_code VARCHAR(10),
                FOREIGN KEY (location_code) REFERENCES Location(location_code),
                FOREIGN KEY (tl_name) REFERENCES TL(tl_name),
                FOREIGN KEY (project_code) REFERENCES Project(project_code)
            )
        """)

        # Date_table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Date_table (
                date_id INT AUTO_INCREMENT PRIMARY KEY,
                cr_date DATE,
                psn VARCHAR(50),
                FOREIGN KEY (psn) REFERENCES Employee(psn)
            )
        """)

        # Shortcut
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Shortcut (
                shortcut_id INT AUTO_INCREMENT PRIMARY KEY,
                psn VARCHAR(50),
                cr_date DATE,
                shortcut_name VARCHAR(255),
                shortcut_value INT,
                FOREIGN KEY (psn) REFERENCES Employee(psn)
            )
        """)

        # Session_Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Session_Table (
                session_id INT AUTO_INCREMENT PRIMARY KEY,
                psn VARCHAR(50),
                cr_date DATE,
                location_code VARCHAR(10),
                project_code VARCHAR(10),
                start_time TIME,
                end_time TIME,
                duration_min FLOAT,
                FOREIGN KEY (psn) REFERENCES Employee(psn),
                FOREIGN KEY (location_code) REFERENCES Location(location_code),
                FOREIGN KEY (project_code) REFERENCES Project(project_code)
            )
        """)

        # Updated_Field
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Updated_Field (
                updated_id INT AUTO_INCREMENT PRIMARY KEY,
                psn VARCHAR(50),
                cr_date DATE,
                location_code VARCHAR(10),
                project_code VARCHAR(10),
                tl_name VARCHAR(255),
                doc_type INT,
                event_date_orig INT,
                event_place_orig INT,
                ext_unique_id INT,
                fs_image_nbr INT,
                fs_image_type INT,
                image_number INT,
                pr_bir_date_orig INT,
                pr_bir_place_orig INT,
                pr_fthr_name_gn_orig INT,
                pr_fthr_name_surn_orig INT,
                pr_mthr_name_gn_orig INT,
                pr_name_gn_orig INT,
                pr_name_surn_orig INT,
                pr_sex_code_orig INT,
                r_num INT,
                FOREIGN KEY (psn) REFERENCES Employee(psn),
                FOREIGN KEY (location_code) REFERENCES Location(location_code),
                FOREIGN KEY (project_code) REFERENCES Project(project_code),
                FOREIGN KEY (tl_name) REFERENCES TL(tl_name)
            )
        """)

        # OCR_Summary
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS OCR_Summary (
                ocr_id INT AUTO_INCREMENT PRIMARY KEY,
                psn VARCHAR(50),
                cr_date DATE,
                location_code VARCHAR(10),
                project_code VARCHAR(10),
                total_ocr_attempt INT,
                partially_ocr_attempt INT,
                ocr_attempt INT,
                total_ocr_duration_formatted TIME,
                FOREIGN KEY (psn) REFERENCES Employee(psn),
                FOREIGN KEY (location_code) REFERENCES Location(location_code),
                FOREIGN KEY (project_code) REFERENCES Project(project_code)
            )
        """)

        # Duration
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Duration (
                duration_id INT AUTO_INCREMENT PRIMARY KEY,
                psn VARCHAR(50),
                location_code VARCHAR(10),
                cr_date DATE,
                total_duration TIME,
                total_ideal_time TIME,
                total_break_time TIME,
                actual_ideal_time TIME,
                total_break_seconds INT,
                total_shortcuts INT,
                total_character_count INT,
                total_records_processed INT,
                total_field_edits INT,
                total_image_count INT,
                processed_image_count INT,
                FOREIGN KEY (psn) REFERENCES Employee(psn),
                FOREIGN KEY (location_code) REFERENCES Location(location_code)
            )
        """)

        logging.info("All necessary tables have been created or verified.")
    except Error as e:
        logging.error(f"Error creating tables: {e}", exc_info=True)

###############################################################################
#                     MIGRATION FUNCTIONS
###############################################################################
def migrate_location_data(connection):
    """
    Insert only four known locations into the Location table.
    """
    cursor = connection.cursor()
    locations = [
        ("MDS", "Madurai"),
        ("MNS", "Madurai Night"),
        ("TEN", "Thirunalveli"),
        ("TSI", "Thenkaasi")
    ]

    insert_count = 0
    for loc_code, loc_name in locations:
        try:
            sql = "INSERT IGNORE INTO Location (location_code, location_name) VALUES (%s, %s)"
            cursor.execute(sql, (loc_code, loc_name))
            insert_count += cursor.rowcount
        except Error as e:
            logging.error(f"Error inserting location '{loc_code}': {e}", exc_info=True)

    connection.commit()
    logging.info(f"Inserted {insert_count} rows into Location table (only 4 fixed records).")

def migrate_project_data(connection, excel_data):
    """
    Insert (project_code, project) from relevant sheets: Session Summary, Duration and OCR Summary, etc.
    Skips blank or None codes/names.
    """
    sheets = [
        'Session Summary',
        'Duration and OCR Summary',
        'Employee_Data',
        'OCR Summary'
    ]
    cursor = connection.cursor()
    insert_count = 0

    for sheet in sheets:
        if sheet not in excel_data:
            continue
        df = excel_data[sheet]
        df = sanitize_column_names(df)

        if 'project_code' in df.columns:
            for _, row in df.iterrows():
                code_raw = row.get('project_code', None)
                if pd.isna(code_raw):
                    continue
                code_str = str(code_raw).strip()
                if not code_str:
                    continue

                name_val = None
                if 'project' in df.columns and pd.notna(row['project']):
                    name_val = str(row['project']).strip()
                if not name_val:
                    name_val = code_str  # fallback

                try:
                    sql = """
                        INSERT IGNORE INTO Project (project_code, project)
                        VALUES (%s, %s)
                    """
                    cursor.execute(sql, (code_str, name_val))
                    insert_count += cursor.rowcount
                except Error as e:
                    logging.error(f"Error inserting project_code='{code_str}': {e}", exc_info=True)

    connection.commit()
    logging.info(f"Inserted {insert_count} projects into Project table (code & name only).")

def migrate_tl_data(connection, excel_data):
    """
    Insert TL(tl_name, location_code) from Employee_Data, skipping invalid references.
    """
    if 'Employee_Data' not in excel_data:
        logging.info("Employee_Data sheet not found, skipping TL migration.")
        return

    df = excel_data['Employee_Data']
    df = sanitize_column_names(df)

    cursor = connection.cursor()
    insert_count = 0

    for _, row in df.iterrows():
        tl_val = None
        for c in ['tl','tl_name']:
            if c in df.columns and pd.notna(row.get(c, None)):
                tl_val = str(row[c]).strip()
                break
        if not tl_val:
            continue

        loc_val = None
        for c in ['location','location_code']:
            if c in df.columns and pd.notna(row.get(c, None)):
                loc_val = str(row[c]).strip()
                break

        if not loc_val:
            continue

        if not location_exists_in_location_table(connection, loc_val):
            continue

        if not tl_exists_in_tl_table(connection, tl_val):
            try:
                sql = "INSERT IGNORE INTO TL (tl_name, location_code) VALUES (%s, %s)"
                cursor.execute(sql, (tl_val, loc_val))
                insert_count += cursor.rowcount
            except Error as e:
                logging.error(f"Error inserting TL '{tl_val}': {e}", exc_info=True)

    connection.commit()
    logging.info(f"Inserted {insert_count} TL rows into TL table.")

def migrate_employee_data(connection, excel_data):
    """
    Insert employees into Employee table from Employee_Data sheet.
    """
    if 'Employee_Data' not in excel_data:
        logging.info("Employee_Data sheet not found, skipping employee migration.")
        return

    df = excel_data['Employee_Data']
    df = sanitize_column_names(df)

    cursor = connection.cursor()
    inserted_count = 0

    for idx, row in df.iterrows():
        # parse psn
        psn_val = None
        for c in ['psn','user','user_id','user_name']:
            if c in df.columns and pd.notna(row.get(c, None)):
                psn_val = str(row[c]).strip()
                break
        if not psn_val:
            continue

        # parse associate_name
        assoc_val = None
        for c in ['associate_name','name','employee_name']:
            if c in df.columns and pd.notna(row.get(c, None)):
                assoc_val = str(row[c]).strip()
                break
        if not assoc_val:
            continue

        # parse experience
        exp_val = None
        for c in ['experience','exp']:
            if c in df.columns and pd.notna(row.get(c, None)):
                exp_val = str(row[c]).strip()
                break

        # parse location_code
        loc_val = None
        for c in ['location','location_code']:
            if c in df.columns and pd.notna(row.get(c, None)):
                loc_val = str(row[c]).strip()
                break
        # If location not in Location table, we skip
        if not loc_val or not location_exists_in_location_table(connection, loc_val):
            continue

        # parse tl_name
        tl_val = None
        for c in ['tl','tl_name']:
            if c in df.columns and pd.notna(row.get(c, None)):
                tl_val = str(row[c]).strip()
                break

        # parse manager
        mgr_val = None
        for c in ['manager','manager_name']:
            if c in df.columns and pd.notna(row.get(c, None)):
                mgr_val = str(row[c]).strip()
                break

        # parse project_code
        proj_val = None
        if 'project_code' in df.columns and pd.notna(row.get('project_code', None)):
            pot_proj = str(row['project_code']).strip()
            if project_exists_in_project_table(connection, pot_proj):
                proj_val = pot_proj

        # Insert
        try:
            sql = """
                INSERT IGNORE INTO Employee (
                    psn, associate_name, experience, location_code,
                    tl_name, manager, project_code
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (psn_val, assoc_val, exp_val, loc_val,
                                 tl_val, mgr_val, proj_val))
            inserted_count += cursor.rowcount
        except Error as e:
            logging.error(f"Error inserting employee {psn_val}: {e}", exc_info=True)

    connection.commit()
    logging.info(f"Inserted {inserted_count} employees from Employee_Data.")

def migrate_date_data(connection, excel_data):
    """
    Insert date data from all relevant sheets, skipping if references don't exist.
    We gather a set of (psn, date) pairs across all sheets to store them in Date_table.
    """
    sheets = excel_data.keys()
    cursor = connection.cursor()
    date_set = set()

    for sheet in sheets:
        df = excel_data[sheet]
        df = sanitize_column_names(df)

        date_cols = ['date','cr_date']
        psn_cols = ['psn','user','user_id','user_name']

        dcol = None
        pcol = None

        for c in date_cols:
            if c in df.columns:
                dcol = c
                break
        for c in psn_cols:
            if c in df.columns:
                pcol = c
                break

        if dcol and pcol:
            for _, row in df.iterrows():
                dv = row[dcol]
                pv = row[pcol]
                if pd.isna(dv) or pd.isna(pv):
                    continue

                date_val = None
                if isinstance(dv, str):
                    try:
                        date_val = pd.to_datetime(dv).date()
                    except:
                        continue
                elif isinstance(dv, pd.Timestamp):
                    date_val = dv.date()

                if not date_val:
                    continue

                psn_val = str(pv).strip()
                if not psn_val:
                    continue

                date_set.add((psn_val, date_val))

    insert_count = 0
    for (pv, dv) in date_set:
        if not psn_exists_in_employee(connection, pv):
            continue
        try:
            q = "INSERT IGNORE INTO Date_table (cr_date, psn) VALUES (%s, %s)"
            cursor.execute(q, (dv, pv))
            insert_count += cursor.rowcount
        except Error as e:
            logging.error(f"Error inserting date row (cr_date={dv}, psn={pv}): {e}", exc_info=True)

    connection.commit()
    logging.info(f"Inserted {insert_count} date rows into Date_table.")

def migrate_shortcut_data(connection, excel_data):
    """
    Migrate data into the Shortcut table from the 'Shortcut' sheet.
    """
    SHEET_NAME = "Shortcut"
    if SHEET_NAME not in excel_data:
        logging.info(f"{SHEET_NAME} not found, skipping Shortcut migration.")
        return

    df = excel_data[SHEET_NAME]
    df = sanitize_column_names(df)
    cursor = connection.cursor()
    insert_count = 0

    for _, row in df.iterrows():
        # parse psn
        psn_val = None
        for c in ['psn','user','user_id','user_name']:
            if c in df.columns and pd.notna(row.get(c, None)):
                psn_val = str(row[c]).strip()
                break
        if not psn_val or not psn_exists_in_employee(connection, psn_val):
            continue

        # parse cr_date
        date_val = None
        for c in ['cr_date','date']:
            if c in df.columns and pd.notna(row.get(c, None)):
                date_val = row[c]
                break
        if not date_val:
            continue

        # parse into real date
        if isinstance(date_val, str):
            try:
                date_val = pd.to_datetime(date_val).date()
            except:
                continue
        elif isinstance(date_val, pd.Timestamp):
            date_val = date_val.date()

        if not date_val:
            continue

        # Ensure date is in Date_table
        ensure_date_in_date_table(connection, psn_val, date_val)

        # parse shortcut_name
        s_name = None
        for c in ['shortcut_name','shortcutlabel','label']:
            if c in df.columns and pd.notna(row.get(c, None)):
                s_name = str(row[c]).strip()
                break
        if not s_name:
            s_name = "Unnamed"

        # parse 'shortcut_value'
        s_val = row.get('shortcut', None)
        if pd.isna(s_val):
            continue
        try:
            s_val = int(s_val)
        except:
            continue

        # Insert
        try:
            sql = """
                INSERT INTO Shortcut (psn, cr_date, shortcut_name, shortcut_value)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(sql, (psn_val, date_val, s_name, s_val))
            insert_count += cursor.rowcount
        except Error as e:
            logging.error(
                f"Error inserting Shortcut row (psn={psn_val}, cr_date={date_val}): {e}",
                exc_info=True
            )

    connection.commit()
    logging.info(f"Inserted {insert_count} rows into Shortcut table.")

###############################################################################
#                     NEW HELPER FUNCTIONS
###############################################################################
def get_employee_location_code(connection, psn_val):
    """
    Fetch the location_code from Employee for a given PSN.
    Returns None if not found or if location_code is NULL.
    """
    cursor = connection.cursor()
    try:
        query = "SELECT location_code FROM Employee WHERE psn = %s LIMIT 1"
        cursor.execute(query, (psn_val,))
        row = cursor.fetchone()
        if row and row[0]:
            return str(row[0]).strip()
        return None
    except Error as e:
        logging.error(f"Error fetching location_code for PSN={psn_val}: {e}", exc_info=True)
        return None

def get_employee_project_code(connection, psn_val):
    """
    Fetch the project_code from Employee for a given PSN.
    Returns None if not found or if project_code is NULL.
    """
    cursor = connection.cursor()
    try:
        query = "SELECT project_code FROM Employee WHERE psn = %s LIMIT 1"
        cursor.execute(query, (psn_val,))
        row = cursor.fetchone()
        if row and row[0]:
            return str(row[0]).strip()
        return None
    except Error as e:
        logging.error(f"Error fetching project_code for PSN={psn_val}: {e}", exc_info=True)
        return None

def get_employee_tl_name(connection, psn_val):
    """
    Fetch the tl_name from Employee for a given PSN.
    Returns None if not found or if tl_name is NULL.
    """
    cursor = connection.cursor()
    try:
        query = "SELECT tl_name FROM Employee WHERE psn = %s LIMIT 1"
        cursor.execute(query, (psn_val,))
        row = cursor.fetchone()
        if row and row[0]:
            return str(row[0]).strip()
        return None
    except Error as e:
        logging.error(f"Error fetching tl_name for PSN={psn_val}: {e}", exc_info=True)
        return None

def compute_duration_minutes(start_time, end_time):
    """
    Compute duration in minutes by subtracting start_time from end_time.
    Both should be datetime.time objects. Returns float minutes (rounded).
    """
    if not start_time or not end_time:
        return 0.0
    dt1 = datetime.datetime.combine(datetime.date(2025, 1, 1), start_time)
    dt2 = datetime.datetime.combine(datetime.date(2025, 1, 1), end_time)
    delta = dt2 - dt1
    return round(delta.total_seconds() / 60.0, 2)

def ensure_date_in_date_table(connection, psn_val, cr_date):
    """
    Ensures that Date_table has a record (psn_val, cr_date).
    Inserts it if missing.
    """
    cursor = connection.cursor()
    try:
        sql = "SELECT COUNT(*) FROM Date_table WHERE psn=%s AND cr_date=%s"
        cursor.execute(sql, (psn_val, cr_date))
        count = cursor.fetchone()[0]
        if count == 0:
            # Insert
            ins = "INSERT IGNORE INTO Date_table (cr_date, psn) VALUES (%s, %s)"
            cursor.execute(ins, (cr_date, psn_val))
            connection.commit()
    except Error as e:
        logging.error(f"Error ensuring (psn={psn_val}, cr_date={cr_date}) in Date_table: {e}", exc_info=True)

###############################################################################
#                     REVISED SESSION_TABLE MIGRATION
###############################################################################
def migrate_session_table(connection, excel_data):
    """
    Migrate data from 'Session Summary' into 'Session_Table',
    ensuring location_code and project_code come from Employee table,
    and that cr_date is recognized in Date_table.
    """
    SHEET_NAME = "Session Summary"
    if SHEET_NAME not in excel_data:
        logging.info(f"{SHEET_NAME} sheet not found, skipping Session_Table migration.")
        return

    df = excel_data[SHEET_NAME].copy()
    df.columns = [c.lower().replace(' ', '_').strip() for c in df.columns]
    
    cursor = connection.cursor()
    insert_count = 0
    skipped_count = 0
    
    for idx, row in df.iterrows():
        # 1) parse psn
        psn_val = None
        for c in ['psn','user','user_id','user_name']:
            if c in df.columns and pd.notna(row.get(c, None)):
                psn_val = str(row[c]).strip()
                break
        if not psn_val or not psn_exists_in_employee(connection, psn_val):
            skipped_count += 1
            continue

        # 2) fetch location_code and project_code from Employee
        loc_val = get_employee_location_code(connection, psn_val)
        proj_val = get_employee_project_code(connection, psn_val)

        # 3) parse cr_date from row
        date_val = None
        for c in ['date','cr_date']:
            if c in df.columns and pd.notna(row.get(c, None)):
                date_val = row[c]
                break
        if not date_val:
            skipped_count += 1
            continue

        # convert to date
        if isinstance(date_val, str):
            try:
                date_val = pd.to_datetime(date_val).date()
            except:
                skipped_count += 1
                continue
        elif isinstance(date_val, pd.Timestamp):
            date_val = date_val.date()
        if not date_val:
            skipped_count += 1
            continue

        # Ensure date is in Date_table
        ensure_date_in_date_table(connection, psn_val, date_val)

        # 4) parse times
        start_val = parse_timeval(row.get('start_time'))
        end_val   = parse_timeval(row.get('end_time'))

        # 5) parse or compute duration
        sheet_duration = None
        if 'duration_min' in df.columns:
            try:
                if pd.notna(row['duration_min']):
                    sheet_duration = float(row['duration_min'])
            except:
                sheet_duration = None

        if sheet_duration is not None:
            dur_val = sheet_duration
        else:
            dur_val = compute_duration_minutes(start_val, end_val)

        # 6) Insert row into Session_Table
        try:
            sql = """
                INSERT INTO Session_Table
                (psn, cr_date, location_code, project_code, start_time, end_time, duration_min)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (psn_val, date_val, loc_val, proj_val,
                                 start_val, end_val, dur_val))
            insert_count += cursor.rowcount
        except Error as e:
            logging.error(
                f"Error inserting row into Session_Table for psn={psn_val}, date={date_val}: {e}",
                exc_info=True
            )

    connection.commit()
    logging.info(f"Inserted {insert_count} rows into Session_Table. Skipped {skipped_count} rows.")

def migrate_updated_field_data(connection, excel_data):
    """
    Migrate data from the 'Updated Fields Pivot' sheet into 'Updated_Field'.
    - psn is taken from the row
    - location_code, project_code, and tl_name are fetched from the Employee table
    - cr_date is taken from the sheet row but also ensured in Date_table
    - The rest of the numeric columns come directly from the sheet
    """
    SHEET_NAME = "Updated Fields Pivot"
    if SHEET_NAME not in excel_data:
        logging.info(f"{SHEET_NAME} sheet not found, skipping Updated_Field migration.")
        return

    df = excel_data[SHEET_NAME].copy()
    df.columns = [c.lower().replace(' ', '_').strip() for c in df.columns]

    cursor = connection.cursor()
    insert_count = 0

    for _, row in df.iterrows():
        # parse psn
        psn_val = None
        for c in ['psn','user','user_id','user_name']:
            if c in df.columns and pd.notna(row.get(c, None)):
                psn_val = str(row[c]).strip()
                break
        if not psn_val or not psn_exists_in_employee(connection, psn_val):
            continue

        # parse date
        date_val = None
        for c in ['date','cr_date']:
            if c in df.columns and pd.notna(row.get(c, None)):
                date_val = row[c]
                break
        if not date_val:
            continue
        if isinstance(date_val, str):
            try:
                date_val = pd.to_datetime(date_val).date()
            except:
                continue
        elif isinstance(date_val, pd.Timestamp):
            date_val = date_val.date()
        if not date_val:
            continue

        # Ensure date is in Date_table
        ensure_date_in_date_table(connection, psn_val, date_val)

        # parse location_code and project_code and tl_name from Employee table
        loc_val = get_employee_location_code(connection, psn_val)
        proj_val = get_employee_project_code(connection, psn_val)
        tl_val = get_employee_tl_name(connection, psn_val)

        # Convert numeric columns
        def parse_int(x):
            if pd.isna(x):
                return 0
            try:
                return int(x)
            except:
                return 0

        doc_type               = parse_int(row.get('doc_type'))
        event_date_orig        = parse_int(row.get('event_date_orig'))
        event_place_orig       = parse_int(row.get('event_place_orig'))
        ext_unique_id          = parse_int(row.get('ext_unique_id'))
        fs_image_nbr           = parse_int(row.get('fs_image_nbr'))
        fs_image_type          = parse_int(row.get('fs_image_type'))
        image_number           = parse_int(row.get('image_number'))
        pr_bir_date_orig       = parse_int(row.get('pr_bir_date_orig'))
        pr_bir_place_orig      = parse_int(row.get('pr_bir_place_orig'))
        pr_fthr_name_gn_orig   = parse_int(row.get('pr_fthr_name_gn_orig'))
        pr_fthr_name_surn_orig = parse_int(row.get('pr_fthr_name_surn_orig'))
        pr_mthr_name_gn_orig   = parse_int(row.get('pr_mthr_name_gn_orig'))
        pr_name_gn_orig        = parse_int(row.get('pr_name_gn_orig'))
        pr_name_surn_orig      = parse_int(row.get('pr_name_surn_orig'))
        pr_sex_code_orig       = parse_int(row.get('pr_sex_code_orig'))
        r_num                  = parse_int(row.get('r_num'))

        try:
            sql = """
                INSERT INTO Updated_Field
                (psn, cr_date, location_code, project_code, tl_name,
                 doc_type, event_date_orig, event_place_orig, ext_unique_id,
                 fs_image_nbr, fs_image_type, image_number,
                 pr_bir_date_orig, pr_bir_place_orig, pr_fthr_name_gn_orig,
                 pr_fthr_name_surn_orig, pr_mthr_name_gn_orig,
                 pr_name_gn_orig, pr_name_surn_orig, pr_sex_code_orig, r_num)
                VALUES
                (%s, %s, %s, %s, %s,
                 %s, %s, %s, %s,
                 %s, %s, %s,
                 %s, %s, %s,
                 %s, %s,
                 %s, %s, %s, %s)
            """
            vals = (
                psn_val, date_val, loc_val, proj_val, tl_val,
                doc_type, event_date_orig, event_place_orig, ext_unique_id,
                fs_image_nbr, fs_image_type, image_number,
                pr_bir_date_orig, pr_bir_place_orig, pr_fthr_name_gn_orig,
                pr_fthr_name_surn_orig, pr_mthr_name_gn_orig,
                pr_name_gn_orig, pr_name_surn_orig, pr_sex_code_orig, r_num
            )
            cursor.execute(sql, vals)
            insert_count += cursor.rowcount
        except Error as e:
            logging.error(
                f"Error inserting Updated_Field row (psn={psn_val}, date={date_val}): {e}",
                exc_info=True
            )

    connection.commit()
    logging.info(f"Inserted {insert_count} rows into Updated_Field.")

def migrate_ocr_summary(connection, excel_data):
    """
    Migrate data from 'OCR Summary' into 'OCR_Summary' table.
    - psn from the row
    - location_code, project_code from Employee table
    - cr_date from the row (ensured in Date_table)
    - Remaining data from the row
    """
    SHEET_NAME = "OCR Summary"
    if SHEET_NAME not in excel_data:
        logging.info(f"{SHEET_NAME} not found, skipping OCR_Summary migration.")
        return

    df = excel_data[SHEET_NAME].copy()
    df.columns = [c.lower().replace(' ', '_').strip() for c in df.columns]

    cursor = connection.cursor()
    insert_count = 0

    for _, row in df.iterrows():
        # psn
        psn_val = None
        for c in ['psn','user','user_id','user_name']:
            if c in df.columns and pd.notna(row.get(c, None)):
                psn_val = str(row[c]).strip()
                break
        if not psn_val or not psn_exists_in_employee(connection, psn_val):
            continue

        # cr_date
        date_val = None
        for c in ['date','cr_date']:
            if c in df.columns and pd.notna(row.get(c, None)):
                date_val = row[c]
                break
        if not date_val:
            continue

        if isinstance(date_val, str):
            try:
                date_val = pd.to_datetime(date_val).date()
            except:
                continue
        elif isinstance(date_val, pd.Timestamp):
            date_val = date_val.date()
        if not date_val:
            continue

        # Ensure date is in Date_table
        ensure_date_in_date_table(connection, psn_val, date_val)

        # location_code, project_code from Employee
        loc_val = get_employee_location_code(connection, psn_val)
        proj_val = get_employee_project_code(connection, psn_val)

        def parse_int(x):
            if pd.isna(x):
                return 0
            try:
                return int(x)
            except:
                return 0

        total_ocr_attempt     = parse_int(row.get('total_ocr_attempt', 0))
        partially_ocr_attempt = parse_int(row.get('partially_ocr_attempt', 0))
        ocr_attempt           = parse_int(row.get('ocr_attempt', 0))
        total_ocr_fmt         = parse_timeval(row.get('total_ocr_duration_formatted', '00:00:00'))

        try:
            sql = """
                INSERT INTO OCR_Summary
                (psn, cr_date, location_code, project_code,
                 total_ocr_attempt, partially_ocr_attempt, ocr_attempt,
                 total_ocr_duration_formatted)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            vals = (
                psn_val, date_val, loc_val, proj_val,
                total_ocr_attempt, partially_ocr_attempt, ocr_attempt,
                total_ocr_fmt
            )
            cursor.execute(sql, vals)
            insert_count += cursor.rowcount
        except Error as e:
            logging.error(
                f"Error inserting row into OCR_Summary for psn={psn_val}, date={date_val}: {e}",
                exc_info=True
            )

    connection.commit()
    logging.info(f"Inserted {insert_count} rows into OCR_Summary.")

def migrate_duration(connection, excel_data):
    """
    Migrate data from "Duration and OCR Summary" into the Duration table.
    - psn from row
    - location_code from Employee
    - cr_date from row, ensured in Date_table
    - the rest from the row
    """
    SHEET_NAME = "Duration and OCR Summary"
    if SHEET_NAME not in excel_data:
        logging.info(f"{SHEET_NAME} not found, skipping Duration migration.")
        return

    df = excel_data[SHEET_NAME].copy()
    df = sanitize_column_names(df)
    cursor = connection.cursor()
    insert_count = 0

    for _, row in df.iterrows():
        # psn
        psn_val = None
        for c in ['psn','user','user_id','user_name']:
            if c in df.columns and pd.notna(row.get(c, None)):
                psn_val = str(row[c]).strip()
                break
        if not psn_val or not psn_exists_in_employee(connection, psn_val):
            continue

        # cr_date
        date_val = None
        for c in ['date','cr_date']:
            if c in df.columns and pd.notna(row.get(c, None)):
                date_val = row[c]
                break
        if not date_val:
            continue

        if isinstance(date_val, str):
            try:
                date_val = pd.to_datetime(date_val).date()
            except:
                continue
        elif isinstance(date_val, pd.Timestamp):
            date_val = date_val.date()
        if not date_val:
            continue

        # Ensure date is in Date_table
        ensure_date_in_date_table(connection, psn_val, date_val)

        # location_code from Employee
        loc_val = get_employee_location_code(connection, psn_val)
        if not loc_val:
            continue

        def parse_time(x):
            if pd.isna(x):
                return datetime.time(0, 0, 0)
            return parse_timeval(x)

        tot_dur   = parse_time(row.get('total_duration','00:00:00'))
        tot_ideal = parse_time(row.get('total_ideal_time','00:00:00'))
        tot_break = parse_time(row.get('total_break_time','00:00:00'))
        act_ideal = parse_time(row.get('actual_ideal_time','00:00:00'))

        def parse_int(x):
            if pd.isna(x):
                return 0
            try:
                return int(x)
            except:
                return 0

        break_sec = parse_int(row.get('total_break_seconds',0))
        short_cnt = parse_int(row.get('total_shortcuts',0))
        char_cnt  = parse_int(row.get('total_character_count',0))
        rec_proc  = parse_int(row.get('total_records_processed',0))
        field_ed  = parse_int(row.get('total_field_edits',0))
        img_count = parse_int(row.get('total_image_count',0))
        proc_cnt  = parse_int(row.get('processed_image_count',0))

        try:
            sql = """
                INSERT INTO Duration
                (psn, location_code, cr_date,
                 total_duration, total_ideal_time, total_break_time, actual_ideal_time,
                 total_break_seconds, total_shortcuts, total_character_count,
                 total_records_processed, total_field_edits,
                 total_image_count, processed_image_count)
                VALUES (%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s,%s, %s,%s)
            """
            vals = (
                psn_val, loc_val, date_val,
                tot_dur, tot_ideal, tot_break, act_ideal,
                break_sec, short_cnt, char_cnt,
                rec_proc, field_ed,
                img_count, proc_cnt
            )
            cursor.execute(sql, vals)
            insert_count += cursor.rowcount
        except Error as e:
            logging.error(
                f"Error inserting Duration row for psn={psn_val}, date={date_val}: {e}",
                exc_info=True
            )

    connection.commit()
    logging.info(f"Inserted {insert_count} rows into Duration table.")

###############################################################################
#                          CHECK & FIX FUNCTIONS
###############################################################################
def check_relationships(connection, excel_data):
    """
    Check the relationships between tables to ensure foreign keys match.
    Logs sample references for debugging.
    """
    cursor = connection.cursor()
    if "Session Summary" not in excel_data:
        logging.error("Session Summary sheet is missing from the Excel file.")
        return
    
    df = excel_data["Session Summary"].copy()
    df.columns = [c.lower().replace(' ', '_').strip() for c in df.columns]
    
    psn_columns = [c for c in df.columns if c in ['psn', 'user', 'user_id', 'user_name']]
    if not psn_columns:
        logging.error("No PSN column found in Session Summary sheet.")
        return
    
    psn_col = psn_columns[0]
    sample_psns = df[psn_col].dropna().unique()[:5]
    logging.info(f"Sample PSNs from Session Summary: {sample_psns}")
    
    for psn in sample_psns:
        cursor.execute("SELECT COUNT(*) FROM Employee WHERE psn=%s", (str(psn).strip(),))
        count = cursor.fetchone()[0]
        logging.info(f"PSN '{psn}' exists in Employee table: {count > 0}")

def fix_missing_references(connection, excel_data):
    """
    Fix missing references by adding necessary entries to the reference tables.
    Currently focuses on 'Session Summary' references. Could be extended.
    """
    cursor = connection.cursor()
    if "Session Summary" not in excel_data:
        logging.error("Session Summary sheet is missing from the Excel file.")
        return
    
    df = excel_data["Session Summary"].copy()
    df.columns = [c.lower().replace(' ', '_').strip() for c in df.columns]
    
    # Fix missing projects
    proj_columns = [c for c in df.columns if c in ['project', 'project_code']]
    if proj_columns:
        proj_col = proj_columns[0]
        missing_projects = []
        
        for proj in df[proj_col].dropna().unique():
            proj_str = str(proj).strip()
            if not project_exists_in_project_table(connection, proj_str):
                missing_projects.append((proj_str, proj_str))
        
        if missing_projects:
            logging.info(f"Adding {len(missing_projects)} missing projects to Project table")
            sql = "INSERT IGNORE INTO Project (project_code, project) VALUES (%s, %s)"
            cursor.executemany(sql, missing_projects)
            connection.commit()
    
    # Fix missing locations if the sheet had them
    loc_columns = [c for c in df.columns if c in ['location', 'location_code']]
    if loc_columns:
        loc_col = loc_columns[0]
        missing_locations = []
        
        for loc in df[loc_col].dropna().unique():
            loc_str = str(loc).strip()
            if not location_exists_in_location_table(connection, loc_str):
                missing_locations.append((loc_str, f"Location {loc_str}"))
        
        if missing_locations:
            logging.info(f"Adding {len(missing_locations)} missing locations to Location table")
            sql = "INSERT IGNORE INTO Location (location_code, location_name) VALUES (%s, %s)"
            cursor.executemany(sql, missing_locations)
            connection.commit()

###############################################################################
#                           MAIN
###############################################################################
def main():
    """
    Performs Excel-to-DB migration in strict order:
      1) Create all needed tables
      2) Migrate location, project, TL, employee
      3) Migrate date_table, shortcut
      4) Migrate session_table (fetch location/project from Employee, fetch or compute duration)
      5) Updated_Field
      6) OCR_Summary
      7) Duration
      8) Check relationships & fix references
      9) Re-run session migration if needed
    """
    host = "localhost"
    user = "root"
    password = "Password123"
    database = "project_p10"

    conn = connect_to_mysql(host, user, password, database)
    if not conn:
        logging.error("Failed DB connection, exiting.")
        sys.exit(1)

    if len(sys.argv) > 1:
        excel_path = sys.argv[1]
    else:
        excel_path = input("Enter the path to the Excel file: ")

    create_tables(conn)
    excel_data = read_excel_data(excel_path)
    if not excel_data:
        logging.error("Excel read error, exiting.")
        conn.close()
        sys.exit(1)

    # If we have a "Shortcut Analysis" sheet, convert it to "Shortcut"
    if "Shortcut Analysis" in excel_data:
        df_shortcut = excel_data["Shortcut Analysis"].copy()
        rename_map = {}
        for col in df_shortcut.columns:
            col_lower = col.lower()
            if any(x in col_lower for x in ["psn", "user", "employee"]):
                rename_map[col] = "psn"
            elif "date" in col_lower:
                rename_map[col] = "cr_date"
            elif "shortcut" in col_lower and "name" in col_lower:
                rename_map[col] = "shortcut_name"
            elif "shortcut" in col_lower:
                rename_map[col] = "shortcut"
        df_shortcut.rename(columns=rename_map, inplace=True)

        needed_cols = ["psn", "cr_date", "shortcut_name", "shortcut"]
        for c in needed_cols:
            if c not in df_shortcut.columns:
                if c == "shortcut_name":
                    df_shortcut[c] = "DEFAULT_SHORTCUT"
                elif c == "shortcut":
                    df_shortcut[c] = 0
                elif c == "psn":
                    df_shortcut[c] = "DEFAULT_USER"
                elif c == "cr_date":
                    df_shortcut[c] = pd.to_datetime("2025-01-01")

        df_shortcut["cr_date"] = pd.to_datetime(
            df_shortcut["cr_date"], errors="coerce"
        ).fillna(pd.to_datetime("2025-01-01"))
        df_shortcut["shortcut"] = pd.to_numeric(
            df_shortcut["shortcut"], errors="coerce"
        ).fillna(0)

        excel_data["Shortcut"] = df_shortcut
        logging.info("Shortcut Analysis sheet has been transformed and stored as 'Shortcut'.")

    try:
        migrate_location_data(conn)
        migrate_project_data(conn, excel_data)
        migrate_tl_data(conn, excel_data)
        migrate_employee_data(conn, excel_data)
        migrate_date_data(conn, excel_data)
        migrate_shortcut_data(conn, excel_data)
        migrate_session_table(conn, excel_data)
        migrate_updated_field_data(conn, excel_data)
        migrate_ocr_summary(conn, excel_data)
        migrate_duration(conn, excel_data)

        check_relationships(conn, excel_data)
        fix_missing_references(conn, excel_data)

        # Re-run session migration if references got fixed
        migrate_session_table(conn, excel_data)

        logging.info("Data migration completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during migration: {e}", exc_info=True)
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
