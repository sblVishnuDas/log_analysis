import pandas as pd
import mysql.connector
from datetime import datetime, timedelta

# Database connection details
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "Password123",
    "database": "project_p10"
}

def convert_excel_date(excel_date):
    """
    Convert an Excel serial date (1900-based) to Python datetime.
    Subtract 2 days to account for Excel’s leap-year bug in older Excel versions.
    Returns None if conversion fails.
    """
    excel_start_date = datetime(1900, 1, 1)
    try:
        days_to_add = int(excel_date) - 2
        return excel_start_date + timedelta(days=days_to_add)
    except:
        return None

def main():
    """
    Main function to:
    1. Drop the production_data table if it exists.
    2. Recreate the table with an id auto-increment column.
    3. Load Excel file, rename columns, parse date strings.
    4. Insert rows into production_data in MySQL.
    """
    # Connect to MySQL
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # 1) Drop table if it exists to ensure new schema is used
    drop_table_query = "DROP TABLE IF EXISTS production_data"
    cursor.execute(drop_table_query)
    conn.commit()

    # 2) Create the table with id auto-increment
    create_table_query = """
    CREATE TABLE production_data (
        id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        psn VARCHAR(50),
        record_date DATE,
        project_code VARCHAR(50),
        location VARCHAR(50),
        production_planned_records INT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # 3) Load Excel file
    file_path = r"C:\Users\18262\Downloads\Target_19_28.xlsx"
    df = pd.read_excel(file_path, sheet_name=0, header=0)
    print("Raw Excel Columns:", df.columns.tolist())

    # Identify the Production Planned Records column
    production_col = None
    for col in df.columns:
        if isinstance(col, str):
            col_lower = col.lower()
            if ("production" in col_lower
                and "planned" in col_lower
                and "records" in col_lower
                and "/hr" not in col_lower):
                production_col = col
                print(f"Found production planned records column: '{col}'")
                break

    if not production_col:
        raise ValueError("Could not find the Production planned Records column in the Excel file.")

    # Rename columns to standardized names, including "date" -> "record_date"
    df = df.rename(columns={
        "PSN": "psn",
        "Project Code": "project_code",
        "LOCATION": "location",
        production_col: "production_planned_records",
        "date": "record_date",
        "Date": "record_date",
        "DATE": "record_date"
    })

    # Parse record_date from strings like "2/19/2025"
    if "record_date" in df.columns:
        print("\nSample dates before conversion:")
        print(df["record_date"].head(3))

        # Convert date strings to datetime (if your file contains strings like '2/19/2025')
        df["record_date"] = pd.to_datetime(df["record_date"], errors='coerce', format='%m/%d/%Y')

        print("\nSample dates after conversion:")
        print(df["record_date"].head(3))

        if df["record_date"].isna().any():
            print(f"WARNING: {df['record_date'].isna().sum()} rows have invalid/empty dates.")
    else:
        print("WARNING: No 'record_date' column found after renaming. Check your Excel headers!")

    # Required columns
    required_columns = ["psn", "record_date", "project_code", "location", "production_planned_records"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Missing columns: {missing_columns}")
        print(f"Available columns: {df.columns.tolist()}")
        raise ValueError("Missing expected columns in Excel data.")

    # Keep only the required columns
    df = df[required_columns]

    print("\nSample data (first 3 rows):")
    print(df.head(3))

    # Convert production_planned_records to integer
    df["production_planned_records"] = pd.to_numeric(df["production_planned_records"], errors='coerce')
    df = df.dropna(subset=["production_planned_records"])
    df["production_planned_records"] = df["production_planned_records"].astype(int)

    # Prepare INSERT statement (do NOT include the auto-increment id)
    insert_query = """
    INSERT INTO production_data (
        psn, record_date, project_code, location, production_planned_records
    ) VALUES (%s, %s, %s, %s, %s)
    """

    # Format record_date as a string for MySQL if it's datetime
    if pd.api.types.is_datetime64_any_dtype(df["record_date"]):
        df["record_date"] = df["record_date"].dt.strftime('%Y-%m-%d')

    # Insert data
    data = df.values.tolist()
    cursor.executemany(insert_query, data)
    conn.commit()

    # Close DB connections
    cursor.close()
    conn.close()

    print(f"\nData successfully migrated to 'production_data' table. {len(data)} rows inserted.")

if __name__ == "__main__":
    main()
