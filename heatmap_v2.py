# Developed for SBL Knowledge Services Limited, (c) AIML 2025
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

############################
# 1) Read the Excel File
############################
file_path = r"D:\AWR\Miscallaneous Code\ANUSHA_20250304_114833.xlsx"
xls = pd.ExcelFile(file_path)

# Load the relevant sheets
duration_ocr_summary = pd.read_excel(xls, sheet_name="Duration and OCR Summary")
ocr_summary = pd.read_excel(xls, sheet_name="OCR Summary")  # Contains "OCR Attempt"

############################
# 2) Helper Function
############################
def time_to_minutes(time_str):
    if isinstance(time_str, str):
        try:
            h, m, s = map(int, time_str.split(':'))
            return h * 60 + m + s / 60.0
        except ValueError:
            return np.nan
    return np.nan

############################
# 3) Data Cleaning
############################
# Convert time columns to numeric (minutes)
duration_ocr_summary["Total Duration"] = duration_ocr_summary["Total Duration"].apply(time_to_minutes)
duration_ocr_summary["Total Ideal Time"] = duration_ocr_summary["Total Ideal Time"].apply(time_to_minutes)

# Create new column: total_time = Total Duration - Total Ideal Time
duration_ocr_summary["total_time"] = duration_ocr_summary["Total Duration"] - duration_ocr_summary["Total Ideal Time"]

# Convert Date columns to datetime, keep only date
duration_ocr_summary = duration_ocr_summary[pd.to_datetime(duration_ocr_summary["Date"], errors="coerce").notna()]
duration_ocr_summary["Date"] = pd.to_datetime(duration_ocr_summary["Date"]).dt.date

ocr_summary = ocr_summary[pd.to_datetime(ocr_summary["date"], errors="coerce").notna()]
ocr_summary["date"] = pd.to_datetime(ocr_summary["date"]).dt.date

# Normalize User columns: strip and lowercase
duration_ocr_summary["User"] = duration_ocr_summary["User"].astype(str).str.strip().str.lower()
ocr_summary["user"] = ocr_summary["user"].astype(str).str.strip().str.lower()

# Debug: Check OCR Attempt values before cleaning
print("Unique OCR Attempt values before cleaning:", ocr_summary["OCR Attempt"].unique())

# Additional cleaning on OCR Attempt: remove commas, trim spaces
ocr_summary["OCR Attempt"] = ocr_summary["OCR Attempt"].astype(str).str.replace(",", "", regex=False).str.strip()
ocr_summary["OCR Attempt"] = pd.to_numeric(ocr_summary["OCR Attempt"], errors="coerce")
print("Unique OCR Attempt values after cleaning:", ocr_summary["OCR Attempt"].unique())
print("Non-null OCR Attempt count in OCR Summary:", ocr_summary["OCR Attempt"].count())

############################
# 4) Merge the Two Sheets
############################
merged_df = pd.merge(
    duration_ocr_summary,
    ocr_summary,
    left_on=["User", "Date"],
    right_on=["user", "date"],
    how="left"
)
merged_df.drop(columns=["user", "date"], inplace=True, errors="ignore")

# Debug: Check merge result for OCR Attempt
print("Sample of merged data (User, Date, OCR Attempt):")
print(merged_df[["User", "Date", "OCR Attempt"]].head(10))
print("Non-null OCR Attempt in merged_df:", merged_df["OCR Attempt"].count())

############################
# 5) Define Columns for Correlation
############################
selected_cols = [
    "total_time",
    "Total Shortcuts",
    "Total Character Count",
    "Total Records Processed",
    "Total Field Edits",
    "OCR Attempt",
    "Total Image Count",
    "Processed Image Count",
]

############################
# 6) Function to Filter and Compute Correlation
############################
def filter_and_correlate(user_id, start_date, end_date):
    filtered_df = merged_df[
        (merged_df["User"] == str(user_id).strip().lower()) &
        (merged_df["Date"] >= pd.to_datetime(start_date).date()) &
        (merged_df["Date"] <= pd.to_datetime(end_date).date())
    ]
    
    if filtered_df.empty:
        print("No data found for the given user and date range.")
        return
    
    available_cols = [col for col in selected_cols if col in filtered_df.columns]
    if not available_cols:
        print("None of the selected columns are available in the dataset.")
        return
    
    subset_df = filtered_df[available_cols].copy()
    
    for col in available_cols:
        subset_df[col] = pd.to_numeric(subset_df[col], errors="coerce")
    
    subset_df.dropna(how="all", inplace=True)
    if subset_df.empty:
        print("After cleaning, no valid numeric data remains.")
        return
    
    corr_matrix = subset_df.corr()
    
    plt.figure(figsize=(10, 6))
    sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", fmt=".2f")
    plt.title(f"Correlation Matrix for User {user_id} ({start_date} to {end_date})")
    plt.show()
    
    print("\nEntire Correlation Matrix:")
    print(corr_matrix)
    
    if "Total Records Processed" in corr_matrix.columns:
        print("\nCorrelations with 'Total Records Processed':")
        print(corr_matrix["Total Records Processed"].sort_values(ascending=False))
    else:
        print("Column 'Total Records Processed' not found in the selected columns.")

############################
# 7) Main Execution
############################
if __name__ == "__main__":
    user_id = input("Enter User ID: ")
    start_date = input("Enter Start Date (YYYY-MM-DD): ")
    end_date = input("Enter End Date (YYYY-MM-DD): ")
    
    filter_and_correlate(user_id, start_date, end_date)


