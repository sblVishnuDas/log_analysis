import os
import shutil
import re

def copy_log_and_txt_files(source_folder, destination_folder):
    # Pattern to match files like "3516_2025-02-19.log" and "3516_2025-02-19.txt"
    pattern = re.compile(r'^\d+_\d{4}-\d{2}-\d{2}\.(log|txt)$', re.IGNORECASE)
    
    copied_files = 0
    missing_files = []
    
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    # Track all target folders found
    target_folders = []

    # First pass: find all relevant folders (logs, log, lo, and other subdirectories)
    for root, dirs, files in os.walk(source_folder):
        folder_name = os.path.basename(root).lower()
        
        # Include "log", "logs", "lo" folders and any other subfolders like "13336"
        if folder_name in ["log", "logs", "lo"] or root.startswith(source_folder):
            target_folders.append(root)

    if not target_folders:
        print(f"No relevant folders found in {source_folder} or its subdirectories.")
        return

    print(f"Found {len(target_folders)} folders to search")

    # Second pass: process files in each target folder
    for folder in target_folders:
        print(f"Searching in: {folder}")

        try:
            for file in os.listdir(folder):
                file_path = os.path.join(folder, file)

                # Skip directories, process only files
                if not os.path.isfile(file_path):
                    continue

                # Check if the file matches our pattern (.log or .txt)
                if pattern.match(file):
                    try:
                        destination_path = os.path.join(destination_folder, file)

                        # Check if destination file already exists
                        if os.path.exists(destination_path):
                            print(f"File already exists at destination: {file}")
                            # Optional: Handle duplicates (rename, skip, overwrite)

                        shutil.copy2(file_path, destination_path)
                        copied_files += 1
                        print(f"Copied: {file}")
                    except Exception as e:
                        print(f"Error copying {file}: {str(e)}")
                        missing_files.append((file, str(e)))
        except Exception as e:
            print(f"Error accessing folder {folder}: {str(e)}")

    print(f"Total files copied: {copied_files}")

    if missing_files:
        print("\nThe following files could not be copied:")
        for file, reason in missing_files:
            print(f"- {file}: {reason}")
        
        print(f"\nTotal files missing: {len(missing_files)}")

if __name__ == "__main__":
    source_folder = r"C:\Users\11884\Documents\Log_Folder_data_13_TEN1\13.03.25"
    destination_folder = r"C:\Users\11884\Documents\Log_Folder_data_13_TEN\13.03.2025\Log_13_TEN1"

    if not os.path.exists(source_folder):
        print("Error: Source folder does not exist.")
    else:
        copy_log_and_txt_files(source_folder, destination_folder)
