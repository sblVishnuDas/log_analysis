# Developed for SBL Knowledge Services Limited, (c) AIML 2025

import subprocess
import sys
import logging

def run_script(script_path, logger):
    """
    Run the specified Python script in a new subprocess and log the results.
    """
    logger.debug(f"Preparing to run script: {script_path}")
    try:
        # 'check=True' will raise a CalledProcessError if the script exits non-zero
        subprocess.run([sys.executable, script_path], check=True)
        logger.info(f"Successfully ran {script_path}")
    except subprocess.CalledProcessError as e:
        logger.error(f"An error occurred while running {script_path}: {e}")
        # Decide whether to exit or continue execution if a script fails.
        # sys.exit(1)  # Uncomment if you want the entire process to stop on error.

def main():
    scripts = [
        r"C:\Users\18262\Music\orginal_log\folder.py",
        r"D:\1 Log Interface\1 LOgin_project\1 LAST CODE\ANUSHA _log_Final.py",
        r"D:\1 Log Interface\1 LOgin_project\1 LAST CODE\data_gpt_7.py"
    ]
    
    # Set up basic logging
    logger = logging.getLogger("MainRunner")
    # Example: set the level to DEBUG for full verbosity
    logger.setLevel(logging.DEBUG)
    
    # Add a console handler with a simple format
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    logger.debug("Starting to run all scripts...")

    for script in scripts:
        run_script(script, logger)

    logger.debug("Finished running all scripts.")

if __name__ == "__main__":
    main()
