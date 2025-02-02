import subprocess
import logging
from datetime import datetime

# **Logging Setup**
LOG_FILE = "pipeline.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def run_module(module_name, script_name):
    """Runs a module and logs its execution status."""
    start_time = datetime.now()
    logging.info(f"Starting {module_name} at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        result = subprocess.run(["python", script_name], capture_output=True, text=True, check=True)
        end_time = datetime.now()
        logging.info(f"{module_name} completed successfully at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"Output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        error_time = datetime.now()
        logging.error(f"{module_name} failed at {error_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.error(f"Error Message: {e.stderr}")
        logging.error(f"Full Command Output: {e.output}")
        return False  # Stop execution if a module fails

    return True  # Continue if successful

def main():
    """Runs all modules in sequence and logs execution."""
    logging.info("Starting News Scraper Pipeline")

    # **Execute Modules Sequentially**
    if not run_module("Module 1: Preprocessing", "module_1.py"):
        return
    if not run_module("Module 2: Transformation", "module_2.py"):
        return
    if not run_module("Module 3: Scraping", "module_3.py"):
        return
    if not run_module("Module 4_5: Storage & Deduplication", "module_4_5.py"):
        return

    logging.info("Pipeline Execution Completed Successfully")

if __name__ == "__main__":
    main()