import logging
from load import load_data_to_duckdb

# Configure logging to write to etl.log
logging.basicConfig(
    filename='etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    '''
    Runs the etl pipeline
    :return: Pipeline is ran successfully  or not.
    '''
    logging.info("Starting the data pipeline...")
    try:
        load_data_to_duckdb()
        logging.info("Data pipeline completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during the pipeline execution: {e}")

if __name__ == "__main__":
    main()