from load import * 
from config import load_config
from extract import *
from transform import *

def etl_pipeline():
        # Step1 : ---------------Extract Data------------
        # Get on the data from API
        with open("Student Depression Dataset.csv", "w") as data:
            download_dataset()
        # Read data file
        df = read_file("Student Depression Dataset.csv")


        # Step2 : ---------------Transform Data----------
        trans_data_obj = DataTransform(df)
        trans_data_obj.handleDuplicated()
        trans_data_obj.handleMissing()
        trans_data_obj.reformatColsNames()
        trans_data_obj.rmIrreltCol(["id"])
        trans_data = trans_data_obj.getCleanedData()

        # Step3: ---------------Load Data into repo(Postgresql)------------
        config = load_config()
        conn = connect_postgresql(config)
        load_data_to_postgresql(conn, trans_data, "student_info")
 
        
if __name__ == "__main__":
    etl_pipeline()
