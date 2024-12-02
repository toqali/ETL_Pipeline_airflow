import kaggle
import pandas as pd



# get on data for once time using API from kaggle
def download_dataset():
    # Authenticate using Kaggle API
    kaggle.api.authenticate()
    # Download the dataset by its URL from Kaggle
    kaggle.api.dataset_download_files("hopesb/student-depression-dataset", unzip=True, path=".")

# read collected data
def read_file(path):
    extension = path.split(".")[-1]
    # read excel file
    if extension in ["xls", "xlsx", "xlsm", "xlsb"]:
        df = pd.read_excel(path)
    # read csv file
    elif  extension == "csv":
        df = pd.read_csv(path) 
    # read json file 
    elif extension == "json":
        df = pd.read_json(path)
    return df
 
    

