from pandas.api.types import is_object_dtype, is_numeric_dtype
from ETL.extract import read_file
class DataTransform:
    def __init__(self, df):
        self.df = df

    def hasMissingVal(self, col):
        """
        Checks if a specific column has missing values.
        """
        return self.df[col].isnull().any()

    def hasDuplicated(self):
        """
        Checks if the DataFrame has duplicate rows.
        """
        return self.df.duplicated().any()

    def handleDuplicated(self):
        """
        Drops duplicate rows from the DataFrame.
        """
        self.df.drop_duplicates(inplace=True)

    def handleMissing(self):
        """
        Handles missing values in a column:
        - Fills with mode for categorical/object columns.
        - Fills with median for numeric columns.
        """
        for col in self.df.columns:
          if self.hasMissingVal(col):
              if is_object_dtype(self.df[col]) or self.df[col].dtype.name == 'category':
                  self.df[col] = self.df[col].fillna(self.df[col].mode()[0])
              elif is_numeric_dtype(self.df[col]):
                  self.df[col] = self.df[col].fillna(self.df[col].median())

    def rmIrreltCol(self, cols):
        """
        Removes irrelevant columns from the DataFrame.
        """
        self.df.drop(columns=cols, inplace=True)
    def reformatColsNames(self):
        rename_map = {
        "id": "id",
        "Gender": "gender",
        "Age": "age",
        "City": "city",
        "Profession": "profession",
        "Academic Pressure": "academic_pressure",
        "Work Pressure": "work_pressure",
        "CGPA": "cgpa",
        "Study Satisfaction": "study_satisfaction",
        "Job Satisfaction": "job_satisfaction",
        "Sleep Duration": "sleep_duration",
        "Dietary Habits": "dietary_habits",
        "Degree": "degree",
        "Have you ever had suicidal thoughts ?": "suicidal_thoughts",
        "Work/Study Hours": "work_study_hours",
        "Financial Stress": "financial_stress",
        "Family History of Mental Illness": "family_history",
        "Depression": "depression"
    }
    
        # Rename the columns in the original DataFrame
        self.df.rename(columns=rename_map, inplace=True)

    def getCleanedData(self):
        """
        Returns the cleaned DataFrame.
        """
        return self.df


# Transformation Function
def transformation():
    df = read_file()  # Reads data
    trans_data_obj = DataTransform(df)
    trans_data_obj.handleDuplicated()
    trans_data_obj.handleMissing()
    trans_data_obj.reformatColsNames()
    trans_data_obj.rmIrreltCol(["id"])
    return trans_data_obj.getCleanedData()
 