

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count




spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()


class DataPipeline:
    def __init__(self) -> None:
        pass
    
    
    def extract(self):
        pass
    
    def transform(self):
        pass
    
    def load(self):
        pass
    
    def preprocess_dataset(self):
        pass
    
    def clean_dataset(self):
        pass