from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count



spark = SparkSession.builder\
        .master("local")\
        .appName("Dressipi-SBRS")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()


class DataPipeline:
    def __init__(self,path_to_data) -> None:

        self.path_to_data = path_to_data
        pass
    
    
    def extract(self,table="train_sessions.csv"):
        table_path = os.path.join(self.path_to_data,table)
        data = spark.read.format("csv")\
                    .option("header", "true")\
                    .load(table_path)
        return data
    
    def transform(self):
        pass
    
    def load(self,clean_data_path="train_sessions.csv"):

        clean_data_path = os.path.join(self.path_to_data,
                                  clean_data_path)
        clean_data = spark.read.format("csv")\
                    .option("header", "false")\
                    .load(clean_data_path)
        return clean_data
    
    def preprocess_dataset(self):

        # 1. clean dataset 
        # (remove duplicates from features)
        self.clean_dataset()
        
        # 2. Load clean data
        clean_data = self.load(
            clean_data_path="clean_item_features.csv"
            )
        
        # 3. transform item to vector of features

        # 4. remove duplicate vectors

        # 5. save new data

        pass
    
    def clean_dataset(self):
        # remove duplicate category ids in features
        data = self.extract(table="item_features.csv")
        
        # transform data
        data = data.dropDuplicates(["feature_category_id"])
        print(f'Distinct count of categories & items :\
                {data.feature_category_id.count()},\
                {data.item_id.count()}')
        
        # save cleaned features
        dest = os.path.join(self.path_to_data,
                            "clean_item_features.csv")
        data.write.csv(dest)
