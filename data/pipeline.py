from pyspark.sql import SparkSession
from pyspark.sql.functions import(
    col, 
    Row,
    count, 
    countDistinct
)
from pyspark.sql.types import(
     StructType,
     StructField,
     StringType,
     ArrayType,
     IntegerType
     )

import os

class DataPipeline:
    spark = SparkSession.builder\
        .master("local")\
        .appName("Dressipi-SBRS")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()
        
    def __init__(self,path_to_data) -> None:

        self.path_to_data = path_to_data
        pass
    
    #------- ETL -------#
    def extract(self,table="train_sessions.csv"):
        table_path = os.path.join(self.path_to_data,table)
        data = spark.read.format("csv")\
                    .option("header", "true")\
                    .load(table_path)
        return data
    
    def create_rdd(self,data):
        schema = StructType([
                  StructField('item_id', StringType(), True),#column_name,data_type,nullable,metadata
                  StructField('features', ArrayType(IntegerType()), False),
                  ])
        #empty_rdd = spark.sparkContext.emptyRDD()

        rdd = spark.createDataFrame(data,schema)
        return rdd

    def transform(self):
        pass
    
    def load(self,clean_data_path="train_sessions.csv"):

        clean_data_path = os.path.join(self.path_to_data,
                                  clean_data_path)
        clean_data = spark.read.format("csv")\
                    .option("header", "false")\
                    .load(clean_data_path)
        return clean_data
    
    def save(self,data,dest_folder,dest_file):
        dest = os.path.join(self.path_to_data,
                            dest_folder)
        data.write.option("header",True)\
            .csv(dest)
        # rename saved file
        spark_out = list(filter(
            lambda filename: filename.startswith('part-'),
            os.listdir(dest)))[0]
        os.rename(dest+'/'+spark_out,dest+'/'+dest_file)

    #---------------------------#
    def preprocess_sessions(self,clean_data=False):
        
        # clean dataset 
        # (remove duplicates from features)
        if clean_data:
          self.clean_dataset()

        # Load sessions
        sessions = self.extract(table="train_sessions.csv")

        # order items by date
        # handle long sessions
        # handle medium and small session
        

        # Transform sessions
        # into a vector of features

        # Save the new data

        pass
    def preprocess_candidate_items(self,clean_data=False):

        # clean dataset 
        # (remove duplicates from features)
        if clean_data:
          self.clean_dataset()
        
        # 1. Extract candidate items 
        candidates = self.extract(table="candidate_items.csv").collect()
        clean_features = self.load(table="clean_item_features/features.csv")

        preprocessed_candidates = []

        for candidate in candidates:
          item_features = clean_features.select(
              ["item_id","feature_category_id"]
              ).where(f"item_id == {candidate.item_id}")\
              .collect()
          num_features = [0]*len(item_features)
          for feature in item_features:
            num_features[feature.feature_category_id-1] += 1
          
          preprocessed_candidates.append([candidate.item_id,
                                          num_features])
        
        # 3. transform them to (item_id, features_vector)
        preprocessed_df = self.create_rdd(preprocessed_candidates)

        # 4. save new data
        self.save(data=preprocessed_df,
                  dest_folder="prep_candidates",
                  dest_file="candidates.csv")
        
        
    
    def clean_dataset(self):
        # remove duplicate category ids in features
        data = self.extract(table="item_features.csv")
        
        # transform data
        data = data.orderBy(col("item_id"))\
                   .dropDuplicates(["item_id","feature_category_id"])
        
        #data.select(count(data.feature_category_id)).show()
        #data.select(count(data.item_id)).show()
          
        # save cleaned features
        self.save(data=data,
                  dest_folder="clean_item_features",
                  dest_file="features.csv")
    
