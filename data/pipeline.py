from pyspark.sql import SparkSession
from pyspark.sql.functions import(
    col, 
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
from collections import defaultdict
from functools import partial

spark = SparkSession.builder\
        .master("local")\
        .appName("Dressipi-SBRS")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()

class DataPipeline:
        
    def __init__(self,path_to_data,spark_session=spark) -> None:

        self.path_to_data = path_to_data
        self.spark = spark_session
        pass
    
    #------- ETL -------#
    def extract(self,table="train_sessions.csv"):
        table_path = os.path.join(self.path_to_data,table)
        data = self.spark.read.format("csv")\
                    .option("header", "true")\
                    .option("inferSchema",True)\
                    .load(table_path)
        return data
    
    def create_rdd(self,data):
        structures = []
        
        #column_name,data_type,nullable,metadata
        structures.append(StructField('item_id', StringType(), False))
        structures.extend([StructField(f'{i}', IntegerType(), True) for i in range(1,74)])

        schema = StructType(structures)
        rdd = self.spark.createDataFrame(data,schema)
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
    def preprocess_long_session(self,session):
      
      items = session.orderBy(['date'])\
                     .select("item_id")
      
      pre_features = self.extract(table="preprocessed_features/features.csv")
      
      recent_items = items.collect()[-1:-20]
      item_features = items.join(pre_features,
                                 items.item_id==pre_features.item_id,
                                 "right"
                                 )
      # filter based on the last 20 consumed items
      # if the similarity between an older item
      # and the vector of 20 recent items 
      # is < treshold
      # remove the item
      # elsewhere
      # accumulate its features to the current features
      
      pass

    def preprocess_sessions(self,clean_data=False,
                            dest_folder="preprocesse_sessions",
                            dest_file="train_sessions.csv"):
        
        # clean dataset 
        # (remove duplicates from features)
        if clean_data:
          self.clean_dataset()

        # Load sessions & clean features
        # order items by date
        sessions_count = self.extract("train_sessions.csv")\
             .groupBy(["session_id"])\
             .agg(count(col="item_id").alias("nb_items"))\
             .sort(col("nb_items"))\
             .where("nb_items <= 20")
        sessions = self.extract("train_sessions.csv")

        sessions = sessions.join(sessions_count,
                      sessions.session_id == sessions_count.session_id,
                      "leftsemi")


        features = self.extract(table="preprocessed_features/features.csv")

        
        # handle long sessions
        
        # handle medium and small session
        se_features = sessions.alias("s")\
                              .join(features.alias("f"),
                                    features.item_id == sessions.item_id,
                                    "inner")\
                              .drop(col("s.item_id"))\
                              .orderBy("date")\
                              .groupBy("session_id")\
                              .avg()
        
        # Save the new data
        self.save(data=se_features,
                  dest_folder=dest_folder,
                  dest_file=dest_file)

    
    def preprocess_features(self,
                            features_path="clean_item_features/features.csv",
                            dest_folder="preprocessed_features",
                            dest_file="features.csv"):
      
        features = self.extract(table=features_path)
        # transforms the features into a sparse vector
        preprocessed_df = features.groupBy(["item_id"])\
                                  .pivot("feature_category_id")\
                                  .count()\
                                  .fillna(0)

        # 4. save new data
        self.save(data=preprocessed_df,
                  dest_folder=dest_folder,
                  dest_file=dest_file)
      
      
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
    
