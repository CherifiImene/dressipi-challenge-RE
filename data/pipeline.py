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
import time
import multiprocessing.pool




global pool
pool = multiprocessing.pool.ThreadPool(1000)

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
        sessions = self.extract(table="train_sessions.csv")\
                       .orderBy(["session_id","date"])
        features = self.extract(table="preprocessed_features/features.csv")

        
        # handle long sessions
        
        # handle medium and small session
        session_ids = sessions.dropDuplicates("session_id")\
                              .select("session_id")\
                              .collect()
        
        # preprocessed sessions will be stored here
        pre_sessions = self.create_rdd(data=spark.sparkContext.emptyRDD())
        
        # function to run in parallel
        def reduce_sessions(session_id):
          session_items = sessions.select(["session_id","item_id"])\
                                  .where(f"session_id == {session_id.session_id}")
          item_features = session_items.alias('sessions')\
                       .select(["session_id","item_id"])\
                       .join(features.alias("f"),
                             session_items.item_id == features.item_id,
                             "full")\
                       .drop(["sessions.item_id","f.item_id"])
          # Transform sessions
          # into a vector of features
          pre_session = item_features.groupBy(["session_id"])\
                       .rdd.reduceByKey(lambda x,y: x+y )
          pre_sessions = pre_sessions.union(pre_session)


          pool.map(reduce_sessions,session_ids,chunksize=len(session_ids))
        """for session_id in session_ids:
          session_items = sessions.select(["session_id","item_id"])\
                                  .where(f"session_id == {session_id.session_id}")
          item_features = session_items.alias('sessions')\
                       .select(["session_id","item_id"])\
                       .join(features.alias("f"),
                             session_items.item_id == features.item_id,
                             "full")\
                       .drop(["sessions.item_id","f.item_id"])
          # Transform sessions
          # into a vector of features
          pre_session = item_features.groupBy(["session_id"])\
                       .rdd.reduceByKey(lambda x,y: x+y )
          pre_sessions = pre_sessions.union(pre_session)"""

        # Save the new data
        self.save(data=pre_sessions,
                  dest_folder=dest_folder,
                  dest_file=dest_file)

        pass
    
    def preprocess_features(self,
                            features_path="clean_item_features/features.csv",
                            dest_folder="preprocessed_features",
                            dest_file="features.csv"):
      
        features = self.extract(table=features_path)
        items = features.dropDuplicates(["item_id"])\
                        .select("item_id")\
                        .collect()
        total_features = features\
                          .select(countDistinct("feature_category_id"))\
                          .collect()[0][0]
        print(f"total features: {total_features}")
        pre_features = []

        # the instructions to be run
        # inside the normal nested loop
        def _initialize_features_accumulator(item):
          #print(f"item: {item.item_id}")
          item_features = features\
                          .select(["item_id",
                                  "feature_category_id"]
                          )\
                          .where(f"item_id = {item.item_id}")\
                          .collect()
          num_features = [0]*(total_features+1) # 1 for the item_id, 73 for the features
          num_features[0] = item.item_id

          def increment_features_accumulator(feature):
            #print(f"feature: {feature.feature_category_id}")
            num_features[int(feature.feature_category_id)] += 1
            #print(f"num_features: {num_features}")
          
          pool.map(increment_features_accumulator,
                    item_features)
          return num_features
        
         

        pre_features = pool\
                  .map(_initialize_features_accumulator,
                        items,len(items)
                        )
        #for item in items:
        #  item_features, num_features = _initialize_features_accumulator()
          #for feature in item_features:
          #  num_features[int(feature.feature_category_id)] += 1
          
          #pre_features.append([num_features])
          
        # 3. transform them to (item_id, features_vector)
        preprocessed_df = self.create_rdd(pre_features)

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
    
