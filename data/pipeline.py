from pyspark.sql import SparkSession
from pyspark.sql.functions import(
    col,
    count,
    countDistinct,
    pandas_udf,
    PandasUDFType
)
from pyspark.sql.types import(
     StructType,
     StructField,
     StringType,
     ArrayType,
     IntegerType,
     FloatType
     )
import os
from collections import defaultdict
from functools import partial
import pandas as pd




class DataPipeline:
    schema = StructType([])

    def __init__(self,path_to_data=None,spark_session=None) -> None:
        self.__initialization(path_to_data=path_to_data,
                              spark_session=spark_session)


    def __initialization(self,spark_session=None,path_to_data=None):
      
        if not spark_session:
          self.spark = SparkSession.builder\
                      .master("local")\
                      .appName("Dressipi-SBRS")\
                      .config('spark.ui.port', '4050')\
                      .getOrCreate()
        
        if not path_to_data:
          path_to_data = './datasets/dressipi_recsys2022_dataset'
        
        self.path_to_data = path_to_data
        self.spark = spark_session

        self.schema = self.pre_sessions_schema

    @property
    def pre_sessions_schema(self):

        features = self.extract(table="preprocessed_features/features.csv")

        structures = []

        #column_name,data_type,nullable,metadata
        structures.append(StructField('session_id', IntegerType(), False))
        structures.extend([StructField(feature, IntegerType(), True)\
                           for feature in features.columns[1:]])

        schema = StructType(structures)
        return schema


    #--------------------------------- ETL -------------------------------------#
    def extract(self,table="train_sessions.csv"):
        table_path = os.path.join(self.path_to_data,table)
        data = self.spark.read.format("csv")\
                    .option("header", "true")\
                    .option("inferSchema",True)\
                    .load(table_path)
        return data

    def create_rdd(self,data):
        ddl_str = "session_id str,"
        return ddl_str

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

    #--------------------------------------------------------------------------#

    @pandas_udf(returnType=schema, functionType=PandasUDFType.GROUPED_MAP)
    def preprocess_long_session(self,session):
      # The session variable has all the item
      # consumed during a session
      session = session.sort_values(by=["date"],
                                    ascending=False)\
                      .drop(["date"],
                            axis=1)

      X = session.loc[:,session.columns[1:]].values # returns a numpy array
      kmodes = K_MODES(k=2)
      max_iter= session.shape[0]
      c_objects, modes = kmodes.fit(X=X,max_iter=max_iter)

      # return the mode of the cluster
      # that contains the last 10 items
      # Assuming that the latter are
      # more relevant
      x = session.loc[0,session.columns[1:]].values
      items = session.loc[:10,session.columns[1:]].values
      clean_data_cluster = 0
      for x in items:
        clean_data_cluster += np.argmin([HammingDistance.evaluate(x,modes[0]),
                                        HammingDistance.evaluate(x,modes[1]),
                                            ])

      clean_data_cluster = (clean_data_cluster > (0.5)*items.shape[0])*1
      # concatenate the mode with the session_id
      row = np.array([session.loc[0,"session_id"]])
      row = np.concatenate((row,modes[clean_data_cluster]),axis=0)

      df = pd.DataFrame(data=[row],
                        columns=session.columns)
      return df



    def _handle_l_m_sessions(self):

      if os.path.exists(os.path.join(
                            self.path_to_data,
                            "/sessions_count/sessions.csv")):
        sessions_count = self.extract("/sessions_count/sessions.csv")
      else:
        sessions_count = self.extract("train_sessions.csv")\
             .groupBy(["session_id"])\
             .agg(count(col="item_id").alias("nb_items"))

        self.save(data=sessions_count,
                  dest_folder="sessions_count",
                  dest_file="sessions.csv")


      sessions = self.extract("train_sessions.csv")\
                     .join(
                        sessions_count.where("nb_items > 10"),
                        sessions.session_id == \
                        sessions_count.session_id,
                        "leftsemi")


      features = self.extract(table="preprocessed_features/features.csv")

      # Contains session features
      # that the user was most interested in
      # during the session
      se_features = sessions.alias("s")\
                      .join(features.alias("f"),
                            features.item_id == sessions.item_id,
                            "inner")\
                      .drop(col("f.item_id"))\
                      .drop(col("s.item_id"))\
                      .groupBy("session_id")\
                      .apply(self.preprocess_long_session)
      return se_features

    def _handle_sh_session(self):
        if os.path.exists(os.path.join(
                            self.path_to_data,
                            "/sessions_count/sessions.csv")):
          sessions_count = self.extract("/sessions_count/sessions.csv")
        else:
          sessions_count = self.extract("train_sessions.csv")\
              .groupBy(["session_id"])\
              .agg(count(col="item_id").alias("nb_items"))

          self.save(data=sessions_count,
                    dest_folder="sessions_count",
                    dest_file="sessions.csv")

        sessions = self.extract("train_sessions.csv")\
                     .join(
                        sessions_count.where("nb_items > 10"),
                        sessions.session_id == \
                        sessions_count.session_id,
                        "leftsemi")


        features = self.extract(table="preprocessed_features/features.csv")

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
        # We consider as long session
        # a session that has more than 20 items


        # handle medium and small session
        se_features = sessions.alias("s")\
                              .join(features.alias("f"),
                                    features.item_id == sessions.item_id,
                                    "inner")\
                              .drop(col("s.item_id"))\
                              .groupBy("session_id")\
                              .avg()\
                              .drop("avg(session_id)","avg(item_id)")

        # Apply threshold
        def apply_threshold(reduced_session,threshold=0.4):
          column_names= ["session_id"]
          column_names.extend([f"avg({i})" for i in range(1,74)])

          transformed_row = [reduced_session["session_id"]]
          for column_name in column_names[1:]:
            if reduced_session[column_name] >= threshold:
              transformed_row.append(1)
            else:
              transformed_row.append(0)
          return transformed_row


        # rename columns
        column_names= ["session_id"]
        column_names.extend([f"{i}" for i in range(1,74)])

        se_features = se_features.rdd.map(apply_threshold).toDF(column_names)


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
