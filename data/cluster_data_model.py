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
     FloatType,
     IntegerType
     )
import os
import pandas as pd

from metrics.hamming_distance import HammingDistance
from utils.models import K_MODES

class ClDataModel:
    """
    This class is responsible for dividing the sessions
    into 2 clusters: relevant and non relevant items
    and 
    extracting the characteristics of each cluster 
    """
    #column_name,data_type,nullable,metadata
    schema = StructType([StructField('session_length', IntegerType(), False),
              StructField('var1_d', FloatType(), False),
              StructField('var2_d', FloatType(), False),
              StructField('var1_t', FloatType(), False),
              StructField('var2_t', FloatType(), False),
              StructField('true_cluster', IntegerType(), False)
              ])
    def __init__(self) -> None:
      self.__initialization()
    
    def __initialization(self): 
        self.dp = DataPipeline()

    def sample(self,sample_size):
        sessions_count = self.dp\
                             .extract("sessions_count/sessions_count.csv")\
                             .where("nb_items >= 20")
        # sample data such that
        # session length | sample fraction
        # 20-30 ---> 70%
        # 31-50 ---> 80%
        # 51-100 --> 100%

        fractions = [0.7,0.8,1.0]
        fractions_dic = {key:fractions[(key>30 and key<= 50) + (key>50 and key<=100)*2] for key in range(20,101)}
        # Sample schema: session_id, nb_items
        sample = sessions_count.where("nb_items >= 20").sampleBy(col="nb_items",fractions=fractions_dic)
        
        
        sessions = self.dp.extract("train_sessions.csv")
        sample = sessions.join(
                        sample,
                        sessions.session_id == \
                        sessions_count.session_id,
                        "leftsemi")


        features = self.dp.extract(table="preprocessed_features/features.csv")

        # Contains session features
        # that the user was most interested in
        # during the session
        # Schema: session_id, date, item_id, F1,...,F73
        sample = sample.alias("s")\
                        .join(features.alias("f"),
                              features.item_id == sample.item_id,
                              "inner")\
                        .drop(col("s.item_id"))
      
        # save the sample
        self.dp.save(data=sample,
                     dest_folder="sample",
                     dest_file="sample.csv")
        return sample

    @pandas_udf(returnType=schema, functionType=PandasUDFType.GROUPED_MAP)
    def cluster_session(self,session):

      # The session doesn't have the puchased item



      # The session variable has all the item
      # consumed during a session
      # session schema: session_id, item_id, date, F1,...,F73
      X = session.sort_values(by=["date"],
                              ascending=False,
                              ignore_index=True)\
                      .drop(["date","item_id","session_id"],
                            axis=1)\
                      .loc[:,:].values
                      

      kmodes = K_MODES(k=2)
      max_iter= session.shape[0]
      c_objects, modes = kmodes.fit(X=X,max_iter=max_iter)

      # Label items in each cluster
      # Assign the time spent consuming the item
      # Compute the distances in each cluster
      # extract the variances
      
      # concatenate the mode with the session_id
      row = np.array([session.loc[0,"session_id"]])
      row = np.concatenate((row,modes[clean_data_cluster]),axis=0)

      df = pd.DataFrame(data=[row],
                        columns=session.columns)
      return df

    def reduce_cluster(self,cluster,items,times,mode):
       pass
    
    def create_or_update_df(self,data):
        pass
    
    def compute_time_diff(self):
        pass
