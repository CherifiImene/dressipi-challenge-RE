class MRR:
  BATCH_SIZE = 0
  MRR = 0
  def __init__(self):
    #self.length = length
    pass
  
  def add_session_to_batch():
    BATCH_SIZE += 1
    metric = 0
  def add(self,results):
    '''
    Update the metric with a result set and the correct next item.
    The result must be sorted correctly.
        
    Parameters
    --------
    results: pandas.Series
        Series of similarity scores with the rank as the index
    '''
    self.length = len(results)
    # This loop can be parallelized
    for rank,_ in enumerate(results):
      self.metric += (1/self.length)*(1/rank)
    
    MRR.add_session_to_batch()
    self.metric = 0

  def update(self):
    '''
      Updates the metric by averaging it over the batch size
      This method is called by the end of each batch
    '''
    metric /= BATCH_SIZE
    return metric
  
  def reset(self):
    BATCH_SIZE = 0
    self.length = 0
    self.metric = 0

