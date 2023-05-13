from metrics.mrr import MRR
class ItemKNN:

  def __init__(self,k,candidate_items):
    self.k = k
    self.candidates = candidate_items
  
  def fit(self,X,y):
    # return the accuracy of the training
    # MRR
    # X.shape=(n,nb_features)
    # Y.shape=(n,nb_features) 
    
    mrr = MRR()
    for item in X:
      distance = HammingDistance()
      for candidate in self.candidates:
        distance.evaluate(item,candidate)
      
      mrr.add(distance.get_best_k(self.k))
    mrr.update

    return mrr.metric
  
  def predict(self,session):
    mrr = MRR()
    
    distance = HammingDistance()
    for candidate in self.candidates:
      distance.evaluate(session,candidate)
    
    k_neigh = distance.get_best_k(self.k)
    mrr.add(k_neigh)

    # TODO convert the neighbor vectors
    # into item_ids
    return k_neigh, mrr.metric


  
