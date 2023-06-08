from math import inf
import random as rd
import numpy as np

class K_MODES:
    
    def __init__(self,k=2) -> None:
      # verify k>=2
      self.k = k
    

    def fit(self,X,max_iter=50):
      
      # Choose the latest(0) and oldest items(-1)
      # as starting modes
      # if the number of elements in a cluster is 1
      # verify wether the cluster can be concatenated with another cluster
      modes = np.array([X[0,:],X[-1,:]])

      if self.k >2:
        modes = np.append(modes,
                        [rd.choices(X,k=self.k-2)],
                        axis=0)
      
      item_shifted = True
      c_objects = None

      while item_shifted and max_iter>0:
        c_objects, item_shifted = self._assign_objects(X,modes,
                                                       c_objects)
        modes = self._update_modes(c_objects,X.shape[1])
        
        max_iter -= 1
      return c_objects, modes

    def _assign_objects(self,X,modes,
                        c_objects=None):
      
      if not c_objects:
        c_objects, condition = self._first_allocation(X,modes)
      else:
        clusters = range(self.k)
        condition = False
        
        clusters_members = c_objects.items()
        for cluster, items in clusters_members:

          for index,item in enumerate(items):
            distances = [HammingDistance\
                      .evaluate(item,
                                modes[c]) for c in clusters]
            n_cluster = np.argmax(distances)
            
            if not condition:
              condition = n_cluster != cluster
              print(f"Condition state changed to : {condition}")
            
            if (n_cluster != cluster):
              print(f"Item : {index}, Clusters are different: old : {cluster}, new: {n_cluster}")
              # remove from old cluster

              '''Issue while using del to delete the item, they are not being deleted '''
              
              c_objects[cluster][index] = c_objects[cluster][-1]
              c_objects[cluster][-1] = -1
              del c_objects[cluster][-1]
             
              # assign to new cluster
              c_objects[n_cluster].append(item)
      return c_objects, condition
    
    def _first_allocation(self,X,modes):
      c_objects = {cluster: [] for cluster in range(self.k)}
      condition = True
      
      for item in X:     
        i_cluster = -1
        min_dist = inf       
        for cluster in range(self.k):
          distance = HammingDistance\
                    .evaluate(item,
                              modes[cluster])

          if distance < min_dist:
            min_dist = distance
            i_cluster = cluster
        c_objects[i_cluster].append(item)
      return c_objects, condition

    def _update_modes(self,
                      c_objects,
                      nb_features):
      
      # Make sure that the modes are numpy array
      new_modes = np.empty(shape=(self.k,
                                  nb_features))

      for cluster in c_objects.keys():
        items = np.array(c_objects[cluster])
        #print(f"items.shape={items.shape}")
        nb_items = items.shape[0]

        
        
        # validate that the number of categories is at least 2
        categories = np.unique(items)

        if len(categories <2):
          categories = range(2)

        frequencies = [np.count_nonzero(items == category,axis=0)/nb_items\
                        for category in categories]
        
        n_mode = np.argmax(frequencies,
                                axis=0)
        
        assert len(n_mode) == 73, f"Expected mode to be list of features : {len(n_mode)}"
        new_modes = np.append(new_modes,[n_mode],axis=0)
      
      return new_modes
