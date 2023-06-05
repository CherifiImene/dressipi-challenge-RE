from math import inf
import random as rd
import numpy as np
from metrics.hamming_distance import HammingDistance


class LOF:
    def __init__(self) -> None:
        pass

class K_MODES:
    
    def __init__(self,k=2) -> None:
        # verify k>=2
        self.k = k
    

    def fit(self,X):
      
      # Choose the latest(0) and oldest items(-1)
      # as starting modes
      modes = [X[0],X[-1]]

      if self.k >2:
        modes.extend(rd.choices(X,k=self.k-2))
      
      ishifted = True
      c_objects = None
      while item_shifted:
        c_objects, item_shifted = self._assign_objects(X,modes,
                                                       c_objects)
        modes = self._update_modes(c_objects)
        X = list(c_objects.values())

    def _assign_objects(self,X,modes,
                        c_objects=None):
      
      if not c_objects:
        c_objects, condition = self._first_allocation(X,modes)
      else:
        clusters = range(self.k)
        condition = False
        
        clusters_members = c_objects.items()
        for cluster, items in clusters_members:

          for item in items:
            distances = [HammingDistance\
                      .evaluate(item,
                                modes[c]) for c in clusters]
            n_cluster = np.argmax(distances)
            
            if not condition:
              condition = n_cluster != cluster
            
            if condition:
              # remove from old cluster
              c_objects[cluster].remove(item)
              
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

    def _update_modes(self,c_objects):
      new_modes = []
      for cluster in c_objects.keys():
        items = np.array(c_objects[cluster])
        nb_items,nb_features = items.shape

        # compute the new mode 
        n_mode = []
        for feature in range(nb_features):
          categories = np.unique(items[:,feature])
          frequencies = [np.count(items == category)/nb_items\
                         for category in categories]
          
          n_mode.append(np.argmax(frequencies))
        new_modes.append(n_mode)
      
      return new_modes
          
      
        


      




class K_MEDOID:
    
    def __init__(self) -> None:
        pass

