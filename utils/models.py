from math import inf
import random as rd
import numpy as np
from metrics.hamming_distance import HammingDistance

class K_MODES:

    def __init__(self,k=2) -> None:
      # verify k>=2
      self.k = k


    def fit(self,X,max_iter=50):

      # Choose the latest(0) and oldest items(-1)
      # as starting modes
      # if the number of elements in a cluster is 1
      # verify wether the cluster can be concatenated with another cluster
      modes = self._huang_init(X)

      item_shifted = True
      c_objects = None

      iters = max_iter
      while item_shifted and iters>0:
        print(f"Iteration: {max_iter-iters}")
        c_objects, item_shifted = self._assign_objects(X,modes,
                                                       c_objects)
        modes = self._update_modes(c_objects,X.shape[1])

        iters -= 1
      return c_objects, modes

    def _huang_init(self,X):
      categories = np.unique(X)
      frequencies = np.array([np.count_nonzero(X == category,axis=0)\
                        for category in categories])

      Qs = []

      # initialize the modes
      for _ in range(self.k):
        if not Qs:
          Q = np.array([(np.argmax(frequencies[:,feature])-feature)%len(categories)\
                for feature in range(frequencies.shape[1])])
        else:
          Q = Qs[-1]*-1 + 1

        Qs.append(Q)
      # Replace the initial Q
      # by their most similar objects in X
      modes = []
      for Q in Qs:
        ipoint = np.argmin([
            HammingDistance.evaluate(item,Q)
            for item in X
        ])
        modes.append(X[ipoint])
      return modes



    def _assign_objects(self,X,modes,
                        c_objects=None):

      if not c_objects:
        c_objects = self._first_allocation(X,modes)
        condition = True
      else:
        clusters = range(self.k)
        condition = False

        clusters_members = c_objects.items()
        for cluster, items in clusters_members:

          for index,item in enumerate(items):
            distances = [HammingDistance\
                      .evaluate(item,
                                modes[c]) for c in clusters]
            n_cluster = np.argmin(distances)

            if not condition:
              condition = n_cluster != cluster

            if (n_cluster != cluster):
              print(f"Item : {index}, Clusters are different: old : {cluster}, new: {n_cluster}")
              # remove from old cluster

              '''Issue while using del to delete the item,
              they are not being deleted when
              the items are in the middle of the array'''

              c_objects[cluster][index] = c_objects[cluster][-1]
              c_objects[cluster][-1] = -1
              del c_objects[cluster][-1]

              # assign to new cluster
              c_objects[n_cluster].append(item)
      return c_objects, condition

    def _first_allocation(self,X,modes):
      c_objects = {cluster: [] for cluster in range(self.k)}

      for item in X:
        distances = [HammingDistance\
                    .evaluate(item,
                              modes[cluster])
                    for cluster in range(self.k)]

        i_cluster = np.argmin(distances)
        c_objects[i_cluster].append(item)
      return c_objects

    def _update_modes(self,
                      c_objects,
                      nb_features):

      # Make sure that the modes are numpy array
      new_modes = []

      for cluster in c_objects.keys():
        items = np.array(c_objects[cluster])

        if not np.any(items):
          # The cluster is empty
          # skip
          new_modes.append(np.zeros(shape=nb_features))
          continue

        print(f"cluster: {cluster}, items: {len(items)}")
        # validate that the number of categories is at least 2
        categories = np.unique(items)

        if len(categories <2):
          categories = range(2)

        frequencies = [np.count_nonzero(items == category,axis=0)\
                        for category in categories]

        if not np.any(frequencies):
          raise Exception(f"Empty frequencies : {frequencies}, x: {items}")
        n_mode = np.argmax(frequencies,
                                axis=0)

        assert len(n_mode) == 73, f"Expected mode to be list of features : {len(n_mode)}"
        new_modes.append(n_mode)


      return np.array(new_modes)
