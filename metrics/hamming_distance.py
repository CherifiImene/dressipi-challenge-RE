class Node:
    def __init__(self,value,**kwargs) -> None:
        self.value = value
        self.occurence = 1
        self.l_child = kwargs.get('l_child',None)
        self.r_child = kwargs.get("r_child",None)
        self.parent = kwargs.get("parent",None)
        
    def __getattr__(self, name: str):
        return self.__dict__[f"_{name}"]

    def __setattr__(self, name, child):
        if name != "l_child" and name != "r_child":
            self.__dict__[f"_{name}"] = child
            return
            
        if isinstance(child,Node)or not child:
            if name == "l_child":
                if not child or child.value < self.value:
                    self.__dict__[f"_{name}"] = child
                    if child :
                        child.parent = self
                else:
                    print(f"Left child must have a value \
                            lower than the current \
                            node value: {self.value}")
            
            
            if name == "r_child":
                if not child or child.value > self.value:
                    self.__dict__[f"_{name}"] = child
                    if child :
                        child.parent = self
                else:
                    print(f"Right child must have a value \
                            higher than the current \
                            node value: {self.value}")
            
            
        else:
            print(f"{name} must be of class Node")
class BinaryTree:
    
    def __init__(self,value) -> None:
        
        self.root = Node(value)
    
    def add_child(self,value):
        node = self.binary_search(value)
        if value > node.value:
            node.r_child = Node(value)
        
        if value < node.value:
            node.l_child = Node(value)
        
        # case of duplicates
        if value == node.value:
            node.occurence += 1
        
    def remove_child(self,value):
        node = self.binary_search(value)
        
        # case the value doesn't exist
        if not node:
            msg = f"Node: {value}, not found in this tree"
            return self.root, msg
        # case it's a leaf node
        if not node.l_child and not node.r_child:
            msg = f"Node: {node.value} Deleted successfully"
            parent = node.parent
            
            if node == parent.l_child:
                parent.l_child = None
            
            if node == parent.r_child:
                parent.r_child = None
                
            return self.root, msg
        # case it has two children
        if node.l_child and node.r_child:
            msg = f"Node: {node.value} Deleted successfully"
            
            # get inorder successor
            inorder_succ = self.inorder_successor(node)
            
            if not inorder_succ.r_child:
                # in case it has no right subtree
                # replace the value of current node 
                # with the value of the inorder successor
                # and delete the inorder successor
                node.value = inorder_succ.value
                if inorder_succ.parent != node:
                    inorder_succ.parent.l_child = None
                else:
                    inorder_succ.parent.r_child = None
                
                del inorder_succ
            else:
                # in case it has a right subtree
                # make the right child of the inorder successor
                # as the left child of the parent of the inorder successor 
                # and replace the value of the current node
                # with the value of the inorder successor
                # and delete the inorder successor
                inorder_succ.parent.l_child = inorder_succ.r_child
                node.value = inorder_succ.value
                del inorder_succ
            return self.root, msg
        
        # case it has one child
        if node.l_child or node.r_child:
            if node.l_child:
                node.value = node.l_child.value
                node.l_child = None
            
            if node.r_child:
                node.value = node.r_child.value
                node.r_child = None
    
    def inorder(self,root):
        if root != None :
            self.inorder(root.l_child)
            
            # case of duplicates
            for _ in range(root.occurence):
              print(root.value)

            self.inorder(root.r_child)
    def k_inorder(self,root,k,nodes):
        
        if root != None :
            self.k_inorder(root.l_child,k,nodes)
            if len(nodes) == k:
              return
            # case of duplicates
            for _ in range(root.occurence):
              nodes.append(root.value)
              if len(nodes) == k:
                return
            
            self.k_inorder(root.r_child,k ,nodes)
            if len(nodes) == k:
              return
    
    def inorder_successor(self,node):
        # if there's a right child
        if node.r_child :
            # search for the minimum value 
            # in the right subtree
            successor = node.r_child
            
            while successor.l_child:
                successor = successor.l_child
            return successor
                
        else :
            # search for the successor
            # in the parents
            child = node
            parent = node.parent
            
            while parent:
                if child != parent.r_child:
                    break
                child = parent
                parent = parent.parent
            return parent
            
            
    def binary_search(self,value):
        current_node = self.root
        prev_node = None
        while current_node != None :
            
            if current_node.value == value:
                
                return current_node
            
            elif current_node.value > value:
                prev_node = current_node
                current_node = current_node.l_child
            
            elif current_node.value < value:
                prev_node = current_node
                current_node = current_node.r_child
        return prev_node



import numpy as np

class HammingDistance:
  def __init__(self):
    self.results = None
  
  def evaluate(vector1,vector2):
    #vectorized evaluation
    vec_evaluation =np.vectorize(HammingDistance.evaluate_,
                                    otypes=[int],
                                    signature='(m),(m)->()')
    return vec_evaluation(vector1,vector2)

  # TODO write a test for this function
  def evaluate_(vector1,vector2):
    if not isinstance(vector1,np.ndarray):
      vector1 = np.array(vector1)

    if not isinstance(vector2,np.ndarray):
      vector2 = np.array(vector2)
    # both variables are binary vectors
    # the xor between them will
    # return only the different values
    assert vector1.shape == vector2.shape , f"The two vectors are of diffrent shapes: {vector1.shape}, {vector2.shape}"
    try:
      diff = (vector1!=vector2).sum()
    except AttributeError as e:
      print(f"vector1 : {vector1}, type: {type(vector1)}")
      print(f"vector2 : {vector2}, type: {type(vector2)}")
    return diff
  
  def add_result(self,result):
    '''
    implementation of an insert
    in a binary search array
    '''
    if self.results == None:
      self.results = BinaryTree(result)
    else:
      self.results.add_child(result)
    
  def get_best_k(self,k):
    first_k_results = []
    self.results\
        .k_inorder(self.results.root,k,first_k_results)
    return first_k_results
