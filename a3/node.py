class intermediate:
    def __init__(self,parent,children,keys,order):
        self.parent = parent
        self.children = children
        self.keys = keys
        self.order = order
        
    def check_overflow(self):
        return len(self.keys) > self.order
    
    def find(self,data):
        if data in self.keys: return True
        else:
            child = self.children[self.index_for_insertion(data)]
            return child.find(data)
    
    def count(self,data):
        child = self.children[self.index_for_insertion(data)]
        return child.count(data)
    
    def range(self,start,end):
        child = self.children[self.index_for_insertion(start)]
        return child.range(start,end)   
    
    def index_for_insertion(self,data):
        i=0
        for key in self.keys:
            if(key > data): break
            i+=1
        return i
    
    def insert_intermediate(self,data):
        node = self
        while(isinstance(node,intermediate)):
            node = node.children[node.index_for_insertion(data)]
        
        node.insert(data)
        node = self
        while(node.parent != None): node = node.parent
        node.set_correct_parent(node)
        return node
    
    def set_correct_parent(self,parent):
        for child in parent.children:
            child.parent = parent
            if(isinstance(child,intermediate)): child.set_correct_parent(child)
    
       
    def insert(self,data,child=[]):
        if(child == []): return self.insert_intermediate(data)
        index = self.index_for_insertion(data)
        self.keys.insert(index,data)
        self.children.insert(index+1,child)
        
        if self.check_overflow():
            left_data = self.keys[:len(self.keys)//2]
            right_data = self.keys[1+(len(self.keys)//2):]
            left_children = self.children[:(len(self.keys)//2)+1]
            right_children = self.children[1+(len(self.keys)//2):]
            
            parent_key = self.keys[len(self.keys)//2]
            right = intermediate(self.parent,right_children, right_data,self.order)
            self.keys = left_data
            self.children = left_children

            if self.parent == None:
                parent = intermediate(None,[self,right],[parent_key],self.order)
            else:
                parent = self.parent.insert(parent_key,right)
            self.parent = parent
            right.parent = parent
            return self.parent
        else: return self


# Leaf node
class leaf:
    def __init__(self,next_leaf,parent,keys,order):
        self.order = order
        self.next_leaf = next_leaf
        self.parent = parent
        self.keys = keys
    
    def range(self,start,end):
        keys_in_range = 0
        till_end = True
        for key in self.keys:
            if (key.data) >= start and key.data <= end:
                keys_in_range+= key.count
                till_end = True
            else: till_end = False
        
        if(self.next_leaf!= None and till_end and self.next_leaf.keys[0].data >= start and self.next_leaf.keys[0].data <= end ):
            keys_in_range += self.next_leaf.range(start,end)
        return keys_in_range
    def find(self,data):
        if(self.find_key(data) != None): return True
        else: return False
    def count(self,data):
        key = self.find_key(data)
        if(key != None): return key.count
        else: return 0
    
    def check_overflow(self):
        return len(self.keys) > self.order
    
    def find_key(self,data):
        for key in self.keys:
            if (key.data == data): return key
        return None
    def index_for_insertion(self,data):
        i=0
        for key in self.keys:
            if(key.data > data): break
            i+=1
        return i
    def insert(self,data):
        key = self.find_key(data)
        if (key != None): key.count +=1
        else: self.keys.insert(self.index_for_insertion(data),node_data(data))

        
        if self.check_overflow():
            left_data = self.keys[:len(self.keys)//2]
            right_data = self.keys[len(self.keys)//2:]
            parent_key = self.keys[len(self.keys)//2].data
            right = leaf(self.next_leaf,self.parent,right_data,self.order)
            self.keys = left_data
            self.next_leaf = right

            if self.parent == None:
                parent = intermediate(None,[self,right],[parent_key],self.order)
                
            else:
                parent = self.parent.insert(parent_key,right)
            
            self.parent = parent
            right.parent = parent
            return self.parent
        else: return self


# leaf node data
class node_data:
    def __init__(self,data):
        self.data = data
        self.count = 1
