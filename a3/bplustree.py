from node import leaf
class bplustree:
    def __init__(self,order):
        self.order = order
        self.root = leaf(None,None,[], order)

    def insert(self,data):
        self.root = self.root.insert(data)

    def range(self,start,end):
        return self.root.range(start,end)
    
    def find(self,data):
        if( self.root.find(data)): return "YES"
        else: return "NO"

    def count(self,data):
        return self.root.count(data)