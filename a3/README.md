## README
- The assignment is built using `python3`
- Run program using this command:
  
  `python3  .2020201037.py ./input.txt ./output.txt`
- Sample queries
```
INSERT 42 
FIND 42
COUNT 4 
RANGE 8 42
```
- Sample output for queries
```
INSERTED 42
YES 42
2 is count for 4
5 keys are present in range 8, 42
```

## Implementation
- The program contains has bplustree class to encapsulate the tree structure and functions
- Two classes are built for intermediate nodes and leaf nodes
- The node_data class contains the actual data and the count for the data
- The leaf_nodes are are connected to each other in a linked list form
- The order 3 is assumed that the node can contain 3 keys in a node
- The insertion is implemented by moving up the node at floor(length/2) index