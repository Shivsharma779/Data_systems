import sys
from bplustree import bplustree

if __name__ == "__main__":
    try:
        if(len(sys.argv) < 3):
            print("Invalid no of argumnets\nUsage: python3 2020201037.py ./inputfile ./outputfile")
            exit(1)
        inp = sys.argv[1]
        out = sys.argv[2]
        bpt = bplustree(3)
        with open(inp,"r") as input_file , open(out,"w") as output_file:
            queries = input_file.readlines()
            for query in queries:
                output = ""
                query = query.upper().strip().split()
                if (query[0] == "INSERT"):
                    bpt.insert(int(query[1]))
                    output = "INSERTED " + query[1] 
                elif (query[0] == "FIND"):
                    output = str(bpt.find(int(query[1]))) + " " + query[1]

                elif (query[0] == "COUNT"):
                    output = str(bpt.count(int(query[1]))) + " is count for " + query[1]
                elif (query[0] == "RANGE"):
                  
                    output = str(bpt.range(int(query[1]),int(query[2]))) + " keys present in range; " + query[1] + ", " + query[2] 
                
                else: output = "Wrong query"
                output_file.write(output+"\n")
                
        
        print("done")
    except Exception as E:
        print("Error",E)

