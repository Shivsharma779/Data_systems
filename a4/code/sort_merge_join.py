#!/bin/python3
import subprocess
import sys
import heapq
import time
"""
steps:
1. Create sorted S and R




"""
temp_file_format = "./.sublist"
class row(object):
    
    order=""
    def __init__(self,data,chunk_no,file_pointer,order_col):
        self.data = data
        self.chunk_no = chunk_no
        self.file_pointer = file_pointer
        self.order_columns=[order_col]
    def get_row(self):
        return self.data
    def get_file_pointer(self):
        return self.file_pointer
    def get_chunk_no(self):
        return self.chunk_no
    def __lt__(self,other):
        if self.order == "desc":
            for col in self.order_columns:
                if (self.data[col] < other.data[col]): return False
                elif (self.data[col] > other.data[col]): return True
            return True
        else:
            for col in self.order_columns:
                if (self.data[col] < other.data[col]): return True
                elif (self.data[col] > other.data[col]): return False
            return True


def get_data(input_file,no_of_rows):
    # global no_of_delimiter
    table=[]
    
    
    current_row = 0
    while no_of_rows != current_row :
        line = input_file.readline()
        if(line == ""): break
        
        table_row = line.split()
        table.append(table_row)
        current_row+=1
         
    return table



def write_to_output(file_pointer,r_data,s_data):
    for s in s_data:
        file_pointer.write(r_data[0]+" "+r_data[1]+" "+s[1]+"\n")
        # print(r_data[0]+" "+r_data[1]+" "+s[1]+"\n")

def merge_files(nr,ns,output_file):
    print("Merging:")
    
    pqr = []
    pqs = []
    # row_no=[0]*no_of_chunks
    # row.order_columns=[order-1]
    row.order="ASC"
    for chunk_no in range(nr):
        filename = temp_file_format+"R"+str(chunk_no)+".txt"
        tempfile = open(filename,"r")
        row_data = get_data(tempfile,1)[0]
        pqr.append(row(row_data,chunk_no,tempfile,1))
    heapq.heapify(pqr)

    for chunk_no in range(ns):
        filename = temp_file_format+"S"+str(chunk_no)+".txt"
        tempfile = open(filename,"r")
        row_data = get_data(tempfile,1)[0]
        pqs.append(row(row_data,chunk_no,tempfile,0))
    heapq.heapify(pqs)
    
    f=open(output_file,"w")
    
    
    chunks_processed_r = 0
    chunks_processed_s = 0
    find_next_r = True
    find_next_s = True
    buffer = [] 
    while chunks_processed_r < nr and chunks_processed_s< ns:
        if(find_next_r):
            elementr = heapq.heappop(pqr)
            data_row_r = elementr.get_row()
            chunk_no_r = elementr.get_chunk_no()
            file_pointer_r = elementr.get_file_pointer()
        if(find_next_s):
            elements = heapq.heappop(pqs)
            data_row_s = elements.get_row()
            chunk_no_s = elements.get_chunk_no()
            file_pointer_s = elements.get_file_pointer()
        
        
        if(data_row_r[1] == data_row_s[0]):
            while(chunks_processed_s < ns and data_row_r[1] == data_row_s[0]):
                buffer += [data_row_s]
                
                next_data_s = get_data(file_pointer_s,1)
                if next_data_s == []: 
                    chunks_processed_s+=1
                    file_pointer_s.close()
                    print("No of sublist processed_s",chunks_processed_s)
                else:
                    heapq.heappush(pqs,row(next_data_s[0],chunk_no_s,file_pointer_s,0))
                if(pqs != []):
                    elements = heapq.heappop(pqs)
                    data_row_s = elements.get_row()
                    chunk_no_s = elements.get_chunk_no()
                    file_pointer_s = elements.get_file_pointer()

            while(chunks_processed_r < nr and data_row_r[1] == buffer[0][0] ):
                write_to_output(f,data_row_r,buffer)
                next_data_r = get_data(file_pointer_r,1)
                if next_data_r == []: 
                    chunks_processed_r+=1
                    file_pointer_r.close()
                    print("No of sublist processed_r",chunks_processed_r)
                else:
                    heapq.heappush(pqr,row(next_data_r[0],chunk_no_r,file_pointer_r,1))
                if(pqr != []):
                    elementr = heapq.heappop(pqr)
                    data_row_r = elementr.get_row()
                    chunk_no_r = elementr.get_chunk_no()
                    file_pointer_r = elementr.get_file_pointer()
            buffer = []
            find_next_r = False
            find_next_s = False
            if(chunks_processed_r == nr or chunks_processed_s ==ns):
                break
        elif (data_row_r[1] > data_row_s[0]):
            next_data_s = get_data(file_pointer_s,1)
            if next_data_s == []: 
                chunks_processed_s+=1
                file_pointer_s.close()
                # print("No of sublist processed_s",chunks_processed_s)
            else:
                heapq.heappush(pqs,row(next_data_s[0],chunk_no_s,file_pointer_s,0))
            find_next_r = False
            find_next_s = True
        else:
            next_data_r = get_data(file_pointer_r,1)
            if next_data_r == []: 
                chunks_processed_r+=1
                file_pointer_r.close()
                # print("No of sublist processed_r",chunks_processed_r)
            else:
                heapq.heappush(pqr,row(next_data_r[0],chunk_no_r,file_pointer_r,1))
            find_next_r = True
            find_next_s = False
        

    f.close()




if __name__ == "__main__":
    inputR = sys.argv[1]
    # inputR = "inputR_"
    inputS = sys.argv[2]
    print(inputR,inputS)
    output_file ="./"+ inputR.split("/")[-1] + "_" + inputS.split("/")[-1]+"_join.txt"
    # output_file = "output"
    # M = 1# in terms of records
    M = int(sys.argv[3]) # in terms of blocks
    
    nr_in_blocks = 100
    nr_in_mem_R  = nr_in_mem_S = int((M*nr_in_blocks)/2)
    start = time.time()
    command = "code/create_sorted_input "+ inputR + " sortedR " + str(nr_in_mem_R)+ " 2 R" 
    nr = int(subprocess.getstatusoutput(command)[1])
    command = "code/create_sorted_input "+ inputS + " sortedS " + str(nr_in_mem_R)+ " 1 S"
    ns = int(subprocess.getstatusoutput(command)[1])
    if(nr+ns >= 2*M):
        print("Memory limit exceeded")
        exit(0)
    merge_files(nr,ns,output_file)    
    end = time.time()
    total_time_taken = end - start
    with open("observations_sort.txt","a") as ob:
        ob.write("\n"+str(M)+" "+str(total_time_taken))
    # create_sublist("sortedS",nr_in_mem_S)  
    # join()  

