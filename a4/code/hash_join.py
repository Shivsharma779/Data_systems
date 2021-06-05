#!/bin/python3
import sys
import time
temp_file_format = "./.sublist"

def find_hash(val,mod,prime):
    calc_val , i = 0, 0
    len_val = len(val)
    while i < len_val:
        calc_val =(calc_val +(ord(val[i])*pow(prime,i+1)%mod)%mod)%mod
        i+=1
    return calc_val%mod


def open_files(inputR,inputS,M):
    sublist_map = {}
    size_sublist_r = [0 for i in range(M)]
    size_sublist_s = [0 for i in range(M)]

    for i in range(M):
        with open(temp_file_format+"R"+str(i)+".txt","w") as f:
            pass
        with open(temp_file_format+"S"+str(i)+".txt","w") as f:
            pass

        

    with open(inputR) as f1: 
        while True:
            l1 = f1.readline()
            l1 = l1.strip("\n").strip()
            if not l1:
                break
            if l1:
                data = l1.split(" ")
                calc_hash = find_hash(data[1],M,19)
                if calc_hash in sublist_map:
                    sublist_map[calc_hash].append(l1+"\n")
                else:
                    sublist_map[calc_hash] = [l1+"\n"]

                if len(sublist_map[calc_hash])==100:
                    with open(temp_file_format+"R"+str(calc_hash)+".txt","a") as f:
                        f.writelines(sublist_map[calc_hash])
                        sublist_map[calc_hash].clear()
                        size_sublist_r[calc_hash]+=100

        for calc_hash in sublist_map:
            with open(temp_file_format+"R"+str(calc_hash)+".txt","a") as f:
                        f.writelines(sublist_map[calc_hash])
                        size_sublist_r[calc_hash]+=len(sublist_map[calc_hash])
                        sublist_map[calc_hash].clear()            

        # print(sublist_map)


    with open(inputS) as f1: 
        while True:
            l1 = f1.readline()
            l1 = l1.strip("\n").strip()
            if not l1:
                break
            if l1:
                data = l1.split(" ")
                calc_hash = find_hash(data[0],M,19)
                if calc_hash in sublist_map:
                    sublist_map[calc_hash].append(l1+"\n")
                else:
                    sublist_map[calc_hash] = [l1+"\n"]

                if len(sublist_map[calc_hash])==100:
                    with open(temp_file_format+"S"+str(calc_hash)+".txt","a") as f:
                        f.writelines(sublist_map[calc_hash])
                        size_sublist_s[calc_hash]+=100
                        sublist_map[calc_hash].clear()

        for calc_hash in sublist_map:
            with open(temp_file_format+"S"+str(calc_hash)+".txt","a") as f:
                        f.writelines(sublist_map[calc_hash])
                        size_sublist_s[calc_hash]+=len(sublist_map[calc_hash])
                        sublist_map[calc_hash].clear()         

        return(size_sublist_r,size_sublist_s)
            

def get_next(size_sublist_r, size_sublist_s,outfile,M):
    out = open(outfile,"w")
    for i in range(M):
        if(min(size_sublist_r[i],size_sublist_s[i])>M*100):
            exit(0)

        if(size_sublist_r[i]<size_sublist_s[i]):
            with open(temp_file_format+"R"+str(i)+".txt") as f:
                rfile = f.readlines()
                rfile = [line.strip("\n").strip(" ").split(" ") for line in rfile]
                # print(rfile)
            with open(temp_file_format+"S"+str(i)+".txt") as f:
                for line in f:
                    line = line.strip("\n").strip()
                    if not line:
                        break
                    line = line.split(" ")
                    for rlines in rfile:
                        if(line[0]==rlines[1]):
                            out.write(rlines[0]+" "+rlines[1]+" "+line[1]+"\n")
        else:
            with open(temp_file_format+"S"+str(i)+".txt") as f:
                sfile = f.readlines()
                sfile = [line.strip("\n").strip(" ").split(" ") for line in sfile]
                # print(rfile)
            with open(temp_file_format+"R"+str(i)+".txt") as f:
                for line in f:
                    line = line.strip("\n").strip()
                    if not line:
                        break
                    line = line.split(" ")
                    for slines in sfile:
                        if(line[1]==slines[0]):
                            out.write(line[0]+" "+slines[0]+" "+slines[1]+"\n")
            



    

if __name__ == "__main__":
    inputR = sys.argv[1]
    # inputR = "inputR_"
    inputS = sys.argv[2]
    # inputS = "inputS_"
    output_file = "./"+ inputR.split("/")[-1] + "_" + inputS.split("/")[-1]+"_join.txt"
    # output_file = "output"
    M = int(sys.argv[3]) # in terms of blocks
    # M = 1# in terms of records
    # print(inputR,inputS,M)
    start = time.time()
    size_sublist_r, size_sublist_s = open_files(inputR,inputS,M)
    # print(size_sublist_r, size_sublist_s)
    get_next(size_sublist_r, size_sublist_s,output_file,M)
    # get_next(inputR,inputS,M)
    
    end = time.time()
    total_time_taken = end - start
    with open("observations_hash.txt","a") as ob:
        ob.write("\n"+str(M)+" "+str(total_time_taken))