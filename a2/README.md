### Readme:
- The zip containes two files for execution of program with threading and without threading
- Both the programs are built with python3
- The name of program without sorting is `sort` and with sorting `sort with thread`
- The binary for python3 is set as `/bin/python3` in the first line of the programs if not there please change the path
- Example commands:
    1. Without threads`./sort input.txt output1.txt 100M desc C1`
    2. With threads: `./sort_with_thread 100M.txt output.txt 100M 10 desc C2`
- The input are taken in Bytes by default to provide in KB, MB, GB use M,K,G respectively ex: `100K,100M,100G`
- The observations of the program ran are stored in `observation_mem.txt`
- The intermediate files are prefixed with a `.` so to view them in a file explorer turn on view hidden files
- A sample metadata file for gensort output is also present