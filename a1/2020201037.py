import sqlparse
import csv 
import re
import sys
agg_func=["max","min","count","avg","sum"]
def output(final_table, column_list,group_by,distinct):
    global agg_func
    display_column = []
    result=[]
    if "*" in column_list: column_list = cartesian_join_header
            
    
    
    row = []
    agg= False
    for column in column_list:
        for func in agg_func:
            if func in column:
                agg = True

    for column in column_list:
        if agg:
            funct = False
            for func in agg_func:
                if func in column:
                    funct = True
                    if column.split('(')[1].split(')')[0].strip() != "*":
                        col_index = cartesian_join_header.index(column.split('(')[1].split(')')[0].strip())
                    else:
                        col_index = 0
                    
                    result_row = call_func(func,col_index,final_table)
                    row.append(str(result_row))
            if not funct:
                agg = True
                row.append(final_table[0][cartesian_join_header.index(column)])
        
    
    if(agg): 
        result.append(row)
        return result
    
    for column in column_list:
        display_column.append(cartesian_join_header.index(column))
            
    
    
    
    if not group_by:
        for rows in final_table:
            row=[]
            for col in display_column:
                row.append(rows[col])
            if distinct:
                if row not in result:
                    result.append(row)
            else:   result.append(row)
    return result
def call_func(func,col,result):
    if func == "max": return max(col,result)
    elif func == "min": return min(col,result)
    elif func == "count": return count(col,result)
    elif func == "sum": return sum(col,result)
    elif func == "avg": return avg(col,result)
def max(A,result):
    maxi=-99999799
    for i in result:
        if(i[A]>maxi):
            maxi=i[A]
    return maxi

def sum(A,result):
    sumi=0
    for i in result:
        sumi+=i[A]
   
    return sumi

def avg(A,result):
    return float(sum(A,result))/len(result)

def min(A,result):
    mini=999999
    for i in result:
        if(i[A]<mini):
            mini=i[A]
    return mini

def count(A,result):
    return len(result)


   

def getdata(tablename):
    i = 0
    database = []
    with open(tablename+'.csv', mode ='r')as file:
        tablefile = csv.reader(file)
        for lines in tablefile:
            row=[]
            for data in lines:
                row.append(int(data.strip("'").strip("\"")))
                
            database.append(row)
    
    return database


def add_to_database(database_schema,tablename,database):
    if tablename in database_schema:
        if tablename in database:
            return 0
        else:
            database[tablename] = getdata(tablename)
            return 0
    else:
        print("No such file in database")
        return -1
    
def check_disticnct(query_list):
    if(query_list[1]== "distinct"):
        return True
    else:
        return False;
try:
    raw = sys.argv[1]
    # print(raw)
    raw=raw.lower()
    if(raw[len(raw)-1] != ';'): 
        print("No ;")
        sys.exit(0)
    else: raw=raw[:len(raw)-1]

    f=open('metadata.txt','r')
    database_schema= {}
    lines=f.readlines()
    i=0
    tablename= ""
    while(i<len(lines)):
        if('<begin_table>' in lines[i]):
            i=i+1
            tablename = lines[i].strip()
            database_schema[tablename] = []
        elif('<end_table>' in lines[i]):
            i=i;
        else:
            database_schema[tablename].append(lines[i].strip().lower())

        i=i+1

    database={}
    for table_name in database_schema:
        add_to_database(database_schema,table_name,database)

    query = sqlparse.parse(raw)[0].tokens
    query_list = []
    for i in query:
        if i.value == ' ':
            continue
        else:
            query_list.append(str(i))

    column_list = []
    table_list = []
    if check_disticnct(query_list):
        column_list= query_list[2].split(',')
        table_list = query_list[4].split(',')
    else:
        column_list= query_list[1].split(',')
        table_list = query_list[3].split(',')

    where_list = []
    if "where" in raw:
        if check_disticnct(query_list):
            if "and" in  query_list[5]:
                where_list = [query_list[5][5:].split("and")[0],"and",query_list[5].split("and")[1]]
            elif "or" in query_list[5]:
                where_list = [query_list[5][5:].split("or")[0],"or",query_list[5].split("or")[1]]
            else:
                where_list = [query_list[5][5:]]
        else:
            if "and" in  query_list[4]:
                where_list = [query_list[4][5:].split("and")[0],"and",query_list[4].split("and")[1]]
            elif "or" in query_list[4]:
                where_list = [query_list[4][5:].split("or")[0],"or",query_list[4].split("or")[1]]
            else:
                where_list = [query_list[4][5:]]

    for i  in range(len(table_list)):
        table_list[i] = table_list[i].strip()
    for i  in range(len(column_list)):
        column_list[i] = column_list[i].strip()
    for i  in range(len(where_list)):
        where_list[i] = where_list[i].strip()

    cartesian_join = database[table_list[0]]
    cartesian_join_header = []
    for table in table_list:
        if(table != table_list[0]): cartesian_join = [[*x,*y] for x  in cartesian_join for y in database[table]]
        cartesian_join_header.extend(database_schema[table])
    #     print(cartesian_join_header,table)



    if len(where_list)>0:
        operators=[">=","<=","=",">","<"]
        result=[]
        log_op=""
        condition1=where_list[0]
        condition2=""
        op1=""
        op2=""
        second_condition=False
        if(len(where_list)>1):
            second_condition = True
            condition2=where_list[2]
            log_op=where_list[1]

        for op in operators:
            if op in condition1:
                op1=op
                break;
        for op in operators:
            if second_condition and op in condition2:
                op2=op 
                break;

        for rows in cartesian_join:

            column=condition1.split(op1)[0].strip()
            operand = condition1.split(op1)[1].strip()
            if op1 == "=": op1f="=="
            else: op1f=op1

            if re.match('^[0-9]*$', operand):
                exp=str(rows[cartesian_join_header.index(column)])+op1f+operand
            else:
                exp=str(rows[cartesian_join_header.index(column)])+op1f+str(rows[cartesian_join_header.index(operand)])
            if second_condition:
                column=condition2.split(op2)[0].strip()
                operand = condition2.split(op2)[1].strip()
        #         print(column,operand,op2)
                if op2 == "=": op2f="=="
                else: op2f=op2

                if re.match('^[0-9]*$', operand):
                    exp=exp+ " " +log_op+" "+ str(rows[cartesian_join_header.index(column)])+op2f+operand
                else:
                    exp=exp+ " " +log_op+" "+str(rows[cartesian_join_header.index(column)])+op2f+str(rows[cartesian_join_header.index(operand)])
            if(eval(exp)):

                result.append(rows)
    else:
        result=cartesian_join


    group_by_col=""
    if "group by" in raw:
        if check_disticnct(query_list):
            if where_list ==[]:
                group_by_col = query_list[6].strip()
            else:
                group_by_col = query_list[7].strip()
        else:
            if where_list == []:
                group_by_col = query_list[5].strip()
            else:
                group_by_col =  query_list[6].strip()

    order_by_col=""
    order=""
    if "order by" in raw and group_by_col == "":
        if check_disticnct(query_list):
            if group_by_col != "":
                if where_list == []:
                    order_by_col= query_list[8].split()[0].strip()
                    if len(query_list[8].split()) > 1 :order= query_list[8].split()[1].strip()
                else:
                    order_by_col= query_list[9].split()[0].strip()
                    if len(query_list[9].split()) > 1 :order=query_list[9].split()[1].strip()
            else:
                if where_list == []:
                    order_by_col= query_list[6].split()[0].strip()
                    if len(query_list[6].split()) > 1 : order= query_list[6].split()[1].strip()
                else:
                    order_by_col= query_list[7].split()[0].strip()
                    if len(query_list[7].split()) > 1 :order=query_list[7].split()[1].strip()
        else: 
            if group_by_col != "": 
                if where_list == []:
                    order_by_col= query_list[7].split()[0].strip()
                    if len(query_list[7].split()) > 1 :order= query_list[7].split()[1].strip()
                else:
                    order_by_col= query_list[8].split()[0].strip()
                    if len(query_list[8].split()) > 1 :order=query_list[8].split()[1].strip()
            else:
                if where_list == []:
                    order_by_col= query_list[5].split()[0].strip()
                    if len(query_list[5].split()) > 1 :order=query_list[5].split()[1].strip()
                else:
                    order_by_col= query_list[6].split()[0].strip()
                    if len(query_list[6].split()) > 1 : order=query_list[6].split()[1].strip()

        
        if order == "desc":
                result = sorted(result, key=lambda x: x[cartesian_join_header.index(order_by_col)],reverse=True)
        elif order.lower() == "asc" or order == "":
                result = sorted(result, key=lambda x: x[cartesian_join_header.index(order_by_col)])

    groups = {}
    if group_by_col != "":
        for row in result:
            if row[cartesian_join_header.index(group_by_col)] in groups:
                groups[row[cartesian_join_header.index(group_by_col)]].extend([row])
            else:
                groups[row[cartesian_join_header.index(group_by_col)]] = [row]

    final_result = []
    group_in_column = False
    for group in groups:
        if group_by_col not in column_list:   
            final_result.extend(output(groups[group],[group_by_col]+column_list,True,check_disticnct(query_list)))
            
        else:
            group_in_column = True;
            final_result.extend(output(groups[group],column_list,True,check_disticnct(query_list)))

    if final_result == []:
        final_result.extend(output(result,column_list,False,check_disticnct(query_list)))

    if column_list[0] == "*":
        column_list = cartesian_join_header

    if group_in_column == False and group_by_col != "": 
        
        column_list = [group_by_col]+column_list

    order_by_col=""
    order=""

    if "order by" in raw and group_by_col != "":
        if check_disticnct(query_list):
            if group_by_col != "":
                
                if where_list == []:
                    order_by_col= query_list[8].split()[0].strip()
                    if len(query_list[8].split()) > 1 :order= query_list[8].split()[1].strip()
                else:
                    order_by_col= query_list[9].split()[0].strip()
                    if len(query_list[9].split()) > 1 :order=query_list[9].split()[1].strip()
            else:
                if where_list == []:
                    order_by_col= query_list[6].split()[0].strip()
                    if len(query_list[6].split()) > 1 : order= query_list[6].split()[1].strip()
                else:
                    order_by_col= query_list[7].split()[0].strip()
                    if len(query_list[7].split()) > 1 :order=query_list[7].split()[1].strip()
        else: 
            if group_by_col != "": 
                if where_list == []:
                    order_by_col= query_list[7].split()[0].strip()
                    if len(query_list[7].split()) > 1 :order= query_list[7].split()[1].strip()
                else:
                    order_by_col= query_list[8].split()[0].strip()
                    if len(query_list[8].split()) > 1 :order=query_list[8].split()[1].strip()
            else:
                if where_list == []:
                    order_by_col= query_list[5].split()[0].strip()
                    if len(query_list[5].split()) > 1 :order=query_list[5].split()[1].strip()
                else:
                    order_by_col= query_list[6].split()[0].strip()
                    if len(query_list[6].split()) > 1 : order=query_list[6].split()[1].strip()

        if order == "desc":
            final_result = sorted(final_result, key=lambda x: x[column_list.index(order_by_col)],reverse=True)
        elif order.lower() == "asc" or order == "":
            final_result = sorted(final_result, key=lambda x: x[column_list.index(order_by_col)])

    if group_in_column == False and group_by_col != "":
        column_list = column_list[1:]
        group_final_result = []
        for i in range(len(final_result)):
            if check_disticnct(query_list):
                if final_result[i][1:] not in group_final_result:
                    group_final_result.append(final_result[i][1:])
            else:
                group_final_result.append(final_result[i][1:])

        final_result = group_final_result

    

    header="<"
    hh=[]
    for column in column_list:
        column_name=""
        for table in database_schema:
            if column in database_schema[table]:
                column_name=table+"."+column
                
        if column_name != "": 
            header+=column_name+","
            hh.append(column_name)
        else: 
            header+=column+","
            hh.append(column)
    header=header[:len(header)-1]+">"
    print(header)
    # print(hh)
    answer = ""
    for row in final_result:
        # print(row)
        for col in row:
            answer+=(str(col)+",")
        answer =answer[:len(answer)-1]
        answer+=("\n")
    print(answer,end="")
except Exception as E:
    print(E)
    print("wrong query")