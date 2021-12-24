# project: p5
# submitter: zluo43
# partner: none
# hours: 18


import pandas as pd

import geopandas

import time

import matplotlib.pyplot as plt

import csv
from zipfile import ZipFile
from io import TextIOWrapper
from io import BytesIO
import time
import json
import sys
import netaddr
import re



def binary_search(arr,x):
    
#adopted from internet, but changed for this project because we are 
#looking at a range of ip here: the df['low'] is not continuous, so when we use binary search, we want the left value 
#when it matches the credential 
#Here the binary search will give the low bound that satfisfies the ip range 
    low, high = 0, len(arr) - 1
    while low < high:
        mid = low + (high - low) // 2
        if x - arr[mid] > arr[mid + 1] - x:
            low = mid + 1
        else:
            high = mid
    if arr[low-1] <= x and arr[low] > x:
        return low - 1
    else:
        return low


    

def df_convert(target= 'ip2location.csv'):
  
    with open('ip2location.csv') as f:
             file= f.read()
    file_clean= file.split("\n")
    ip_list= []
    for line in file_clean:
        line_clean = line.split(",")
        ip_list.append(line_clean)
    ip_df = pd.DataFrame(ip_list)   
    ip_df.rename(columns = ip_df.iloc[0],inplace=True) #return a new dataframe 
    #ip_df.dropna()
    ip_df.sort_values(by = ["low"])     # in case it is not sorted, although it looks like it is sorted??
    ip_df.drop(ip_df.index[-1],inplace=True)        #drop last and first row; last row looks wrong coz its empty 
    ip_df.drop(ip_df.index[0],inplace=True)
    #print (ip_df)
    return ip_df 




def ip_check(ip_input,ip_df): #implement search binary search

    ip_data=list(map(int,ip_df["low"])) #put ip into a list and make sure it is integer by using map()
    #print (ip_data)
    ip_match = []
    for ip in ip_input: 
        try:
   
            ip_match_dict = {}
            int_ip = int(netaddr.IPAddress(ip))
            t1 = time.time() 
            index_pos= binary_search(ip_data,int_ip)  #binary search to find index position on original DF
            t2 = time.time()
            
            ip_match_dict["ip"] = ip
           
            ip_match_dict["region"] = ip_df.iloc[index_pos,3]    #third column for regions 
            
            ip_match_dict["int_ip"] = int_ip
            
            ip_match_dict["ms"] = t2 - t1
            
            ip_match.append(ip_match_dict)
            
        except:
            print('wrong')
            break
            
    ip_match_js = json.dumps(ip_match)
    print(ip_match_js)
#     ip_match_try=json.loads(ip_match_js)
    
    return ip_match_js

#part 2 sample

def zip_iterator(target):
    with ZipFile(target) as zf:
        with zf.open(target.replace('.zip','.csv')) as f:
            reader = csv.reader(TextIOWrapper(f))
            for row in reader:
                yield row
def sample(zip1,zip2,mod):
    
    #print (ip_df.head())
    
  
    reader = zip_iterator(zip1)
    header = next(reader) # the list of all column names
    header.append('region')
    ip_idx = header.index("ip")
    zip2_list = []
    row_index = 0
    ip_df=df_convert()
   
    
    
    for row in reader: #row refer to actual data
        if row_index % int(mod) == 0:   #no remainder 
            ip_list = [str(re.sub(r'[a-zA-Z]','0',row[ip_idx]))]    #replace alphabet with 0
            row.append(json.loads(ip_check(ip_list,ip_df))[0]['region'])   #list of dict so calling index 
            zip2_list.append(row)
        row_index =row_index+1
 
    
    #zip2_list_sort=sorted(zip2_list) #coz only for list
    zip2_list.sort(key=lambda 
                row: int(netaddr.IPAddress(re.sub(r'[a-zA-A]','0', row[0]))))
    
    #write another 
    with ZipFile(zip2, "w") as zf:
        with zf.open(zip2.replace(".zip", ".csv"), "w") as raw:
            with TextIOWrapper(raw) as f:
                writer = csv.writer(f, lineterminator='\n')
                writer.writerow(header)
                for row in zip2_list:    #change
                    writer.writerow(row)
def world(file,svg):
    counter = {}
    world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    with ZipFile(file) as zf:     
        files = zf.namelist()
        for file in files:
            with zf.open(file) as f:
                reader = pd.read_csv(f)
                region = reader['region'].tolist()  #convert a column to list
                for ele in region:
                    #use setdefault because the key value is not in the dictionary yet;default count is 0
                    #same as get()
                    counter[ele] = counter.setdefault(ele,0)+1          
                    
    country=world['name'].tolist()
    for i in country:
        if i not in list(counter.keys()):
            counter[i]=counter.setdefault(i,0)
            #This is the region that is not on web_request so number=0
    new_df=pd.DataFrame(list(counter.items()),columns=['name','Count']) #keep name so can merge on common 
    #world.join(new_df)
    #merget probably works better
    world1 = world.merge(new_df)
    world_no_antarctica = world1[world1['continent'] != 'Antarctica']
    ax = world_no_antarctica.plot(column='Count',figsize = (14,12))
    ax.set_title('Web Request')
    ax.get_figure().savefig(str(svg),format="svg", bbox_inches="tight")
    
    
def phone(input):
    phone_number = []
    phone_re= r"\(?\d{3}\)?\ ?-?\d{3}-\d{4}"       #(xxx) xxx-xxxx
    with ZipFile(input) as zf:
        for extension in zf.namelist():
            if 'htm' in extension:
                with zf.open(extension) as f:
                    tio = TextIOWrapper(f)
                    reader = tio.read()
                    output = list(set(re.findall(phone_re,reader)))
                    phone_number.extend(output)      #extend used on a list of addition
    phone_number = list(set(phone_number))

    #return phone_number
    for i in phone_number: #print on each own line
        print (i)

def main():
    if len(sys.argv) < 2:
        print("usage: main.py <command> args...")
    elif sys.argv[1] == "ip_check":
        ip_input = sys.argv[2:]
        ip_df=df_convert()
        ip_check(ip_input,ip_df)
    elif sys.argv[1] == "sample":
        sample(sys.argv[2],sys.argv[3],sys.argv[4])
        
    elif sys.argv[1] == "world":
        world(sys.argv[2],sys.argv[3])   
    
    elif sys.argv[1] == "phone":
        phone(sys.argv[2])        
        
    else:
        print("unknown command: " + sys.argv[1])
        
if __name__ == '__main__':
     main()