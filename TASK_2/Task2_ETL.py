import argparse
from pathlib import Path
import json 
from subprocess import PIPE, Popen
from os import listdir
from os.path import isfile, join
import fnmatch
import pandas as pd
from pandas import json_normalize
import numpy as np
from datetime import datetime
import os

Flag =False
time = datetime.now()
#create parser instance 
parser = argparse.ArgumentParser()    
#add positional argument
parser.add_argument("dir_path",help = "enter the directory_path")
#add optional argument
parser.add_argument("-u",action="store_true",default = False,dest="timeFormat")
#parse the arguments
args = parser.parse_args()

basePath = Path(args.dir_path)
# list for all files in a directory
files = [file for file in basePath.iterdir() if(file.is_file() & fnmatch.fnmatch(file,"*.json"))]
#files = [item for item in listdir(basePath) if isfile(join('.', item))]
# empty dict for checksum and file in a key and value format.
checksums = {}
# empty list for the duplicated checksums
duplicates = []


# Iterate over the list of files
for filename in files:
    # Use Popen to call the md5sum utility
    with Popen(["md5sum", filename], stdout=PIPE) as proc:
        # checksum command return list of two elements
        # value of hash function
        # file name
        # the following expression will retrieve only the value of hash function
        checksum = proc.stdout.read().split()[0]
        # Append duplicate to a list if the checksum is found
        if checksum in checksums:
            duplicates.append(filename)
            Flag=True
        else:    
            checksums[checksum] = filename
if(Flag==True):
    print(f"Found Duplicates: {duplicates}")
else:
    print("Not Found Duplicates")
for filename in duplicates:
        os.remove(filename)
for file in files:
        if(file not in duplicates):
            records = [json.loads(line) for line in open(file,'r')]
            df = pd.json_normalize(records)
            df=df[['a','r','u','cy','ll','tz','t','hc']]
            web_browser=df['a'].str.split(' ', n=1, expand=True)[0]
            web_browser=df['a'].str.split('/', n=1, expand=True)[0]
            df['web_browser'] = web_browser
            operating_sys = df['a'].str.split("(" , expand = True , n = 1)
            operating_sys = operating_sys[1].str.split(" ", expand = True , n = 1)[0]
            operating_sys=operating_sys.str.replace(';',"")
            df['operating_sys'] = operating_sys
            df['r'] = df['r'].replace(r'http://', '', regex=True)
            df['r'] = df['r'].str.split('/', n=1, expand=True)[0]
            df['u']=df['u'].str.split('//', n=1, expand=True)[1]
            df['u']=df['u'].str.split('/', n=1, expand=True)[0]
            df['longitude'] = df['ll'].str[0]
            df['latitude'] = df['ll'].str[1]
            df.rename(columns={'tz':'timezone','t':'time_in','hc':'time_out','cy':'city','r':'from_url','u':'to_url'},inplace=True)
            df.drop(columns=['a','ll'],inplace=True)
            df = df.dropna() 
            if(args.timeFormat):
                df['time_in'] = df['time_in']
                df['time_out'] = df['time_out']
            else: 
                df['time_in'] = pd.to_datetime(df['time_in'])
                df['time_out'] =pd.to_datetime(df['time_out'])
            df=df[['web_browser','operating_sys','from_url','to_url','city','longitude','latitude','timezone','time_in','time_out']]
            print('The number of rows transformed from file  :',len(df), 'and directory path :',args.dir_path )
            df.set_index('web_browser',inplace=True)
            #print(df)
            csv_file = str(file).replace('.json',' ')
            csv_file=csv_file.split('/')[1]
            df.to_csv(args.dir_path+'/Target/'+csv_file+'.csv')
total_excutation_time = (datetime.now() - time)
print('Total Execuation Time {}'.format(total_excutation_time))
    