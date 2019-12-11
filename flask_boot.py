# -*- coding: utf-8 -*-
"""
Created on Mon Dec 17 16:10:15 2018

@author: tsai
"""

from flask import Flask,render_template, request
from flask_bootstrap import Bootstrap
import time
from os.path import isfile, isdir, join
from os import walk
import os, time, sys
import pyodbc
import multiprocessing
import logging
app = Flask(__name__)
bootstrap = Bootstrap(app)

directory = 'C:\\Users\\'+time.strftime('%Y_%m_%d')
if not os.path.exists(directory):
    os.makedirs(directory)
logging.basicConfig(filename='C:\\Users\\'+time.strftime('%Y_%m_%d')+'\\'+ time.strftime('scan_%Y_%m_%d_%H_%M_%S.log'),datefmt='%Y_%m_%d_%H_%M_%S', format='%(asctime)s %(message)s')

#網頁模式
@app.route('/', methods=['GET' , 'POST'])
def index():
    if request.method == 'POST':
        try:
            loginuser = Selectuser()
            mydirname, myproj, mypath,myfilename,myonelevelscan = SelectProj_list(request.values['myID'])
            MountDisk()
            DeleteFileIndex(request.values['myID'])
            count, mytime =SplitFile(request.values['myID'],mypath,myfilename,myproj,loginuser,mydirname,myonelevelscan)
            return "檔案數目: "+str(count)+"; 總花費時間: "+str(mytime)+"秒"
        except Exception as e:
            logging.error('IndexFailed ', exc_info=True)
    if request.method == 'GET':
        return render_template('index.html')
#API模式
@app.route('/myproject/<string:inputid>', methods=['GET'])
def API(inputid):
    if request.method == 'GET':
        try:
            mydirname, myproj, mypath,myfilename,myonelevelscan = SelectProj_list(inputid)
            loginuser = myproj
            MountDisk()
            DeleteFileIndex(inputid)
            count, mytime =SplitFile(inputid,mypath,myfilename,myproj,loginuser,mydirname,myonelevelscan)
            return "檔案數目: "+str(count)+"; 總花費時間: "+str(mytime)+"秒"
        except Exception as e:
            logging.error('APIFailed ', exc_info=True)       

#排程模式        
@app.route('/schedule', methods=['GET'])
def schedule():
    server = 'serverIP'
    database = 'DbName'
    username = 'username'
    password = 'password'
    try:
        MountDisk()
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()
        
        for i in range(0,18): #(0,13)1 to 13
            pool = multiprocessing.Pool(processes=7)
            #讀取資料庫專案群
            cursor.execute("SELECT (SQL)")
            ID=[]
            path=[] 
            filename=[] 
            proj=[]
            loginuser= []
            dirname = []
            onelevelscan = []
            for row in cursor.fetchall():
                ID.append(row[0])
                path.append(row[1])
                filename.append(row[2])
                proj.append(row[3])
                loginuser.append("排程")
                dirname.append(row[4])
                onelevelscan.append(row[5])
                DeleteFileIndex(row[0])
            
            
            pool.starmap(SplitFile, zip(ID,path,filename,proj,loginuser,dirname,onelevelscan))     
            pool.close()
            pool.join()
        return("Done")
    except Exception as e:
        logging.error('ScheduleFailed ', exc_info=True)
        
        



def SplitFile(myID,mypath,myfile,myproj,loginuser,mydirname,myonelevelscan):
    tStart = time.time()

    count=0
    dirpath=""
    level=[]
    param=[]
    
    dirpath="IP"+mydirname+"/"
    # 遞迴列出所有子目錄與檔案
    for root, dirs, files in walk(mypath):
        slack="\\"
        for name in files:
            fullpath = join(root, name)
            temppath = fullpath[fullpath.find(myfile):]
            temppath = temppath[temppath.find(slack):]
            while(temppath[temppath.find(slack)+1:].find(slack) > 0):     
                temppath2 = temppath[temppath.find(slack)+1:]            
                level.append(temppath[temppath.find(slack)+1:temppath2.find("\\")+1])
                dirpath= dirpath+ temppath[temppath.find(slack)+1:temppath2.find("\\")+1]+"/"
                temppath = temppath2[temppath2.find(slack):]  
            
            localtime   = time.localtime()
            timeString  = time.strftime("%Y/%m/%d %H:%M:%S", localtime)
            fullpath = fullpath.replace('\\', '/')
            dirpath = dirpath + name
            
            if(os.path.exists(fullpath)):
                if(len(level) == 0):
                    paramMerge(myID,myproj,fullpath,name,time.ctime(os.path.getctime(fullpath)),os.path.getsize(fullpath),timeString,param,"","","","","","","","",dirpath)
                    count = count+1
                elif(len(level) == 1 and myonelevelscan == 'n'):
                    paramMerge(myID,myproj,fullpath,name,time.ctime(os.path.getctime(fullpath)),os.path.getsize(fullpath),timeString,param,level[0],"","","","","","","",dirpath)
                    count = count+1
                elif(len(level) == 2 and myonelevelscan == 'n'):
                    paramMerge(myID,myproj,fullpath,name,time.ctime(os.path.getctime(fullpath)),os.path.getsize(fullpath),timeString,param,level[0],level[1],"","","","","","",dirpath)
                    count = count+1
                elif(len(level) == 3 and myonelevelscan == 'n'):
                    paramMerge(myID,myproj,fullpath,name,time.ctime(os.path.getctime(fullpath)),os.path.getsize(fullpath),timeString,param,level[0],level[1],level[2],"","","","","",dirpath)
                    count = count+1
                elif(len(level) == 4 and myonelevelscan == 'n'):
                    paramMerge(myID,myproj,fullpath,name,time.ctime(os.path.getctime(fullpath)),os.path.getsize(fullpath),timeString,param,level[0],level[1],level[2],level[3],"","","","",dirpath)
                    count = count+1
                elif(len(level) == 5 and myonelevelscan == 'n'):
                    paramMerge(myID,myproj,fullpath,name,time.ctime(os.path.getctime(fullpath)),os.path.getsize(fullpath),timeString,param,level[0],level[1],level[2],level[3],level[4],"","","",dirpath)
                    count = count+1
                elif(len(level) == 6 and myonelevelscan == 'n'):
                    paramMerge(myID,myproj,fullpath,name,time.ctime(os.path.getctime(fullpath)),os.path.getsize(fullpath),timeString,param,level[0],level[1],level[2],level[3],level[4],level[5],"","",dirpath)
                    count = count+1
                elif(len(level) == 7 and myonelevelscan == 'n'):
                    paramMerge(myID,myproj,fullpath,name,time.ctime(os.path.getctime(fullpath)),os.path.getsize(fullpath),timeString,param,level[0],level[1],level[2],level[3],level[4],level[5],level[6],"",dirpath)
                    count = count+1
                elif(len(level) >= 8 and myonelevelscan == 'n'):#
                    paramMerge(myID,myproj,fullpath,name,time.ctime(os.path.getctime(fullpath)),os.path.getsize(fullpath),timeString,param,level[0],level[1],level[2],level[3],level[4],level[5],level[6],"",dirpath)
                    count = count+1
           else:
                logging.warning("NO exits on the path :"+fullpath, exc_info=True)

            
            dirpath="IP"+mydirname+"/"
            level.clear()
    InsertSplitTb(param)
    tEnd = time.time()
    print ("檔案數目",count,"; It cost ",(tEnd - tStart)," sec")
    InsertLogFile(loginuser,myproj,(tEnd - tStart),count)
    return count, (tEnd - tStart)

def MountDisk(): #要做Mount
    server = 'IP' 
    database = 'dBName' 
    username = 'username' 
    password = 'password' 
    
        
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    
    cursor.execute("(SQL)SELECT")

    for row in cursor.fetchall():
        disk=str(row[0])
        path=str(row[1])
        backup_storage_available = os.path.isdir(disk)
        
        if backup_storage_available:
            print("Backup storage already connected.")
        else:
            print("Connecting to backup storage.")
            mount_command = "net use "+disk+" "+path+" /user:帳號 密碼"
            os.system(mount_command)

#刪除之前的資料庫資料,之後在新增
def DeleteFileIndex(myID):
    server = 'IP' 
    database = 'dbName' 
    username = 'username' 
    password = 'password' 
    
        
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    cursor.execute("SQL(Delete)",(myID))
    
    cnxn.commit()

        
def Selectuser():
    server = 'IP' 
    database = 'dbName' 
    username = 'username' 
    password = 'password' 
    
        
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    
    cursor.execute("SQL(SELECT)")
    for row in cursor.fetchall():
        return row[0]

#可以針對其中一個proj做更新
def SelectProj_list(myID):
    server = 'IP' 
    database = 'dbName' 
    username = 'username' 
    password = 'password' 
    
        
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    
    cursor.execute("SQL(SELECT)"+ str(myID))
    for row in cursor.fetchall():
        return row[0], row[1], row[2], row[3], row[4]

def InsertSplitTb(param):
    server = 'IP' 
    database = 'dbName' 
    username = 'username' 
    password = 'password' 
    
    try:    
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = cnxn.cursor()

        cursor.executemany("SQL(insert))"
                   ,param)
        cnxn.commit()
    except Exception as e:
        logging.error('InsertFailed ', exc_info=True)
        
def InsertLogFile(loginuser, proj, mytime, count): 
    server = 'IP' 
    database = 'dbName' 
    username = 'username' 
    password = 'password' 
    
    localtime   = time.localtime()
    timeString  = time.strftime("%Y/%m/%d %H:%M:%S", localtime)    
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()

    cursor.execute("SQL(insert))"
                   ,loginuser,proj,mytime,count,timeString)
    cnxn.commit()
    
def paramMerge(myID,myproj,filename,short,d_date,d_size,levela,param,level1,level2,level3,level4,level5,level6,level7,level8,level9): #參數做合併
    
    try:
        param.append([myID,myproj,filename,short,d_date,d_size,level1,level2,level3,level4,level5,level6,level7,level8,level9,levela])
        #print(param)
    except Exception as e:
        logging.error('paramMergeFailed ', exc_info=True)

#與按鈕歷史紀錄串接
@app.route('/history', methods=['GET', 'POST'])
def history():
    server = 'IP' 
    database = 'dbName' 
    username = 'username' 
    password = 'password' 
    
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()       
    cursor.execute("SQL(select)")
    data = cursor.fetchall() #data from database
    return render_template("xxx.html", value=data)
    
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')