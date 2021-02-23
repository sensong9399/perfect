# -*- coding: utf-8 -*-
from ws4py.client.threadedclient import WebSocketClient
import pandas as pd
import OpenOPC
import time
import json
import MySQLdb
import numpy as np
import prometheus_client
from prometheus_client import Gauge
from prometheus_client.core import CollectorRegistry
import requests


def readtags(tags = "/home/client/boxconfig/tags.xlsx"):
    #从excel读取tag
    df = pd.read_excel(tags)
    taglist={}
    #先计算出有多少种采集策略
    dfpv1 = pd.pivot_table(df,index=[u"采集策略"])
    for ti,tag in enumerate(dfpv1.iterrows()):
        taglist[tag[0]]=[] #初始化各策略的列表
    dfpv = pd.pivot_table(df,index=[u"采集策略",u"标签"])
    for ti,tag in enumerate(dfpv.iterrows()):
        taglist[tag[0][0]].append(tag[0][1])

    return taglist

def readtagsetting(tags = "/home/client/boxconfig/tags.xlsx"):
    #从excel读取tag
    df = pd.read_excel(tags)
    taglist={}
    #先计算出有多少种采集策略
    dfpv1 = pd.pivot_table(df,index=[u"采集策略"])
    for ti,tag in enumerate(dfpv1.iterrows()):
        taglist[tag[0]]=[] #初始化各策略的列表
    dfpv = pd.pivot_table(df,index=[u"采集策略",u"标签",u"值区间",u"最大值",u"最小值"])
    for ti,tag in enumerate(dfpv.iterrows()):
        taglist[tag[0][0]].append([tag[0][1],tag[0][2],tag[0][3],tag[0][4]])

    return taglist


def readconf(configfile="/home/client/boxconfig/config.json"):
    #读取系统配置文件
    f = open(configfile)
    content = f.read()
    conf = json.loads(content)
    f.close()
    return conf


def mysqlinsert(mysql_host, database, username, password, data):
    #向mysql插值
    try:
        db = MySQLdb.connect(mysql_host,username, password,database)
        cursor = db.cursor()
        try:
            sqlstr = "insert into item_data (item_id,long_time,write_count,project_id,create_time,timestamp,value) values (%s, %s, %s, %s, %s, %s, %s)"
            cursor.executemany(sqlstr,data)
            db.commit()
            cursor.close()
            db.close()

        except Exception, e:
            printlog("mysql insert error! " + str(e))
            print("mysql insert error! " + str(e))
    except:
        printlog("mysql setting error! " + str(e))
        print("mysql setting error! " + str(e))




class DummyClient(WebSocketClient):
    def opened(self):
        print("open websocket")
    def __init__(self,url):
        super(DummyClient,self).__init__(url)
    def received_message(self,m):
        pass
    def send(self, payload):
        try:
            super(DummyClient, self).send(payload)
        except Exception, e:
            try:
                print(e)
                #尝试重新连接
                super(DummyClient,self).__init__(self.url)
                super(DummyClient,self).connect()
                super(DummyClient,self).send(payload)
                printlog("sending:"+str(e) + " Successfully reconnected websocket!")
            except Exception, e:
                printlog("failed to send to cloud! "+ str(e)) #记录到logfile
                print("failed to send to cloud! "+ str(e))

def checkinternet():
    #检查外网
    pass


def checkopc2(taglist,opc):
    #检查opc
    print(taglist)
    d = readconf()
    if opc == "":  #最初opc为空
        try:  #c尝试第一次连接
            opc = OpenOPC.open_client(d["opcserverip"])
            opc.connect(d["opcservername"])
            opc.read(taglist)
        except Exception, e:
            printlog("Failed to connect opc:"+str(e))
            print("Failed to connect opc:"+str(e))
            opc = ""
    else: #并非第一次连接
        try: #测试这一批读取是否正常
            opc.read(taglist)
        except Exception, e: #如果不正常，最多尝试3次尝试重新连接。
            for i in range(1,3):
                printlog("opc reading:"+str(e))
                print("opc reading:"+str(e))
                try:
                    opc = OpenOPC.open_client(d["opcserverip"])
                    opc.connect(d["opcservername"])
                    opc.read(taglist)
                    if opc !="":
                        break
                except Exception, e:
                    printlog("checking opc " + str(i) + " times " + str(len(taglist)) + " tags " + "Read failed! " + str(e))
                    print("checking opc " + str(i) + " times " + str(len(taglist)) + " tags " + "Read failed! " + str(e))
                    opc = ""

    return opc
def matrixcalculate(settingarr,valuearr,lastarr,ratio):
    starttime=time.time()
    try:
        sarr=settingarr[:,1]
        varr=valuearr[:,1]
        larr=lastarr[:,1]
        sarr=sarr.astype(float)
        varr=varr.astype(float)
        larr=larr.astype(float)
        print("--rarr = (larr-varr)/sarr--",len(sarr),len(varr),len(larr))
        sarr[np.isnan(sarr)]="-0.001"
        varr[np.isnan(varr)]="-0.001"
        larr[np.isnan(larr)]="-0.001"
        rarr = (larr-varr)/sarr
        rarr[abs(rarr)>=ratio]=True
        rarr[abs(rarr)<ratio]=False
        rarr=rarr.astype(bool)
        resultarr = np.logical_not(rarr)
    except Exception,e:
        print(e)
        resultarr = np.array([])
    endtime=time.time()
    print("caculation takes " + str(endtime-starttime) + " seconds")
    return resultarr

def recordopc(tagv,opc,split):
    starttime=time.time()
    taglast = []
    try:
        taglist=tagv[:,0].tolist()
        batchs=int(len(taglist)/split)+1
        for batch in range(1,batchs+1):
            st=time.time()
            taglast+=opc.read(taglist[(batch-1)*split:min(batch*split,len(taglist))])
            ed=time.time()
            print("opc reading",(batch-1)*split,min(batch*split-1,len(taglist)),str(ed-st)+" seconds")

    except Exception,e:
        print("readopc erro: " +str(e))
        printlog("readopc erro: "+str(e))
        opc=checkopc2(taglist[(batch-1)*split:min(batch*split,len(taglist))],opc)
    endtime=time.time()
    print("read opc takes:"+str(endtime-starttime)+" seconds")
    return np.array(taglast)

def printlog(logtext,logtime = ""):
    #记录并打印日志文本，若未输入时间使用当前时间
    if logtime == "":
        logtime = time.localtime()
    timestr = str(time.strftime("%Y-%m-%d %H:%M:%S", logtime))
    logfile = open("/home/client/boxconfig/logfile","a+")
    readinglog = timestr + ", " + logtext + "\n"
    logfile.write(readinglog)
    logfile.close


if __name__ == "__main__":
    ls=time.time()
    lm=time.time()
    ld=time.time()
    lw=time.time()
    configj = readconf()
    ssecond = int(configj["S"]) #秒级采集频率
    sminute = int(configj["M"]) #分级采集频率
    sday = int(configj["D"]) #天级采集频率
    sweek = int(configj["W"]) #周级采集频率
    tagarray = {} #tags范围设定
    tagvalue ={}#tag当前值
    taglast ={}#tag上次值
    tagsetting = readtagsetting() #读出带有策略的tag清单
    tagarray["S"]=np.array(tagsetting["S"])
    tagarray["M"]=np.array(tagsetting["M"])
    tagarray["D"]=np.array(tagsetting["D"])
    tagarray["W"]=np.array(tagsetting["W"])
    ratio=float(configj["ratio"])
    opc = checkopc2(tagarray["S"][:,0].tolist()[1],"")
    print(opc)
    taglast["S"] = recordopc(tagarray["S"],opc,configj["split"])
    print(len(taglast["S"]),"taglast length")
    ws = DummyClient(configj["websocket_url"])
    ws.connect()
    while True:
        data = [] #新一轮，清空发给websocket的数据
        opcdata=[] #新一轮，情况opc读取数据
        if time.time() - lw > sweek:
            opcdata = recordopc(np.vstack((tagarray["S"],tagarray["M"],tagarray["D"],tagarray["W"])),opc,configj["split"]).tolist()
            taglast["S"] = recordopc(tagarray["S"],opc,configj["split"])
            print("weeeeeeeeeek",len(opcdata))
            lw=time.time()
        elif time.time() - ld > sday:
            opcdata = recordopc(np.vstack((tagarray["S"],tagarray["M"],tagarray["D"])),opc,configj["split"]).tolist()
            taglast["S"] = recordopc(tagarray["S"],opc,configj["split"])
            print("daaaaaaaaaay",len(opcdata))
            ld=time.time()
        elif time.time() - lm > sminute:
            opcdata = recordopc(np.vstack((tagarray["S"],tagarray["M"])),opc,configj["split"]).tolist()
            taglast["S"] = recordopc(tagarray["S"],opc,configj["split"])
            print("minuuuuuuuuuute",len(opcdata))
            lm=time.time()
        elif time.time() - ls > ssecond:
            tagvalue["S"] = recordopc(tagarray["S"],opc,configj["split"])
            tagresultarr = matrixcalculate(tagarray["S"],tagvalue["S"],taglast["S"],ratio)
            taglast["S"] = tagvalue["S"]
            try:
                opcdata=tagvalue["S"][np.logical_not(tagresultarr)]
                print('opcdata:',opcdata)
                if configj["showlist"]=="yes":
                    print(np.array(opcdata)[:,0].tolist())
            except Exception,e:
                print(tagresultarr,tagvalue["S"])
                print("array is wrong! ",e)
                opc = checkopc2(tagarray["S"][:,0].tolist()[1],"")
            ls=time.time()
        else:
            pass
        try:
            if len(opcdata) > 0:
                start = time.time()
                for i,item in enumerate(opcdata):
                    payload=""
                    record={}
                    record["item_id"] = str(item[0])
                    record["write_count"] = 1000000
                    record["project_id"] = configj["project_id"]
                    record["create_time"] = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                    record["timestamp"] =str(item[3])
                    record["value"] =  item[1]
                    record["long_time"] = str(int(time.time()))

                    payload = json.dumps(record)
                    if configj["sendtocloud"] == "yes":
                        #starttocassa=time.time()
                        ws.send(payload)
                        #endtocassa=time.time()
                        #print("sent to cassandra cloud," + str(endtocassa - starttocassa)+" seconds.")
                    data.append((str(item[0]),str(int(time.time())),1000000,configj["project_id"], time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),str(item[3]),item[1]))
                    if False:
                        starttocloud=time.time()

                        metric_name=str(item[0])
                        opc_name=metric_name[1:metric_name.find(']')]
                        tmpstr=metric_name[metric_name.find(']')+1:]
                        tag_name=tmpstr[0:tmpstr.find('.')]
                        subtag_name=tmpstr[tmpstr.find('.')+1:]
                        value=item[1]
                        projectid= configj["project_id"]
                        metric_name=tag_name+'__'+subtag_name

                        REGISTRY=CollectorRegistry(auto_describe=False)

                        my_metric=Gauge(metric_name,"metric name",["opc",'tag','subtag','project_id'],registry=REGISTRY)
                        my_metric.labels(opc=opc_name,tag=tag_name,subtag=subtag_name,project_id=projectid).set(value)

                        requests.post("http://47.244.196.79:9091/metrics/job/baolong/",data=prometheus_client.generate_latest(REGISTRY))
                        endtocloud=time.time()
                        print("sent to prometheus cloud," + str(endtocloud - starttocloud)+" seconds.")

                #插入本地数据库
                if configj["insertlocal"] == "yes":
                    mysqlinsert(configj["mysql_host"], configj["database"], configj["username"], configj["password"], data)
                    print("inserted to local mysql")
                else:
                    print("DO NOT insert to local mysql!")
                end = time.time()
                print(len(data),"records ,It takes ",end-start,"seconds")
        except Exception, e:
                print(e)
                printlog(str(e))

