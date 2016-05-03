import pymssql;
from sqlalchemy import *;
import Quote
import datetime as dt;

class DataCenter(object):
    def __init__(self):
        self.dbconn=create_engine()
        self.alltimescale=[dt.timedelta(seconds=1*60),dt.time]
        # get info about code
        #{'CFEIF0000':{'TradeUnit':,'Tick':,'TradeTime':{(dt.date(),dt.date()):[(dt.time(),dt.time()),()]}}}
    def getBarList(self,description):
    '''{'CFEIF9000':{'TimeScale':dt.timedelta,'Date':(dt.date(),dt.date()),'Source':,'DealNA'}}'''
        # complete all property
        for k,v in description.items():
            
            
        # choose the best timescale to convert
    def getLevel(self,description):
    '''{'CFEIF9000':{'Aggressor':True,'BuySell':'',['']}}'''    