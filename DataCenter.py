import pymssql;
from sqlalchemy import *;
import Quote
import datetime as dt;
import pandas as pd;

class DataCenter(object):
	def __init__(self):
		self.dbconn=create_engine('mssql+pymssql://STRAT01:ACSYstrat01@SERVER03/FTBDB')
		self.sourceinfo={\
							'Tick':\
							{\
								'Table':'F_L_',\
								'TimeScale':\
								{\
									dt.timedelta(seconds=1*60):'1m',\
									dt.timedelta(seconds=5*60):'5m',\
									dt.timedelta(seconds=15*60):'15m',\
									dt.timedelta(seconds=60*60):'1h'\
								}\
							},\
							'Exchange':\
							{\
								'Table':'ExchangeData_',\
								'TimeScale':\
								{\
									dt.timedelta(days=1):'1D'
								}\  
							},\
							'TradeBlazer':\
							{\
								'Table':'TBData_',\
								'TimeScale':\
								{\
									dt.timedelta(days=1):'1D'
								}\
							}\
					  }
		# get info about code
		#{'CFEIF0000':{'TradeUnit':,'Tick':,'TradeTime':{(dt.date(),dt.date()):[(dt.time(),dt.time()),()]}}}
	def getBarList(self,description):
	'''{'CFEIF9000':{'TimeScale':dt.timedelta,'Date':(dt.date(),dt.date()),'Source':,'DealNA'}}'''
		rtn={}
		for k,v in description.items():
			rtn[k]=self.__getBarList__(k,v)
			
		return rtn    
		
	def getLevel(self,description):
	'''{'CFEIF9000':{'Aggressor':True,'BuySell':'',['']}}'''    
	# def __check__(self,value,type='BarList'):
	#     if type=='BarList':
	#         paramslist=['TimeScale','Date','Source','DealNA']
	#         for k,v in value.items():
				
	#     elif type=='Level':
	#     else:
	#         raise Exception('check type isn\'t exist!')
	def __getBarList__(self,code,info):
		# choose time scale
		datasource=self.sourceinfo[info['Source']]
		tablename=datasource['Table']
		bestkey=datasource['TimeScale'].keys()[0]
		bestvalue=info['TimeScale']/bestkey
		
		for k,v in datasource['TimeScale'].items():
			value=info['TimeScale']/k
			if value<bestvalue and info['TimeScale']%k==dt.timedelta(seconds=0):
				bestvalue=value
				bestkey=k
		
		if bestvalue<1:
			raise Exception(info['Source']+' don\'t deal with the timescale!')
		
		rtn=