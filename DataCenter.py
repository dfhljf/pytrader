import pymssql;
from sqlalchemy import *;
import Quote
import datetime as dt;
import pandas as pd;
import Type;

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
		self.codeinfo=self.__getCodeInfo__()
		#{'CFEIF0000':{'TradeUnit':,'Tick':,'TradeTime':{(dt.date(),dt.date()):[(dt.time(),dt.time()),()]}}}
	def getBarList(self,description):
	'''{'CFEIF9000':{'TimeScale':dt.timedelta,'Date':(dt.date(),dt.date()),'Source':,'DealNA','Format':'DataFrame'}}'''
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
		
		if tablename=='F_L_':
			rtn=pd.read_sql_query('exec ConvertTimeScale N\'{acsycode}\',N\'{from}\',{to},N\'{start}\',N\'{end}\''.format(acsycode=code,from=datasource['TimeScale'][bestkey],\
			to=info['TimeScale'].total_seconds()*2,start=info['Date'][0].strftime('%Y-%m-%d'),end=info['Date'][1].strftime('%Y-%m-%d')),self.dbconn )
		else:
			rtn=pd.read_sql_query('select * from {tablename} where AcsyCode=N\'{acsycode}\' and Date>=N\'{start}\' and Date<=N\'{end}\' order by Date'.format(tablename=tablename,\
			acsycode=code,start=info['Date'][0].strftime('%Y-%m-%d'),end=info['Date'][1].strftime('%Y-%m-%d')),self.dbconn)
		
		rtn['Datetime']=pd.to_datetime(rtn['Date']+' '+rtn['Time'],format='%Y-%m-%d %H%M%S%F')
		rtn.drop(labels=['Date','Time'],axis=1,inplace=True)
		return self.__convertToBarList__(rtn,code,info) if info['Format']=='BarList' else rtn
	def __convertToBarList__(self,df,code,info):
		rtn=Quote.BarList(code,info['TimeScale'])
		for x in df.index:
			dat=df.loc[x,:]
			code=dat.Code
			datetime=dat.Datetime
			op=dat.OpenPrice
			hi=dat.Hi
			lo=dat.lo
			cl=dat.close
			bar=Bar(dat)
		
	def __getCodeInfo__(self):
		pass
		
		