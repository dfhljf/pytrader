import pymssql;
from sqlalchemy import *;
# import Quote
import datetime as dt;
import pandas as pd;
# import Type;

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
		self.calendar=self.__getCalendar__()
		# get info about code
		self.codeinfo=self.__getCodeInfo__()

	def getBarList(self,description):
		'''{'CFEIF9000':{'TimeScale':dt.timedelta,'Date':(dt.date(),dt.date()),'Source':,'DealNA','Format':'DataFrame'}}'''
		rtn={}
		for k,v in description.items():
			rtn[k]=self.__getBarList__(k,v)
			
		return rtn    
		
	def getLevel(self,description):
		'''{'CFEIF9000':{'Aggressor':True,'BuySell':'',['']}}'''    
		pass
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
		bestvalue=info['TimeScale']/dt.timedelta(seconds=1*60)
		
		for k,v in datasource['TimeScale'].items():
			value=info['TimeScale']/k
			if value<bestvalue and info['TimeScale']%k==dt.timedelta(seconds=0):
				bestvalue=value
				bestkey=k
		
		if bestvalue<1:
			raise Exception(info['Source']+' don\'t deal with the timescale!')
		
		if tablename=='F_L_':
			#pass
			rtn=pd.read_sql_query('exec ConvertTimeScale N\'{acsycode}\',N\'{fro}\',{to},N\'{start}\',N\'{end}\''.format(\
			acsycode=code,fro=datasource['TimeScale'][bestkey],to=info['TimeScale'].total_seconds()*2,start=info['Date'][0].strftime('%Y-%m-%d'),end=info['Date'][1].strftime('%Y-%m-%d')),self.dbconn)
		else:
			rtn=pd.read_sql_query('select * from {tablename} where AcsyCode=N\'{acsycode}\' and Date>=N\'{start}\' and Date<=N\'{end}\' order by Date'.format(tablename=tablename,\
			acsycode=code,start=info['Date'][0].strftime('%Y-%m-%d'),end=info['Date'][1].strftime('%Y-%m-%d')),self.dbconn)
		
		# calculate volume
		data=pd.DataFrame()
		rtngrp=rtn.groupby('Date')
		for name,group in rtngrp:
			group['AccuBargainAmount']=group['AccuBargainAmount'].apply(pd.Series.interpolate)
			group['Volume']=pd.rolling_apply(group['AccuBargainAmount'],window=2,func=lambda x: x[1]-x[0])
			group['Volume'][0]=group['AccuBargainAmount'][0]
			group['Date']=name
			if info['DealNA']=='Remove':
				group=group[[not x for x in group.isnull().values.any()]]
			elif info['DealNA']=='Fill':
				group=group.ffill()
			if data.empty:
				data=group
			else:
				data=pd.concat([data,group],axis=1)
		
		data['Datetime']=pd.to_datetime(data['Date']+' '+data['Time'],format='%Y-%m-%d %H%M%S%f')
		rtn=data[['Code','AcsyCode','Datetime','OpenPrice','HighPrice','LowPrice','ClosePrice','Volume','OpenInterest']]
		
		return self.__convertToBarList__(rtn,code,info) if info['Format']=='BarList' else rtn
	def __convertToBarList__(self,df,code,info):
		rtn=Quote.BarList(code,info['TimeScale'])
		
		for x in df.index:
			dat=df.loc[x,:]
			code=dat.Code
			dt_start=dat.Datetime
			dt_end=dat.Datetime+info['TimeScale']
			
			bar=Bar(code,dt_start,dt_end,0)
			bar.setvalue(dat.OpenPrice,dat.HighPrice,dat.LowPrice,dat.ClosePrice,dat.Volume,dat.OpenInterest)
			rtn.push(bar)
		return rtn
		
	def __getCodeInfo__(self):
		CommodityInfo=pd.read_sql_query('select * from CommodityInfo where AcsyCode like N\'%0000\'',self.dbconn)
		TradeDate=pd.read_sql_query('select * from TradeDate where StartDate is not null',self.dbconn)
		TradeTime=pd.read_sql_query('select * from TradeTime',self.dbconn)
		CommodityInfo.index=CommodityInfo['AcsyCode'].apply(lambda x: x[:-4])
		CommodityInfo.drop(labels='AcsyCode',axis=1,inplace=True)
		CommodityInfo=CommodityInfo.T.to_dict()
		TradeDate.index=TradeDate['AcsyCode']
		TradeDate=TradeDate.T.to_dict()
		TT={}
		for name,group in TradeTime.groupby('AcsyCode'):
			TT[name]=[(dt.datetime.strptime(v['StartTime'],'%H%M%S%f').time(),dt.datetime.strptime(v['EndTime'],'%H%M%S%f').time()) for k,v in group.T.to_dict().items()]
		TradeTime={k:{(dt.datetime.strptime(v['StartDate'],'%Y-%m-%d').date(),dt.date(9999,12,31) if v['EndDate']==None else dt.datetime.strptime(v['EndDate'],'%Y-%m-%d').date()):\
		TT[k]} for k,v in TradeDate.items()}
		TT={}
		
		for k,v in TradeTime.items():
			key=k[:-4]
			if key in TT.keys():
				TT[key].update(v)
			else:
				TT[key]=v
		
		rtn={}
		keys=set(TT.keys())
		for k,v in CommodityInfo.items():
			v.update({'TradeTime': TT[k] if k in keys else []})
			rtn[k]=v
		return rtn
		
	def __getCalendar__(self):
		return pd.read_sql_query('select * from Calendar',self.dbconn)
		
		