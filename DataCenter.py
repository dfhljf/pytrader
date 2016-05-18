import pymssql;
from sqlalchemy import *;
import Quote
import datetime
import pandas as pd;
# import Type;
import os;
import json;
#import JSONConverter;

class DataCenter(object):
	def __init__(self):
		self.config=self.__getConfig__()
		self.sourceinfo=self.__getSourceInfo__()
		self.dbconn=create_engine('mssql+pymssql://{user}:{password}@SERVER03/FTBDB'.format(user=self.config['DataBase']['User'],password=self.config['DataBase']['Password']))
		self.calendar=self.__getCalendar__()
		self.codeinfo=self.__getCodeInfo__()

	def getBarList(self,description):
		rtn={}
		pivot=pd.DataFrame()
		for k,v in description.items():
			rtn[k]=self.__getBarList__(k,v)
			if v['Format']=='Pivot':
				x=rtn.pop(k)
				x['Datetime']=x.index
				if pivot.empty:
					pivot=x
				else:
					pivot=pd.concat([pivot,x])
		# pivot all 
		rtn['Open']=pivot.pivot('Datetime','AcsyCode','OpenPrice')
		rtn['High']=pivot.pivot('Datetime','AcsyCode','HighPrice')
		rtn['Low']=pivot.pivot('Datetime','AcsyCode','LowPrice')
		rtn['Close']=pivot.pivot('Datetime','AcsyCode','ClosePrice')
		rtn['Volume']=pivot.pivot('Datetime','AcsyCode','Volume')
		rtn['OpenInt']=pivot.pivot('Datetime','AcsyCode','OpenInterest')	
		return rtn    
		
	def getLevel(self,description):
		'''{'CFEIF9000':{'Aggressor':True,'BuySell':'',['']}}'''    
		pass
	def __getBarList__(self,code,info):
		# choose time scale
		datasource=self.sourceinfo[info['Source']]
		tablename=datasource['Table']
		bestvalue=info['TimeScale']/datetime.timedelta(seconds=1*60)
		bestkey=datetime.timedelta(seconds=1*60)
		
		for k,v in datasource['TimeScale'].items():
			value=info['TimeScale']/k
			if value<bestvalue and info['TimeScale']%k==datetime.timedelta(seconds=0):
				bestvalue=value
				bestkey=k
		
		if bestvalue<1:
			raise Exception(info['Source']+' don\'t deal with the timescale!')
		
		if tablename=='F_L_':
			#pass
			rtn=pd.read_sql_query('exec ConvertTimeScale N\'{acsycode}\',N\'{fro}\',{to},N\'{start}\',N\'{end}\''.format(\
			acsycode=code,fro=datasource['TimeScale'][bestkey],to=info['TimeScale'].total_seconds()*2,start=info['Date'][0].strftime('%Y-%m-%d'),end=info['Date'][1].strftime('%Y-%m-%d')),self.dbconn)
		else:
			rtn=pd.read_sql_query('select * from {tablename} where AcsyCode=N\'{acsycode}\' and Date>=N\'{start}\' and Date<=N\'{end}\' order by Date'.format(tablename=tablename+datasource['TimeScale'][bestkey],\
			acsycode=code,start=info['Date'][0].strftime('%Y-%m-%d'),end=info['Date'][1].strftime('%Y-%m-%d')),self.dbconn)
			rtn['Time']='000000000'
		
		# calculate volume
		if info['TimeScale']<=datetime.timedelta(days=1):
			index=list(rtn.index[[True]+list(rtn['Date'][1:]!=rtn['Date'][:-1])])+[len(rtn)]
			for x in range(0,len(index)-1):
				rtn.loc[index[x]:index[x+1]-1,'AccuBargainAmount']=rtn.loc[index[x]:index[x+1]-1,'AccuBargainAmount'].interpolate()
			rtn['Volume']=[0]+list(rtn['AccuBargainAmount'][1:].values-rtn['AccuBargainAmount'][:-1].values)
			index.pop(len(index)-1)
			rtn.loc[index,'Volume']=rtn.loc[index,'AccuBargainAmount']
		
		# deal na
		if info['DealNA']=='Remove':
			rtn=rtn[[not x for x in rtn.isnull().values.any(axis=1)]]
		elif info['DealNA']=='Fill':
			rtn=rtn.ffill()
		
		# datetime for index
		rtn.index=pd.to_datetime(rtn['Date']+' '+rtn['Time'],format='%Y-%m-%d %H%M%S%f')
		rtn=rtn[['Code','AcsyCode','OpenPrice','HighPrice','LowPrice','ClosePrice','Volume','OpenInterest']]
		
		# format output
		if info['Format'] in {'BarList'}:
			rtn=self.__convertToBarList__(rtn,code,info)

		return rtn
	def __convertToBarList__(self,df,code,info):
		df['dt_start']=df.index
		df['dt_end']=df['dt_start']+info['TimeScale']
		tmp=df.as_matrix().tolist()
		rtn=Quote.BarList(code,info['TimeScale'])
		
		for x in tmp:
			bar=Quote.Bar(code,x[-2],x[-1],0)
			bar.setvalue(*x[2:-2])
			rtn.push(bar)
		return rtn
	# init info	
	def __getCodeInfo__(self):
		rtn={}
		if self.config['UseCache']:
			rtn=self.__loadCache__('CodeInfo.json')
		if rtn=={}:
			CommodityInfo=pd.read_sql_query('select Future,Exchange,TradeUnit,Tick,Target,DeliveryMethod,Unit1,Unit2'+\
      			',TablePrefix,DominantContracts,AcsyCode from CommodityInfo where AcsyCode like N\'%0000\'',self.dbconn)
			TradeDate=pd.read_sql_query('select * from TradeDate where StartDate is not null',self.dbconn)
			TradeTime=pd.read_sql_query('select * from TradeTime',self.dbconn)
			CommodityInfo.index=CommodityInfo['AcsyCode'].apply(lambda x: x[:-4])
			CommodityInfo.drop(labels='AcsyCode',axis=1,inplace=True)
			CommodityInfo=CommodityInfo.T.to_dict()
			TradeDate.index=TradeDate['AcsyCode']
			TradeDate=TradeDate.T.to_dict()
			TT={}
			for name,group in TradeTime.groupby('AcsyCode'):
				TT[name]=[(datetime.datetime.strptime(v['StartTime'],'%H%M%S%f').time(),datetime.datetime.strptime(v['EndTime'],'%H%M%S%f').time()) for k,v in group.T.to_dict().items()]
			TradeTime={k:{(datetime.datetime.strptime(v['StartDate'],'%Y-%m-%d').date(),datetime.date(9999,12,31) if v['EndDate']==None else datetime.datetime.strptime(v['EndDate'],'%Y-%m-%d').date()):\
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
			self.__saveCache__('CodeInfo.json',rtn)
		return rtn
		
	def __getCalendar__(self):
		calendar={}
		if self.config['UseCache']:
			calendar=self.__loadCache__('Calendar.json')
		if calendar=={}:
			calendar=pd.read_sql_query('select * from Calendar',self.dbconn)
			self.__saveCache__('Calendar.json',calendar,'frame')
		return calendar
		
	# config
	def __getConfig__(self):
		with open('DataCenter.Config.json','r') as fp:
			return json.load(fp)
	def __getSourceInfo__(self):
		sourceinfo={}
		if self.config['UseCache']:
			sourceinfo=self.__loadCache__('SourceInfo.json')
		if sourceinfo=={}:
			unit={'m':60,'h':3600,'D':86400}
			for k,v in self.config['SourceInfo'].items():
				v['TimeScale']={datetime.timedelta(seconds=int(x[:-1])*unit[x[-1]]):x for x in v['TimeScale']}
				sourceinfo[k]=v
			self.__saveCache__('SourceInfo.json',sourceinfo)
		return sourceinfo
	def __loadCache__(self,filename,typ='dict'):
		rtn={}
		if os.path.exists(self.config['CachePath']+'/'+filename):
			if typ=='dict':
				with open(self.config['CachePath']+'/'+filename,'r') as fp:
					rtn= eval(fp.read())
			elif typ in {'frame','series'}:
				rtn=pd.read_json(self.config['CachePath']+'/'+filename,typ=typ)
		return rtn
	def __saveCache__(self,filename,obj,typ='dict'):
		self.__createDir__(self.config['CachePath'])
		if typ=='dict':
			with open(self.config['CachePath']+'/'+filename,'w') as fp:
				fp.write(str(obj))
		elif typ in {'frame','series'}:
			obj.to_json(self.config['CachePath']+'/'+filename)
	def __createDir__(self,path):
		if not os.path.exists(path):
			os.makedirs(path)
			return True
		return False