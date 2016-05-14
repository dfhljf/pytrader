import datetime as dt;
#from DataCenter import DataCenter;
import Type;
import math;
import pandas as pd;

class Tick(object):
    def __init__(self,code,datetime,lastprice,bid,ask,accuvol,accuturnover,openint):
        self.code=code
        self.datetime=datetime
        self.lastprice=lastprice
        self.bid=bid
        self.ask=ask
        self.accuvol=accuvol
        self.accuturnover=accuturnover
        self.openint=openint

class Level(object):
    def __init__(self,acsycode):
        self.acsycode=acsycode
        self.bid={}
        self.ask={}
    def update(self,tick):
        

class Bar(object):
    def __init__(self,code,start,end,vol_start):
        self.code=code
        self.dt_start=start
        self.dt_end=end
        self.vol_start=vol_start
        self.__is_init__=False
    def setvalue(op,high,low,close,accuvolume,openint):
        self.open=op
        self.high=high
        self.low=low
        self.close=close
        self.volume=accuvolume-self.vol_start
        self.openint=openint
    def update(self,tick):
        assert type(tick)==Quote.Tick
        if tick.code!=self.code:
            raise Exception('code don\'t match!')
        if now>=self.dt_end:
            return False
        if self.__is_init__:
            self.dt_now=tick.datetime
            self.high=max(self.high,tick.lastprice)
            self.low=min(self.low,tick.lastprice)
            self.close=tick.lastprice
            self.volume=tick.accuvol-self.vol_start
            self.openint=tick.openint
        else:
            self.dt_now=tick.datetime
            self.open=self.high=self.low=self.close=tick.lastprice
            self.volume=tick.accuvol-self.vol_start
            self.openint=tick.openint
            self.__is_init__=True
        return True

class BarList(object):
    def __init__(self,acsycode,timescale):
        self.acsycode=acsycode
        self.timescale=timescale
        self.barlist=pd.Series()
        self.datetime=[]
        self.open=pd.Series()
        self.high=pd.Series()
        self.low=pd.Series()
        self.close=pd.Series()
        self.volume=pd.Series()
        self.openint=pd.Series()
    def update(self,tick):
        if self.barlist.isempty():
            bar=self.__new_bar__(tick.code,tick.datetime,0)
            self.barlist.append(pd.Series(data=[bar],index=bar.dt_start))
            self.__sync__(True)
        while not self.barlist[0].update(tick):
            accuvol=self.barlist[0].vol_start+self.barlist[0].volume
            bar=self.__new_bar__(tick.code,self.barlist[0].dt_end,accuvol if tick.accuvol>=accuvol else 0)
            bar.setvalue(self.barlist[0].open,self.barlist[0].high,self.barlist[0].low,self.barlist[0].close,accuvol,self.barlist[0].openint)
            self.barlist.push(bar.dt_start,bar)  
            self.__sync__(True)
        self.__sync__(False)     
    def push(self,bar):
        assert type(bar)==Quote.Bar
        self.barlist.push(bar.dt_start,bar)
        self.__sync__(True)
    def __new_bar__(self,code,datetime,vol_start):
        count=(datetime.hour*3600+datetime.minute*60+datetime.second)/self.timescale.total_seconds()
        today=dt.datetime(datetime.year,datetime.month,datetime.day)
        bar=Bar(code,today+math.floor(count)*self.timescale,today+math.ceil(count)*self.timescale,vol_start)
        return bar
    def __sync__(self,is_insert):
        bar=self.barlist[0]
        if is_insert:
            self.datetime.insert(0,bar.dt_start)
            self.open.push(bar.dt_start,bar.open)
            self.high.push(bar.dt_start,bar.high)
            self.low.push(bar.dt_start,bar.low)
            self.close.push(bar.dt_start,bar.close)
            self.volume.push(bar.dt_start,bar.volume)
            self.openint.push(bar.dt_start,bar.openint)
        else:
            self.high[0]=bar.high
            self.low[0]=bar.low
            self.close[0]=bar.close
            self.volume[0]=bar.volume
            self.openint[0]=bar.openint
    def __getitem__(self,index):
        return self.barlist[index]
        

class Quote(object):
    def __init__(self,codemap):
        """codemap={'SHFRB1610':
                            {'start':dt.date(2015,1,1),
                            'end':dt.date(2016,1,1),
                            'timescale':dt.timedelta(minute=5),
                            'source':'F_L_[ExchangeData_1D,ACSYRTData,csv]',
                            'dealna':'remove[fill]'
                            }
                   }"""
        assert type(codelist)==dict
        self.__load_history__()
    def __getattr__(self,attr):
        self.datamap[attr]
    def __load_history__(self):
        pass

