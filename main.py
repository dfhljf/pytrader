from DataCenter import DataCenter;
import datetime as dt;
import JSONConverter;
import json;

def main():
    #json.dumps({dt.timedelta(days=1):0},cls=JSONConverter.JSONEncoder)
    #x=JSONConverter.JSONEncoder().encode({dt.timedelta(1):0})
    dc=DataCenter()
    dc.getBarList({'SHFAG9000':{'TimeScale':dt.timedelta(seconds=5*60),'Date':(dt.date(2010,1,1),dt.date(2016,5,13)),'Source':'Tick','DealNA':'Remove','Format':'DataFrame'}})
    pass
if __name__ == '__main__':
    main()