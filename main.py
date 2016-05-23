from Quote import *;
import datetime as dt;
import JSONConverter;
import json;

def main():
    quote=Quote({'SHFAG9000':
    {'TimeScale':dt.timedelta(seconds=1*60),
    'Date':(dt.date(2010,1,1),dt.date(2016,5,13)),
    'Source':'Tick',
    'DealNA':'Fill',
    'Format':'BarList'},
    'SHFAU9000':
    {'TimeScale':dt.timedelta(seconds=1*60),
    'Date':(dt.date(2010,1,1),dt.date(2016,5,13)),
    'Source':'Tick',
    'DealNA':'Fill',
    'Format':'BarList'}})
    
    pass
if __name__ == '__main__':
    main()