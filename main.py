from Quote import *;
import datetime as dt;
import JSONConverter;
import json;

def main():
    quote=Quote({'SHFAG9000':
    {'TimeScale':dt.timedelta(days=1),
    'Date':(dt.date(2010,1,1),dt.date(2016,5,13)),
    'Source':'Exchange',
    'DealNA':'Fill',
    'Format':'Pivot'},
    'SHFAU9000':
    {'TimeScale':dt.timedelta(days=1),
    'Date':(dt.date(2010,1,1),dt.date(2016,5,13)),
    'Source':'Exchange',
    'DealNA':'Fill',
    'Format':'Pivot'}})
    
    pass
if __name__ == '__main__':
    main()