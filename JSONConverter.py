import json;
import datetime as dt;

class JSONDecoder(json.JSONDecoder):

    def __init__(self, *args, **kargs):
        json.JSONDecoder.__init__(self, object_hook=self.dict_to_object,
                             *args, **kargs)
    
    def dict_to_object(self, d): 
        if '__type__' not in d:
            return d
        d=json.loads(d)
        type = d.pop('__type__')
        if type=='datetime':
            return dt.datetime(**d)
        elif type=='date':
            return dt.date(**d)
        elif type=='time':
            return dt.time(**d)
        elif type=='timedelta':
            return dt.timedelta(**d)
        else:
            d['__type__'] = type
            return d

class JSONEncoder(json.JSONEncoder):
    """ Instead of letting the default encoder convert datetime to string,
        convert datetime objects into a dict, which can be decoded by the
        DateTimeDecoder
    """
        
    def default(self, obj):
        if isinstance(obj, dt.datetime):
            return '''{{
                \\"__type__\\" : \\"datetime\\",
                \\"year\\" : {year},
                \\"month\\" : {month},
                \\"day\\" : {day},
                \\"hour\\" : {hour},
                \\"minute\\" : {minute},
                \\"second\\" : {second},
                \\"microsecond\\" : {microsecond},
            }}'''.format(year=obj.year,month=obj.month,day=obj.day,hour=obj.hour,minute=obj.minute,second=obj.second,microsecond=obj.microsecond)   
        elif isinstance(obj,dt.date):
            return '''{{
                \\"__type__\\":\\"date\\",
                \\"year\\":{year},
                \\"month\\":{month},
                \\"day\\":{day}
            }}'''.format(year=obj.year,month=obj.month,day=obj.day)
        elif isinstance(obj,dt.time):
            return '''{{
                \\"__type__\\":\\"time\\",
                \\"hour\\":{hour},
                \\"minute\\":{minute},
                \\"second\\":{second},
                \\"microsecond\\":{microsecond}
            }}'''.format(hour=obj.hour,minute=obj.minute,second=obj.second,microsecond=obj.microsecond)
        elif isinstance(obj,dt.timedelta):
            return '''{{
                \\"__type__\\":\\"timedelta\\",
                \\"days\\":{days},
                \\"seconds\\":{seconds},
                \\"microseconds\\":{microseconds}
            }}'''.format(days=obj.days,seconds=obj.seconds,microseconds=obj.microseconds)
        else:
            return json.JSONEncoder.default(self, obj)
