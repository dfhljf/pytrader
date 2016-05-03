class Series(object):
    def __init__(self,value=None):
        if type(value)==dict:
            self.__keys__={k:i for i,k in enumerate(value.keys())}
            self.__values__=list(value.values())
        else:
            self.__keys__={}
            self.__values__=[]
    def __getitem__(self,index):
        if type(index)==int:
            return self.__values__[index]
        else:
            return self.__values__[self.__keys__[index]]
    def __len__(self):
        return len(self.__values__)
    def push(self,key,value):
        self.__keys__.update({key:len(self)})
        self.__values__.append(value)
    def isempty(self):
        return self.__values__==[]
class Dict(object):
    def __init__(self,value=None):
        if type(value)==dict:
            self.__keys__={k:i for i,k in enumerate(value.keys())}
            self.__values__=list(value.values())
        else:
            self.__keys__={}
            self.__values__=[]
    def __getattr__(self,attr):
        if attr in ['__keys__','__values__']:
            return object.__getattribute__(self,attr)
        else:
            return self.__values__[self.__keys__[attr]]
    def __setattr__(self,attr,value):
        if attr in ['__keys__','__values__']:
            object.__setattr__(self,attr,value)
        else:
            self.__values__[self.__keys__[attr]]=value
    def __getitem__(self,index):
        if type(index)==int:
            return self.__values__[index]
        else:
            return self.__values__[self.__keys__[index]]