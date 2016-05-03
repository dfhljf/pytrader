from Dict import *;

def main():
    x=Dict({(0,'a'):'b',(1,'c'):'d'})
    x.a='1'
    print(x[0])
    print(x['a'])
    y=x+{'k':'x'}
    z={'k':'x'}+x
    x+={'k':'x'}
    for (k,v) in x.items():
        print('{0},{1}'.format(k,v))
    
    
    pass
if __name__ == '__main__':
    main()