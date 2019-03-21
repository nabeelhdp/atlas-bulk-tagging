#!/usr/bin/python

def tagfail(failfile,offset):
 
    with open('failfile') as f:
      data=json.load(f)

    print(data.keys())

 
