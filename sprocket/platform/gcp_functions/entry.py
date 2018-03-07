#!/bin/usr/python
import sys
import json
import sprocket.controlling.worker.worker as worker

def main(ev):
    event = json.loads(ev) 
    worker.worker_handler(event, None) 

if __name__ == '__main__':
    print "in python main function"
    main(sys.argv[1])

