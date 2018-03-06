#!/bin/usr/python
import sys
import json
import sprocket.controlling.worker.worker as worker

def main(eventfile):
    with open(eventfile, 'r') as f:
        event = json.load(f) 
        worker.worker_handler(event, None) 

if __name__ == '__main__':
    main(sys.argv[1])

