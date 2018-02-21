#!/usr/bin/python
import sprocket.controlling.worker.worker as worker


def lambda_handler(event, _):
    worker.worker_handler(event, None)
