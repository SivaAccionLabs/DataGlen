#!/usr/bin/env python
# pylint: disable=invalid-name

"""
Name   :    producer.py
Purpose:    Generates sample data for spark structured streaming app
Author :    Siva
Created:    07/03/2019
"""

import sys
import getopt
import random
import time
import json
from time import gmtime, strftime

from kafka import KafkaProducer
KAFKA_BROKERLIST = "localhost:9092"

# Kafka topic
TOPIC = "dataglen"

def CURRENT_TIME_MILLIS():
    return int(round(time.time() * 1000))

def run(brokers):
    '''
    Produce sample data to the kafka topic
    '''
    producer = KafkaProducer(bootstrap_servers=[brokers])
    while True:
        rand_key = random.randint(0, 5)
        rand_value = random.randint(0, 100)
        data = {
            "key": "key" + str(rand_key),
            "value": str(rand_value),
            "timestamp": strftime("%Y-%m-%dT%H:%M:%S.000Z", gmtime())
        }
        sample_data = json.dumps(data)
        producer.send(TOPIC, sample_data)
        time.sleep(1)


if __name__ == '__main__':
    broker_list = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hb:", ["brokerlist="])
    except getopt.GetoptError:
        print 'producer.py [-b localhost:9092] '
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'producer.py [--brokerlist broker:port [,broker:port]]'
            sys.exit()
        elif opt in ("-b", "--brokerlist"):
            print "brokerlist: %s" % arg
            broker_list = arg

    run(broker_list if broker_list is not None else KAFKA_BROKERLIST)
