import asyncio
from dataclasses import dataclass, field
import json
import random

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker

# exercise2.6.solution.
# Retrieving Data from Kafka - Key Points
# Build a Kafka Consumer Refer: 
# https://knowledge.udacity.com/questions/86800
# https://github.com/confluentinc/examples/blob/6.2.0-post/clients/cloud/python/consumer.py
# 

BROKER_URL = "PLAINTEXT://localhost:9092"


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])

    while True:
    message = c.poll(1.0)
    if message is None:
          print("no message received by consumer")
    elif message.error() is not None:
          print(f"error from consumer {message.error()}")
    else:
          print(f"consumed message {message.key()}: {message.value()}")
            
    await asyncio.sleep(0.01)

# add topic from kaka_server.py
def run_consumer():
    try:
        asyncio.run(consume('police_department_calls_3'))
        
    except KeyboardInterrupt as e:
        print("Shutting down...")
    
    
if __name__ == "__main__":
    main()
