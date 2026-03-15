"""
Producer simulator for delivery tracking.
Sends messages with key=driver_id and value=json to topic 'driver-locations'.

Usage:
  - install requirements in producer/requirements.txt
  - configure BROKERS variable if needed
  - run: python producer_simulator.py
"""
import time
import json
import random
import argparse
from confluent_kafka import Producer

BROKERS = 'localhost:29092,localhost:29093,localhost:29094'
TOPIC = 'driver-locations'


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Delivered record to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def make_producer(brokers=BROKERS):
    conf = {
        'bootstrap.servers': brokers,
        'acks': 'all',
        'enable.idempotence': True,
        'retries': 5,
        'linger.ms': 5,
        'compression.type': 'lz4',
    }
    return Producer(conf)


def simulate(producer, num_drivers=10, interval=1.0, count=None):
    status_choices = ['idle', 'delivering', 'available']
    sent = 0
    try:
        while True:
            if count is not None and sent >= count:
                break
            driver_id = random.randint(1, num_drivers)
            payload = {
                'driver_id': driver_id,
                'latitude': round(random.uniform(-23.7, -23.5), 6),
                'longitude': round(random.uniform(-46.7, -46.5), 6),
                'timestamp': int(time.time() * 1000),
                'status': random.choice(status_choices)
            }
            
            # Formatação String Pura (Compatível com StringConverter do Kafka Connect)
            key = str(driver_id).encode('utf-8')
            value = json.dumps(payload).encode('utf-8')
            
            # Use poll to handle delivery callbacks efficiently before producing next message
            producer.poll(0)
            producer.produce(TOPIC, key=key, value=value, callback=delivery_report)
            
            sent += 1
            
            # Use poll as sleep, handles callbacks while waiting
            # 1.0 interval means we wait up to 1 second handling events
            producer.poll(interval)
            
    except KeyboardInterrupt:
        print('\nInterrupted, flushing...')
    finally:
        producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--drivers', type=int, default=50, help='number of drivers to simulate')
    parser.add_argument('--interval', type=float, default=0.2, help='seconds between events')
    parser.add_argument('--brokers', type=str, default=BROKERS, help='bootstrap brokers')
    parser.add_argument('--count', type=int, default=None, help='number of events to send (default: unlimited)')
    args = parser.parse_args()

    p = make_producer(brokers=args.brokers)
    print(f"Starting simulator: drivers={args.drivers} interval={args.interval}s brokers={args.brokers} count={args.count}")
    simulate(p, num_drivers=args.drivers, interval=args.interval, count=args.count)
