import json

import pika
import time

import redis

from config import STATUS_RUNNING, KEY_STATUS, KEY_SCAN_ID, STATUS_COMPLETE, REDIS_EXPIRY_SECONDS, STATUS_ERROR, \
    QUEUE_NAME, QUEUE_HOST, REDIS_HOST, REDIS_PORT

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=QUEUE_HOST))
channel = connection.channel()

channel.queue_declare(queue=QUEUE_NAME, durable=True)
print('[Waiting for messages. To exit press CTRL+C]')


def callback(ch, method, properties, body):

    print("[START CALLBACK]")

    data_string = body.decode()
    print(f"data_string: {data_string}")

    data = json.loads(data_string)
    scan_id = data[KEY_SCAN_ID]

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    try:
        print('[RUNNING]')
        data[KEY_STATUS] = STATUS_RUNNING
        r.set(scan_id, json.dumps(data), ex=REDIS_EXPIRY_SECONDS)

        time.sleep(2)

        print('[COMPLETE]')
        data[KEY_STATUS] = STATUS_COMPLETE
        r.set(scan_id, json.dumps(data), ex=REDIS_EXPIRY_SECONDS)

    except Exception as e:
        data[KEY_STATUS] = STATUS_ERROR
        r.set(scan_id, json.dumps(data), ex=REDIS_EXPIRY_SECONDS)
        print(f"[ERROR]: {e}")

    print("[ACK]")
    ch.basic_ack(delivery_tag=method.delivery_tag)

    print("[END CALLBACK]")


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=callback)

channel.start_consuming()
