import json

from datetime import datetime
import uuid

import redis
import pika


from fastapi import FastAPI

from config import STATUS_ACCEPTED, STATUS_NOT_FOUND, REDIS_EXPIRY_SECONDS, QUEUE_NAME, REDIS_HOST, REDIS_PORT, \
    DATETIME_FORMAT, QUEUE_HOST

app = FastAPI()


@app.get("/")
def index():
    return {"app": "ingest"}


@app.get('/status')
def get_status(scan_id: str):

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    data_string = r.get(scan_id)

    if data_string is None:
        data = None
        status = STATUS_NOT_FOUND
    else:
        data = json.loads(data_string)
        status = data['status']

    return {
        'status': status,
        'data': data
    }


@app.get("/create-scan")
def create_scan(scan_id: str = None):

    # Allow custom scan_id
    if not scan_id:
        scan_id = uuid.uuid4().hex

    current_time = datetime.utcnow()
    current_time_string = current_time.strftime(DATETIME_FORMAT)

    scan_data = {
        'scan_id': scan_id,
        'created': current_time_string,
        'status': STATUS_ACCEPTED
    }

    scan_data_string = json.dumps(scan_data)

    # Add scan data to redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.set(scan_id, scan_data_string, ex=REDIS_EXPIRY_SECONDS)

    add_to_queue(scan_data_string)

    return {"scan-id": scan_id}


def add_to_queue(message):

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=QUEUE_HOST))
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    channel.basic_publish(
        exchange='',
        routing_key=QUEUE_NAME,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
        ))

    connection.close()

