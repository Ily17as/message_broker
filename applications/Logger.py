import pika
from datetime import datetime
import json
from Globals import RMQ_HOST, RMQ_USER, RMQ_PASS, EXCHANGE_NAME

LOG_FILE = 'rabbitmq_messages.log'


def setup_logging():
    with open(LOG_FILE, 'w') as f:
        f.write(f"\n\n=== Log started at {datetime.now().isoformat()} ===\n")


def log_message(body):
    # TO WRITE start *********************************************************
    data = json.loads(body.decode())
    timestamp = datetime.now().isoformat()
    sender = data['sender']
    message_type = data['type']
    number = data['number']
    # TO WRITE finish ********************************************************

    log_entry = f"[{timestamp}] {sender} : {message_type} : {number}\n" # you can edit
    with open(LOG_FILE, 'a') as f:
        f.write(log_entry)
    print(log_entry.strip())


def main():
    setup_logging()
    # TO WRITE start *********************************************************
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=RMQ_HOST,
        credentials=pika.PlainCredentials(RMQ_USER, RMQ_PASS)
    ))
    channel = connection.channel()

    for queue_name in ['squared', 'cubed']:
        channel.queue_declare(queue=queue_name, durable=True)
        channel.basic_consume(queue=queue_name,
                                on_message_callback=lambda ch, method, properties, body: [log_message(body), ch.basic_ack(
                                delivery_tag=method.delivery_tag)]
                              )

    result = channel.queue_declare(queue='', exclusive=True)
    temp_queue = result.method.queue
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='fanout', durable=True)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=temp_queue)

    def original_callback(ch, method, properties, body):
        log_message(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    channel.basic_consume(queue=temp_queue, on_message_callback=original_callback)

    print(" [*] Logger is running. To exit press CTRL+C")
    channel.start_consuming()

    # TO WRITE finish ********************************************************


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(' [*] Logger stopped')
