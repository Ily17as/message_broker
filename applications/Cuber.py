import pika
import sys
import json

from Globals import RMQ_HOST, RMQ_USER, RMQ_PASS, EXCHANGE_NAME


def main():
    # TO WRITE start *********************************************************
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=RMQ_HOST,
        credentials=pika.PlainCredentials(RMQ_USER, RMQ_PASS)
    ))
    channel = connection.channel()

    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type='fanout',
        durable=True
    )
    channel.queue_declare(queue='cubed', durable=True)

    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name)

    def callback(ch, method, properties, body):
        message = json.loads(body)
        number = message['number']
        squared = number ** 3
        message['type'] = 'cubed'
        message['number'] = squared
        channel.basic_publish(
            exchange="",
            routing_key='cubed',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )
        print(f'{message["sender"]} : cubed : {squared}')
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=False
    )

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
    # TO WRITE finish ********************************************************
    pass

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(' [x] Cuber stopped')
        sys.exit(0)
