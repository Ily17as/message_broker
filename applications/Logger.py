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

    # TO WRITE finish ********************************************************

    log_entry = f"[{timestamp}] {sender} : {message_type} : {number}\n" # you can edit
    with open(LOG_FILE, 'a') as f:
        f.write(log_entry)
    print(log_entry.strip())


def main():
    setup_logging()
    # TO WRITE start *********************************************************

    # TO WRITE finish ********************************************************


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(' [*] Logger stopped')
