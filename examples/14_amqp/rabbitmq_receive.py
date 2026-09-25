"""Classic RabbitMQ 'hello world' consumer (slide 71). Stop with CTRL+C.

References:
    - RabbitMQ tutorial one (Python): https://www.rabbitmq.com/tutorials/tutorial-one-python
    - Pika documentation: https://pika.readthedocs.io/
"""

from __future__ import annotations

import pika
from pika.adapters.blocking_connection import BlockingChannel


def on_message(ch: BlockingChannel, method: object, properties: object, body: bytes) -> None:
    """Callback invoked by pika for every delivered message."""
    print(f" [x] Received {body.decode()!r}")


def main() -> None:
    """Consume forever from the 'hello' queue."""
    # See: https://www.rabbitmq.com/tutorials/tutorial-one-python
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()
    channel.queue_declare(queue="hello")
    channel.basic_consume(queue="hello", on_message_callback=on_message, auto_ack=True)
    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    main()
