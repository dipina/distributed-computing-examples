"""Classic RabbitMQ 'hello world' producer (slide 71). Needs a running broker + pika.

    docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
    python examples/14_amqp/rabbitmq_receive.py      # terminal 1
    python examples/14_amqp/rabbitmq_send.py hola    # terminal 2

References:
    - RabbitMQ tutorial one (Python): https://www.rabbitmq.com/tutorials/tutorial-one-python
    - Pika documentation: https://pika.readthedocs.io/
"""

from __future__ import annotations

import sys

import pika


def main(text: str) -> None:
    """Publish ``text`` to the 'hello' queue through the default exchange."""
    # See: https://www.rabbitmq.com/tutorials/tutorial-one-python
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()
    channel.queue_declare(queue="hello")
    channel.basic_publish(exchange="", routing_key="hello", body=text.encode())
    print(f" [x] Sent {text!r}")
    connection.close()


if __name__ == "__main__":
    main(" ".join(sys.argv[1:]) or "Hello World!")
