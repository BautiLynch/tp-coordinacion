import pika
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareCloseError


class MessageMiddlewareImplementation:
    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag = method.delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag = method.delivery_tag)
            
            on_message_callback(body, ack, nack)

        try:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback) 
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Error en la conexion") from e
        except Exception as e:
            raise MessageMiddlewareMessageError("Error al consumir") from e
            
    def stop_consuming(self):
        try:
            if self.connection is None or self.connection.is_closed:
                return

            self.connection.add_callback_threadsafe(
                self.channel.stop_consuming
            )

        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Error en la conexion") from e
        except Exception as e:
            raise MessageMiddlewareMessageError("Error al detener") from e
    def close(self):
        try:
            try:
                self.channel.close()
            finally:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError("Error al desconectar") from e

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareImplementation, MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = None
        self.channel = None
        self.queue_name = queue_name
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            self.channel = self.connection.channel()

            self.channel.queue_declare(queue=queue_name, durable=True)
            self.channel.basic_qos(prefetch_count=1)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Error al conectarse") from e
        except Exception as e:
            raise MessageMiddlewareMessageError("Error inicializando") from e
    
    def send(self, message):
        try:
            self.channel.basic_publish(exchange='',
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode = pika.DeliveryMode.Persistent
                ))
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Error en la conexion") from e
        except Exception as e:
            raise MessageMiddlewareMessageError("Error al enviar mensaje") from e

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareImplementation, MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = None
        self.channel = None
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.queue_name = None

        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            self.channel = self.connection.channel()

            self.channel.exchange_declare(exchange=self.exchange_name,
                            exchange_type='direct')
            queue = self.channel.queue_declare(queue='', exclusive=True)
            self.queue_name = queue.method.queue
            for routing_key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name,
                                    queue=self.queue_name,
                                    routing_key=routing_key)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Error al conectarse") from e
        except Exception as e:
            raise MessageMiddlewareMessageError("Error inicializando") from e
    
    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Error en la conexion") from e
        except Exception as e:
            raise MessageMiddlewareMessageError("Error al enviar mensaje") from e

    def send_rk(self, message, routing_key):
            try:
                self.channel.basic_publish(exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message)
            except pika.exceptions.AMQPConnectionError as e:
                raise MessageMiddlewareDisconnectedError("Error en la conexion") from e
            except Exception as e:
                raise MessageMiddlewareMessageError("Error al enviar mensaje") from e
