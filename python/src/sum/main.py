import os
import logging
import threading
import zlib
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
EOF_RK = "EOF_SUM"
class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.eof_exchange_send = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, "EOF", []
        )
        self.eof_exchange_recv = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, "EOF", [EOF_RK]
                )
        self.data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, []
        )
        self.amount_by_fruit_by_client = {}
        self.threads = []
        self.ack_by_clients = {}
        self.data_lock = threading.Lock()
        self.condvar = threading.Condition()
        self.current_client = None
        self.end_event = threading.Event()
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def _process_data(self, client, fruit, amount):
        logging.info(f"Process data")
        with self.data_lock:
            amount_by_fruit = self.amount_by_fruit_by_client.get(client, {})
            amount_by_fruit[fruit] = amount_by_fruit.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
            self.amount_by_fruit_by_client[client] = amount_by_fruit

    def _process_eof(self, client, is_gateway_message):
        logging.info(f"Broadcasting data messages")
        if is_gateway_message:
            self.eof_exchange_send.send_rk(message_protocol.internal.serialize([client, False]), EOF_RK)
            return
        amount_by_fruit = {}
        with self.condvar:
            while self.current_client == client and (not self.end_event.is_set()):
                self.condvar.wait()
        with self.data_lock:
            amount_by_fruit = self.amount_by_fruit_by_client.pop(client, {})
        for final_fruit_item in amount_by_fruit.values():
            key = message_protocol.internal.serialize(
                [client, final_fruit_item.fruit]
            )
            hash = zlib.adler32(key)
            if self.end_event.is_set():
                return
            self.send_to_agg([client, final_fruit_item.fruit, final_fruit_item.amount], hash % AGGREGATION_AMOUNT)
        if self.end_event.is_set():
            return
        self.send_to_all_agg([client])

    def process_data_messsage(self, message, ack, nack):
        with self.condvar:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                self.current_client = fields[0]
        if len(fields) == 3:
            try:
                self._process_data(*fields)
            finally:
                with self.condvar:
                    self.current_client = None
                    self.condvar.notify_all()
        elif len(fields) == 2:
            self._process_eof(*fields)
        else:
            nack()
            return
        ack()

    def send_to_agg(self, message, id):
        self.data_output_exchange.send_rk(
            message_protocol.internal.serialize(
                message
            ),
            f"{AGGREGATION_PREFIX}_{id}"
        )
    def send_to_all_agg(self, message):
        for i in range(AGGREGATION_AMOUNT):
            self.send_to_agg(message, i)
            
    def sigterm_handler(self, signum, frame):
        if self.end_event.is_set():
            return
        self.end_event.set()
        try:
            self.input_queue.stop_consuming()
        finally:
            self.eof_exchange_recv.stop_consuming()

    def close(self):
        for mom in [self.input_queue,self.eof_exchange_send,self.eof_exchange_recv, self.data_output_exchange]:
            try:
                mom.close()
            except Exception as e:
                logging.error(f"Error closing message middleware: {e}")
                                    
    def start(self):
        control_thread = threading.Thread(target=self.eof_exchange_recv.start_consuming, args=(self.process_data_messsage,))
        self.threads.append(control_thread)
        try:
            control_thread.start()
            self.input_queue.start_consuming(self.process_data_messsage)
        finally:
            self.end_event.set()
            with self.condvar:
                self.condvar.notify_all()
            if control_thread.is_alive():
                try:
                    self.eof_exchange_recv.stop_consuming()
                finally:
                    control_thread.join()
                    self.close()
            else:
                self.close()
            
def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
