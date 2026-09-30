import os
import logging
import signal
import threading

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.agg_by_client = {}
        self.total_by_client = {}
        self.end_event = threading.Event()
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def _process_data(self, client, fruit_top_final):
        new_fruits = []
        for fruit, amount in fruit_top_final:
            new_fruits.append(fruit_item.FruitItem(fruit, int(amount)))
        
        top_client = self.total_by_client.get(client, [])
        top_client.extend(new_fruits)
        top_client.sort()
        self.total_by_client[client] = top_client[-TOP_SIZE:]

        self.agg_by_client[client] = self.agg_by_client.get(client, 0) + 1
        if self.agg_by_client[client] < AGGREGATION_AMOUNT:
            return
        final_top = self.total_by_client.get(client, [])
        final_top.reverse()

        result = [
            (item.fruit, item.amount)
            for item in final_top
        ]
        self.output_queue.send(message_protocol.internal.serialize([client, result]))
        del self.total_by_client[client]
        del self.agg_by_client[client]
        
    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 2:
            self._process_data(*fields)
        else:
            nack()
            return
        ack()

    def sigterm_handler(self, signum, frame):
        if self.end_event.is_set():
            return
        self.end_event.set()
        self.input_queue.stop_consuming()
        
    def close(self):
        for mom in [self.input_queue, self.output_queue]:
            try:
                mom.close()
            except Exception as e:
                logging.error(f"Error closing message middleware: {e}")
    
    def start(self):
        try:
            self.input_queue.start_consuming(self.process_messsage)
        finally:
            self.close()

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
