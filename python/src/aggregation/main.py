import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_client = {}
        self.sums_by_client = {}

    def _process_data(self, client, fruit, amount):
        logging.info("Processing data message")
        fruit_top = self.fruit_top_by_client.get(client, [])
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                fruit_top[i] = fruit_top[i] + fruit_item.FruitItem(
                    fruit, amount
                )
                updated_fruit = fruit_top.pop(i)
                bisect.insort(fruit_top, updated_fruit)
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))
        self.fruit_top_by_client[client] = fruit_top

    def _process_eof(self, client):
        logging.info("Received EOF")
        self.sums_by_client[client] = self.sums_by_client.get(client, 0) + 1
        if self.sums_by_client[client] < SUM_AMOUNT:
            return
        fruit_top = self.fruit_top_by_client.get(client, [])
        fruit_chunk = list(fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top_final = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        message = [client, fruit_top_final]
        self.output_queue.send(message_protocol.internal.serialize(message))
        if len(self.fruit_top_by_client.get(client, [])) != 0:
            del self.fruit_top_by_client[client]  # TODO: Revisar si esto esta bien asi
        del self.sums_by_client[client]

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
