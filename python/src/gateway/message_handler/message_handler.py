from common import message_protocol
import uuid

class MessageHandler:

    def __init__(self):
        self.id = str(uuid.uuid4())
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.id, True])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) <= 1 or fields[0] != self.id:
            return
        return fields[1]
