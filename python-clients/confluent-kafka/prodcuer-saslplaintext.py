#
# Example Kafka Producer using confluent-kafka python library.
# Reads lines from standard input 'stdin' and sends to Kafka topic.
# Send message 'stop' to stop the script

import sys
from confluent_kafka import Producer

if len(sys.argv) != 5:
    print("Usage: {} <bootstrap.servers> <topic>\n".format(sys.argv[0]))
    sys.exit(1)

bootstrap_servers = sys.argv[1]
topic = sys.argv[2]
keytab_path = sys.srgv[3]
principal = sys.srgv[4]

# Create producer instance. The minimum configuration requirement is 'bootstrap.servers'.
producer = Producer({
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.kerberos.keytab': keytab_path,
    'sasl.kerberos.principal': principal
    })

# Optional delivery callback (per-message triggered by poll() or flush())
def delivery_callback(error, message):
    if error:
        print("Message delivery failed {}".format(error))
    else:
        print("Message delivered sucessfully {}:{}:{}".format(message.topic(), message.partition(), message.offset()))

while True:
    messsage_to_send = input("Enter message to send to Kafka - ")

    # Exit condition; Send message 'stop' to stop the script
    if messsage_to_send == "stop":
        break

    try:
        print("Sending message '{}'".format(messsage_to_send))
        producer.produce(topic, messsage_to_send, callback=delivery_callback)
    except BufferError:
        # When messages are produced faster than producer can send it to kafka
        print("Prodcuer queue is full. Messages waiting for delivery - {}".format(len(producer)))
    producer.poll(0)

# Send all messages to kafka before exit
producer.flush()
