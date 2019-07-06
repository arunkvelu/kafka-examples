# Example Kafka Consumer using confluent-kafka python library.
# Print consumed messages to standard output 'stdout'
# Press 'ctrl+C' to stop the script
import sys
import signal
from confluent_kafka import Consumer

if len(sys.argv) != 6:
    print("Usage: {} <bootstrap.servers> <topic>\n".format(sys.argv[0]))
    sys.exit(1)

bootstrap_servers = sys.argv[1]
topic = sys.argv[2]
consumer_group_id = sys.argv[3]
keytab_path = sys.srgv[4]
principal = sys.srgv[5]

# Creates consumer instance. The minimum configuration requirements are 'bootstrap.servers' and 'group.id'.
consumer = Consumer({
'bootstrap.servers': bootstrap_servers,
'group.id': consumer_group_id,
'security.protocol': 'SASL_PLAINTEXT',
'sasl.kerberos.keytab': keytab_path,
'sasl.kerberos.principal': principal
})

# Script below captures Ctrl+C, closes the consumer(to avoid resource leak) and stops the script
def signal_handler(signal, frame):
    print('Aborting for Ctrl+C!')
    consumer.close()
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

def print_partition_assignment(consumer, partitions):
    print("Parition assignment: {}".format(partitions))

# subscribe() method accepts list of topics (strings) to subscribe to
topics = []
topics.append(topic)
consumer.subscribe(topics, on_assign=print_partition_assignment)

# Read messages from Kafka, print to stdout
while True:
    # Maximum time to block waiting for messages; in seconds
    # default timeout, if not given, is -1 which means infinite
    message = consumer.poll(timeout=2)

    if message is None:
        continue
    if message.error():
        print("Error consuming messages from kafka - {}".format(message.error()))
    else:
        print("{}:{}:{}:{} - {}".format(message.topic(), message.partition(), message.offset(), message.key(), message.value()))
