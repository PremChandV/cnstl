# The above code is a Python script that includes various imports and defines functions related to
# working with Kafka, networking, and data processing. Here is a breakdown of the main components:
import socket
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import json
import csv
import constants as constants
import conversion
import tcp_server as tcp
from parse_request_param import proccess_request_parameters

"""
    The provided Python script sets up a TCP server that communicates with Kafka topics to process and
    respond to commands received over the network.
    
    :param admin_client: The `admin_client` is an instance of the `AdminClient` class from the
    `confluent_kafka.admin` module. It is used to interact with the Kafka cluster for administrative
    operations such as creating topics, listing topics, and other cluster metadata management tasks. In
    the provided code, the
    :param topic_name: The `topic_name` parameter is a string that represents the name of a Kafka topic.
    In the provided code snippet, the `topic_name` is used to specify the name of the Kafka topics that
    the code interacts with, such as `SRX_REQUEST_TOPIC`, `SRX_RESPONSE_TOPIC`, `
    :param num_partitions: The `num_partitions` parameter in Kafka refers to the number of partitions to
    create for a topic. Partitions allow you to parallelize the data and distribute it across multiple
    brokers in a Kafka cluster. Having multiple partitions for a topic enables greater throughput and
    scalability
    :param replication_factor: The `replication_factor` parameter in Apache Kafka refers to the number
    of replicas for each partition in a topic. Replication ensures fault tolerance by creating multiple
    copies of the data across different brokers in the Kafka cluster. This parameter specifies how many
    copies of each partition should be maintained
"""

# The lines you provided are setting up various constants in the Python script for configuring Kafka
# topics and server settings:
SRX_REQUEST_TOPIC = "srx.drs.request.test"
SRX_RESPONSE_TOPIC = "srx.drs.response.test"
SRX_UI_REQUEST_TOPIC = "srx.drs.ui.req.param"
SRX_UI_RESPONSE_TOPIC = "srx.drs.ui.res.param"
BOOTSTRAP_SERVER = "127.0.0.1:9092"
GROUP_ID = "SRX"


def create_topic_if_not_exists(
    admin_client, topic_name, num_partitions, replication_factor
):
    """
    The function `create_topic_if_not_exists` checks if a topic exists and creates it with specified
    partitions and replication factor if it does not already exist.

    :param admin_client: The `admin_client` parameter is an object that represents an administrative
    client used to interact with the Apache Kafka cluster. It is typically used to perform
    administrative operations such as creating topics, listing topics, configuring brokers, etc
    :param topic_name: The `topic_name` parameter is the name of the topic that you want to create if it
    does not already exist in the Kafka cluster
    :param num_partitions: The `num_partitions` parameter in the `create_topic_if_not_exists` function
    refers to the number of partitions that the topic should have. Partitions in Apache Kafka allow you
    to parallelize the data in a topic by splitting it into multiple partitions. Each partition can be
    hosted on a different broker,
    :param replication_factor: The `replication_factor` parameter in the `create_topic_if_not_exists`
    function refers to the number of replicas for each partition in the topic. Replication factor
    determines how many copies of each partition will be created and distributed across brokers in the
    Kafka cluster for fault tolerance and high availability. It ensures
    """
    # Check if topic exists
    topics = admin_client.list_topics(timeout=10).topics
    if topic_name not in topics:
        # Create topic
        new_topic = NewTopic(topic_name, num_partitions, replication_factor)
        fs = admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Topic {topic} created")
            except Exception as e:
                print(f"Failed to create topic {topic} : {e}")
    else:
        print(f"Topic {topic_name} already exists.")

def server_start(ip, port):
    # The above Python code is initializing a Kafka Admin Client and then creating topics if they do
    # not already exist. It is creating topics for SRX_REQUEST_TOPIC, SRX_RESPONSE_TOPIC,
    # SRX_UI_REQUEST_TOPIC, and SRX_UI_RESPONSE_TOPIC with a replication factor of 1 and a partition
    # count of 1. This code is setting up the necessary Kafka topics for communication and data
    # storage within a Kafka cluster.
    # Initialize Kafka Admin Client
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVER})

    # Create topics if they do not exist
    create_topic_if_not_exists(admin_client, SRX_REQUEST_TOPIC, 1, 1)
    create_topic_if_not_exists(admin_client, SRX_RESPONSE_TOPIC, 1, 1)
    # added topic for store data
    create_topic_if_not_exists(admin_client, SRX_UI_REQUEST_TOPIC, 1, 1)
    create_topic_if_not_exists(admin_client, SRX_UI_RESPONSE_TOPIC, 1, 1)

    # Configure Kafka Producer
    # The above code is creating a producer configuration dictionary with a key "bootstrap.servers"
    # and a value BOOTSTRAP_SERVER. It then creates a producer object using the Producer class with
    # the provided configuration.
    producer_config = {
        "bootstrap.servers": BOOTSTRAP_SERVER,
    }
    producer = Producer(producer_config)

    # The above code is configuring a Kafka consumer in Python. It sets up the consumer with the
    # specified configuration parameters such as the bootstrap servers, group id, auto commit
    # settings, offset reset behavior, and maximum poll interval. It then subscribes the consumer to a
    # specific topic named SRX_RESPONSE_TOPIC.
    # Configure Kafka Consumer
    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVER,
        "group.id": GROUP_ID,
        "enable.auto.commit": True,
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 6000000,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([SRX_RESPONSE_TOPIC])

    def handle_client_connection(connection):
        # The above Python code snippet is attempting to continuously receive data from a connection
        # in chunks of 4096 bytes. For each chunk of data received, it logs the data using the
        # `create_log` function and then processes the data using the `process_sequence_command`
        # function with the `producer` argument. The loop continues until no more data is received
        # (when `data` is empty).
        try:
            while True:
                data = connection.recv(4096)
                create_log(f"{data}")
                if not data:
                    break

                process_sequence_command(data, producer)

                # The above code snippet is a Python script that is waiting for a response from a
                # Kafka consumer. It continuously polls for messages from the consumer, checks for any
                # errors, and processes the received message. If there are no messages, it continues
                # to poll. If there is an error, it checks if it is the end of the partition or
                # another type of error. If it is the end of the partition, it continues. If it is
                # another type of error, it prints the error message and breaks out of the loop.
                # Finally, it processes the received message, commits the offset, and prepares the
                # response
                # Wait for a resposnse from Kafka consumer
                while True:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            # End of partition
                            continue
                        else:
                            print(f"Consumer error : {msg.error}")
                            break
                    response = msg.value()
                    consumer.commit()
                    hexa_response = prepare_reponse(response)
                    # The above code snippet is checking if the variable `hexa_response` is an
                    # instance of bytes. If it is not, it encodes `hexa_response` into bytes. Then, if
                    # `hexa_response` is not empty, it sends the encoded `hexa_response` over a
                    # connection and breaks out of the loop.
                    if isinstance(hexa_response, bytes):
                        hexa_response = hexa_response
                    else:
                        hexa_response = hexa_response.encode()
                    if hexa_response:
                        connection.sendall(hexa_response)
                        break
        # The above code is a Python exception handling block. It is trying to catch specific
        # exceptions - `ConnectionResetError` and any other general `Exception`.
        except ConnectionResetError as e:
            print("Connection reset by peer {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            connection.close()

    # The above Python code is creating a server socket using the `socket` module. It binds the server
    # socket to a specific IP address and port number specified by the variables `ip` and `port`. It
    # then prints a message indicating that the server is listening on the specified IP address and
    # port. Finally, it starts listening for incoming connections with a backlog of 1 connection.
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((ip, port))
    print(f"Server listening on {ip}:{port}")
    server_socket.listen(1)

    # The above Python code is setting up a server that continuously accepts incoming connections from
    # clients. It uses a `while True` loop to keep accepting connections, prints a message when a
    # client connects, and then calls a function `handle_client_connection` to handle the client's
    # requests. The server will continue running until a KeyboardInterrupt (Ctrl+C) is received, at
    # which point it will print "Server shutting down." and close the server socket.
    try:
        while True:
            connection, client_address = server_socket.accept()
            print(f"Connection from {client_address}")
            handle_client_connection(connection)
    except KeyboardInterrupt:
        print("Server shutting down.")
    finally:
        server_socket.close()


def process_sequence_command(command, producer):
    """
    proccessing multiple sequences commands...
    """
    # The code snippet provided is written in Python. It defines a delimiter `b"\xaa\xab\xba\xbb"`
    # which is a byte sequence used to split a string `command` into segments using the `split()`
    # method. The code then initializes an empty list `results` and a counter `i` to 0.
    delimiter = b"\xaa\xab\xba\xbb"
    segments = command.split(delimiter)
    # print(f"segments : {segments}")
    results = []
    i = 0
    # The above Python code is iterating over a list of segments starting from the second element
    # (index 1). For each segment, it processes the segment data by adding a delimiter, extracting
    # command_id, group_id, and requested_param_data using the `process_command` function. It then
    # creates a dictionary `req_command` with keys "command_id" and "group_id" and prints and converts
    # it to a JSON string using `json.dumps`.
    for segment in segments[1:]:
        i = i + 1
        print(f"Segment : {i}")
        if segment:
            segment_data = delimiter + segment
            command_id, group_id, requested_param_data = process_command(segment_data)
            req_command = {"command_id": command_id, "group_id": group_id}
            print(req_command)
            req_command = json.dumps(req_command)
            print(req_command)

            producer.produce(SRX_REQUEST_TOPIC, key=None, value=req_command)
            producer.flush()

            # Added DRS UI Data Producer
            producer.produce(SRX_UI_REQUEST_TOPIC, key=None, value=requested_param_data)
            producer.flush()

def process_command(command):
    """
    The function `process_command` parses a command, processes request parameters, and prints the
    requested data if the command is not equal to a specific value.
    
    :param command: b"S82S41S61S50S37FFFS93S10000M"
    :return: The function `process_command` is returning the `command_id`, `group_id`, and
    `request_data`.
    """
    group_id, command_id, decoded_request_parameters = parse_command(command)
    if command != b"S82S41S61S50S37FFFS93S10000M":
        request_data = proccess_request_parameters(
            command_id, group_id, decoded_request_parameters
        )
        print("requested_data", request_data)
    return command_id, group_id, request_data


def parse_command(data):
    # Assuming data is received as a bytes object
    # print(command)
    # Example: b"\xaa\xab\xba\xbb\x00\x00\x00\x00\x00d\x00\x1f\xcc\xcd\xdc\xdd\xaa\xab\xba\xbb\x00\x00\x00\x00\x00d\x00\x1f\xcc\xcd\xdc\xdd"
    # command = command.split(b"\xaa\xab\xba\xbb")
    # Define lengths of each segment in bytes
    # print("TESTST DATS AFTER SLIP ..", command)

    # The above code in Python is defining constants for the lengths of different parts of a message
    # structure. It specifies the length of the header, message size, command ID, group ID, and footer
    # in bytes. These constants can be used in a program to ensure consistency and readability when
    # working with messages of this structure.
    HEADER_LENGTH = 4
    MESSAGE_SIZE_LENGTH = 4
    COMMAND_ID_LENGTH = 2
    GROUP_ID_LENGTH = 2
    FOOTER_LENGTH = 4

    # The above code is extracting different parts of a data packet based on the defined lengths. It
    # extracts the header, message size, and command ID from the data packet using slicing operations.
    # The `header` is extracted from the beginning of the data up to the `HEADER_LENGTH`. The
    # `message_size` is extracted from the position after the header up to `MESSAGE_SIZE_LENGTH`
    # bytes. The `command_id` is extracted from the position after the message size up to
    # `COMMAND_ID_LENGTH` bytes.
    # Extract each part based on the defined lengths
    header = data[:HEADER_LENGTH]
    # header = b"\xaa\xab\xba\xbb"
    message_size = data[HEADER_LENGTH : HEADER_LENGTH + MESSAGE_SIZE_LENGTH]
    command_id = data[
        HEADER_LENGTH
        + MESSAGE_SIZE_LENGTH : HEADER_LENGTH
        + MESSAGE_SIZE_LENGTH
        + COMMAND_ID_LENGTH
    ]
    # The above code is extracting the `group_id` from a `data` variable. It is calculating the
    # starting index of the `group_id` within the `data` variable by adding the lengths of the header,
    # message size, command ID, and group ID. It then uses this calculated index to extract the
    # `group_id` from the `data` variable.
    group_id = data[
        HEADER_LENGTH
        + MESSAGE_SIZE_LENGTH
        + COMMAND_ID_LENGTH : HEADER_LENGTH
        + MESSAGE_SIZE_LENGTH
        + COMMAND_ID_LENGTH
        + GROUP_ID_LENGTH
    ]
    # The code snippet is extracting two parts of a data string. 
    # 1. The `footer` variable is extracting the last `FOOTER_LENGTH` characters from the `data`
    # string.
    # 2. The `request_parameters` variable is extracting a portion of the `data` string starting from
    # the sum of `HEADER_LENGTH`, `MESSAGE_SIZE_LENGTH`, `COMMAND_ID_LENGTH`, and `GROUP_ID_LENGTH`,
    # up to the last `FOOTER_LENGTH` characters.
    # The code snippet is extracting two parts of a data string.
    footer = data[-FOOTER_LENGTH:]
    request_parameters = data[
        HEADER_LENGTH
        + MESSAGE_SIZE_LENGTH
        + COMMAND_ID_LENGTH
        + GROUP_ID_LENGTH : -FOOTER_LENGTH
    ]

    # The above Python code is decoding various parts of a message or data structure. Here's a
    # breakdown of each step:
    # Decode parts if needed (assuming they're not in plain text, you can convert them to hex or other formats)
    decoded_header = header.hex()
    decoded_message_size = int.from_bytes(message_size, byteorder="big")
    decoded_command_id_hex = command_id.hex()
    decoded_command_id = int(decoded_command_id_hex, 16)
    decoded_group_id_hex = group_id.hex()
    decoded_group_id = int(decoded_group_id_hex, 16)
    decoded_footer = footer.hex()
    decoded_request_parameters = request_parameters.hex()

    # Print decoded parts for debugging
    # print(
    #     f"Decoded Data: header={decoded_header}, message_size={decoded_message_size}, command_id={decoded_command_id}, group_id={decoded_group_id}, request_parameters={decoded_request_parameters}, footer={decoded_footer}"
    # )

    return decoded_command_id, decoded_group_id, decoded_request_parameters


def prepare_reponse(response):
    # The above Python code is trying to parse a JSON response and extract specific data from it. It
    # first loads the JSON response using `json.loads(response)`, then extracts values for keys
    # "response_size", "response_unit_id", "response_group_id", and "response_data" from the parsed
    # JSON data. Finally, it prints out these extracted values along with their corresponding keys.
    try:
        response_data = json.loads(response)
        print("Consume Data :", response_data)
        response_size = response_data["response_size"]
        response_unit_id = response_data["response_unit_id"]
        response_group_id = response_data["response_group_id"]
        response_data = response_data["response_data"]
        print(
            "response_size",
            response_size,
            "response_group_id",
            response_group_id,
            "response_unit_id",
            response_unit_id,
            "response_data",
            response_data,
        )
        # The above Python code snippet is assigning values to the variables `response_header` and
        # `response_status`.
        response_header = constants.RESPONSE_HEADER
        response_status = conversion.to_bytes_and_hex(
            0, constants.RESPONSE_MSG_STATUS_SIZE
        )
        # The code snippet is converting the `response_group_id` variable to bytes and then to a
        # hexadecimal representation. It first strips any leading or trailing whitespace from the
        # `response_group_id` variable, converts it to an integer, and then converts it to bytes with
        # a specific size defined by `constants.RESPONSE_GROUP_ID_SIZE`. Finally, it converts the
        # bytes to a hexadecimal representation using the `conversion.to_bytes_and_hex` function.
        # response_size = response_structure["response_size"]
        response_group_id = conversion.to_bytes_and_hex(
            int(response_group_id.strip()),
            constants.RESPONSE_GROUP_ID_SIZE,
        )
        # The above code is converting the `response_unit_id` to bytes and then to a hexadecimal
        # representation. It first strips any leading or trailing whitespace from the
        # `response_unit_id`, converts it to an integer, and then converts it to bytes with a specific
        # size defined by `constants.RESPONSE_UNIT_ID_SIZE`. Finally, it converts the bytes to a
        # hexadecimal representation.
        response_unit_id = conversion.to_bytes_and_hex(
            int(response_unit_id.strip()),
            constants.RESPONSE_UNIT_ID_SIZE,
        )
        # The above Python code snippet is taking the value of `constants.RESPONSE_FOOTER`, converting
        # `response_size` to an integer, and then converting the integer `response_size` to bytes and
        # hexadecimal format using the `conversion.to_bytes_and_hex` function with a specified size of
        # `constants.RESPONSE_MSG_SIZE`.
        response_footer = constants.RESPONSE_FOOTER
        response_size = int(response_size)
        response_size = conversion.to_bytes_and_hex(
            response_size, constants.RESPONSE_MSG_SIZE
        )
        # The above Python code snippet is attempting to create a log message and format a string
        # based on the values of `response_header`, `response_status`, `response_size`,
        # `response_group_id`, `response_unit_id`, `response_data`, and `response_footer`. The
        # formatted string is then converted to bytes using `bytes.fromhex()` after replacing any
        # occurrences of `r"\x"` in the final string. If an exception occurs during this process, the
        # code will return an error message indicating the issue encountered while querying the
        # database.
        # create_log((f"AFTER EXISTING COMMAND ID  {response_unit_id} AND GROUP_ID {response_group_id} AND response_status {response_status} AND response_size {response_size}"))
        final_string = f"{response_header}{response_status}{response_size}{response_group_id}{response_unit_id}{response_data}{response_footer}"
        formatted_string = bytes.fromhex(final_string.replace(r"\x", ""))
        return formatted_string
    except Exception as e:
        return f"Error querying database: {e}".encode()

def create_log(message):
    """
    The function `create_log` writes a message to a CSV file for logging purposes.
    
    :param message: The `create_log` function takes a `message` parameter, which is a string containing
    the error message that you want to log in a CSV file. The function appends this message to a CSV
    file named "cmd_errors.csv" along with a timestamp
    """
    file_path = "./cmd_errors.csv"
    with open(file_path, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([message])


# The above Python code is checking if the script is being run as the main program using the `if
# __name__ == "__main__":` condition. It then sets the IP address and port number variables.
if __name__ == "__main__":
    ip = "127.0.0.1"
    port = 5485
    if constants.IS_RANDOM:
        tcp.start_tcp_server(ip, port)
    else:
        server_start(ip, port)
