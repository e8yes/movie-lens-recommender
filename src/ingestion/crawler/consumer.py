import logging
from kafka import KafkaConsumer
from time import sleep
from typing import Any

from src.ingestion.database.common import CreateConnection


class XmdbEntryHandlerInterface:
    """Declares what functionalities each handler must provide.
    """

    def __init__(self) -> None:
        pass

    def Topic(self) -> str:
        """The Kafka queue it manages.
        """
        return str()

    def EntryDeserializer(self, x: str) -> Any:
        """Defines how each entry from the Kafka queue is deserialized.
        The deserialized object will later be sent to the 
        XmdbEntryHandlerInterface.ProcessEntry() function.

        Args:
            x (str): The entry (in bytes) to be deserialized.

        Returns:
            Any: The desired deserialized object.
        """
        return None

    def ProcessEntry(self, entry: Any, pg_conn: Any) -> str:
        """Defines how the specified entry is to be processed.

        Args:
            entry (Any): A deserialized entry which need to be processed.
            pg_conn (pyscopg2.connection): A DB connection which connects to
                the postgres ingestion database server.

        Returns:
            str: A representative string of the processed entry, for logging
                purposes.
        """
        return str()


class XmdbEntryConsumer:
    """It consumes and processes XMDB messages from the Kafka queues.
    """

    def __init__(self,
                 kafka_host: str,
                 postgres_host: str,
                 postgres_password: str,
                 handler: XmdbEntryHandlerInterface) -> None:
        """Constructs a consumer with the specified entry handler.

        Args:
            kafka_host (str): The host address (with port number) which points
                to the Kafka XMDB topics server
            postgres_host (str): The IP address which points to the postgres
                ingestion database server
            postgres_password (str): The password of the postgres user.
            handler (XmdbEntryHandlerInterface): See above.
        """
        self.kafka_host = kafka_host
        self.postgres_host = postgres_host
        self.postgres_password = postgres_password
        self.handler = handler

    def __Consume(self) -> None:
        consumer = KafkaConsumer(
            self.handler.Topic(),
            bootstrap_servers=[self.kafka_host],
            enable_auto_commit=True,
            value_deserializer=self.handler.EntryDeserializer)

        pg_conn = CreateConnection(host=self.postgres_host,
                                   password=self.postgres_password)

        for message in consumer:
            entry = message.value
            repr = self.handler.ProcessEntry(entry=entry, pg_conn=pg_conn)

            # TODO: Implement proper rate limiting.
            logging.info("XmdbEntryConsumer.__Consume() repr={0}".format(repr))
            sleep(1)

    def Run(self) -> None:
        """Polls and consumes from the Kafka queue. This function blocks and
        never returns, unless it's interrupted.
        """
        logging.basicConfig(
            format='%(asctime)s %(levelname)-8s %(message)s',
            level=logging.INFO,
            datefmt='%Y-%m-%d %H:%M:%S')

        while True:
            try:
                self.__Consume()
            except KeyboardInterrupt:
                return
            except:
                pass

            sleep(1)
