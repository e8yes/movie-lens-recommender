from pyspark.sql import SparkSession
from typing import List

from src.ingestion.database.reader import IngestionReaderInterface
from src.ingestion.database.reader_cql import ConfigureCassandraSparkSession
from src.ingestion.database.reader_cql import CassandraIngestionReader
from src.ingestion.database.reader_psql import ConfigurePostgresSparkSession
from src.ingestion.database.reader_psql import PostgresIngestionReader
from src.ingestion.database.writer import IngestionWriterInterface
from src.ingestion.database.writer_cql import CassandraIngestionWriter
from src.ingestion.database.writer_psql import PostgresIngestionWriter


class IngestionReaderFactory:
    """It configures and creates an ingestion reader with its implementation
    chosen based on the arguments supplied.
    """

    def __init__(self,
                 cassandra_contact_points: List[str],
                 postgres_host: str,
                 postgres_user: str,
                 postgres_password: str) -> None:
        """Supplies the combination of parameters to the factory for it to
        choose the configuration and reader implementation.

        In particular, you should specify the list of Cassandra contact points
        if a Cassandra based implmentation is desired. Otherwise, specifies
        the postgres host, user and password when a PostgreSQL based
        implementation is desired.

        Args:
            cassandra_contact_points (List[str]): The list of contact points
                to try connecting for cluster discovery. A contact point can
                be a string (ip or hostname), a tuple (ip/hostname, port) or a
                :class:`.connection.EndPoint` instance.
            postgres_host (str): The IP address (with port number) which
                points to the postgres server.
            postgres_user (str): The postgres user to use while accessing the
                ingestion database.
            postgres_password (str): The password of the postgres user.
        """
        self.cassandra_contact_points = cassandra_contact_points
        self.postgres_host = postgres_host
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password

        if cassandra_contact_points is not None:
            self.implementation = "Cassandra"

        if postgres_host is not None and \
            postgres_user is not None and \
                postgres_password is not None:
            self.implementation = "PostgreSQL"

        if self.implementation is None:
            raise "No suitable implementation found."

    def ConfigureSparkSession(
            self, spark_builder: SparkSession.Builder) -> SparkSession.Builder:
        if self.implementation == "Cassandra":
            return ConfigureCassandraSparkSession(
                contact_points=self.cassandra_contact_points,
                builder=spark_builder)
        elif self.implementation == "PostgreSQL":
            return ConfigurePostgresSparkSession(builder=spark_builder)

    def Create(self, spark: SparkSession) -> IngestionReaderInterface:
        """Creates an ingestion reader.

        Args:
            spark (SparkSession): A spark session configured with
                IngestionReaderFactory.ConfigureSparkSession().

        Returns:
            IngestionReaderInterface: An ingestion reader.
        """
        if self.implementation == "Cassandra":
            return CassandraIngestionReader(
                contact_points=self.cassandra_contact_points,
                spark=spark)
        elif self.implementation == "PostgreSQL":
            return PostgresIngestionReader(
                db_host=self.postgres_host,
                db_user=self.postgres_user,
                db_password=self.postgres_password,
                spark=spark)


class IngestionWriterFactory:
    """It creates an ingestion writer with its implementation chosen based on
    the arguments supplied.
    """

    def __init__(self,
                 cassandra_contact_points: List[str],
                 postgres_host: str,
                 postgres_password: str) -> None:
        """Supplies the combination of parameters to the factory for it to
        choose the write implementation.

        In particular, you should specify the list of Cassandra contact points
        if a Cassandra based implmentation is desired. Otherwise, specifies
        the postgres host and password when a PostgreSQL based implementation
        is desired.

        Args:
            cassandra_contact_points (List[str]): The list of contact points
                to try connecting for cluster discovery. A contact point can
                be a string (ip or hostname), a tuple (ip/hostname, port) or a
                :class:`.connection.EndPoint` instance.
            postgres_host (str): The IP address (with port number) which
                points to the postgres server.
            postgres_password (str): The password of the "postgres" user.
        """
        self.cassandra_contact_points = cassandra_contact_points
        self.postgres_host = postgres_host
        self.postgres_password = postgres_password

        if cassandra_contact_points is not None:
            self.implementation = "Cassandra"

        if postgres_host is not None and \
                postgres_password is not None:
            self.implementation = "PostgreSQL"

        if self.implementation is None:
            raise "No suitable implementation found."

    def Create(self) -> IngestionWriterInterface:
        """Creates an ingestion writer.

        Returns:
            IngestionWriterInterface: an ingestion writer.
        """
        if self.implementation == "Cassandra":
            return CassandraIngestionWriter(
                contact_points=self.cassandra_contact_points)
        elif self.implementation == "PostgreSQL":
            return PostgresIngestionWriter(
                host=self.postgres_host,
                password=self.postgres_password)
