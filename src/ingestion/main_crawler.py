import argparse
from multiprocessing import Process

from src.ingestion.crawler.consumer import XmdbEntryConsumer
from src.ingestion.crawler.handler_tmdb import TmdbEntryHandler


def __Run(kafka_host: str,
          postgres_host: str,
          postgres_password: str,
          tmdb_api_key: str) -> None:
    tmdb_handler = TmdbEntryHandler(tmdb_api_key=tmdb_api_key)
    tmdb_consumer = XmdbEntryConsumer(kafka_host=kafka_host,
                                      postgres_host=postgres_host,
                                      postgres_password=postgres_password,
                                      handler=tmdb_handler)
    tmdb_process = Process(target=tmdb_consumer.Run)
    tmdb_process.start()

    try:
        tmdb_process.join()
    except KeyboardInterrupt:
        tmdb_process.terminate()
        tmdb_process.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Consumes XMDB entries and updates the corresponding ingestion tables with content pulling from external sources.")
    parser.add_argument(
        "--kafka_host", type=str,
        help="The host address (with port number) which points to the Kafka XMDB topics server.")
    parser.add_argument(
        "--postgres_host", type=str,
        help="The IP address which points to the postgres ingestion database server.")
    parser.add_argument("--postgres_password",
                        type=str,
                        help="The password of the postgres user.")
    parser.add_argument(
        "--tmdb_api_key", type=str,
        help="See https://www.themoviedb.org/documentation/api")

    args = parser.parse_args()

    if args.kafka_host is None:
        print("kafka_host is required.")
        exit(-1)
    if args.postgres_host is None:
        print("postgres_host is required.")
        exit(-1)
    if args.postgres_password is None:
        print("postgres_password is required.")
        exit(-1)
    if args.tmdb_api_key is None:
        print("tmdb_api_key is required.")
        exit(-1)

    __Run(kafka_host=args.kafka_host,
          postgres_host=args.postgres_host,
          postgres_password=args.postgres_password,
          tmdb_api_key=args.tmdb_api_key)
