import argparse
import logging
from src.ingestion.crawler.populate import PopulateXmdbEntries

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Takes unpopulated IMDB/TMDB entries and pumps them into the associated Kafka queue, so the entires can get populated by the crawler, which consumes the queue.")
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
        "--xmdb_type", type=str,
        help="The type of XMDB to be populated. Value can be chosen from {imdb, tmdb}")

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
    if args.xmdb_type is None:
        print("xmdb_type is required.")
        exit(-1)

    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

    entry_count = PopulateXmdbEntries(
        postgres_host=args.postgres_host,
        postgres_password=args.postgres_password, kafka_host=args.kafka_host,
        x=args.xmdb_type)
    logging.info("main_xmdb_populator: {0} entries have been scheduled to be populated.".
                 format(entry_count))
