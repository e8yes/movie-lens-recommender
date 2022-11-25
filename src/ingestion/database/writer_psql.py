import json
import psycopg2.extras as pge
from datetime import datetime
from psycopg2 import connect
from psycopg2 import DatabaseError
from typing import List

from src.ingestion.database.common import INGESTION_DATABASE
from src.ingestion.database.common import CONTENT_PROFILE_TABLE
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_ID
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_TITLE
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_GENRES
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_TAGS
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_IMDB_ID
from src.ingestion.database.common import \
    CONTENT_PROFILE_TABLE_IMDB_PRIMARY_INFO
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_TMDB_ID
from src.ingestion.database.common import \
    CONTENT_PROFILE_TABLE_TMDB_PRIMARY_INFO
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_TMDB_CREDITS
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_TMDB_KEYWORDS
from src.ingestion.database.common import USER_PROFILE_TABLE
from src.ingestion.database.common import USER_PROFILE_TABLE_ID
from src.ingestion.database.common import USER_RATING_TABLE
from src.ingestion.database.common import USER_RATING_TABLE_USER_ID
from src.ingestion.database.common import USER_RATING_TABLE_CONTENT_ID
from src.ingestion.database.common import USER_RATING_TABLE_RATED_AT
from src.ingestion.database.common import USER_RATING_TABLE_RATING
from src.ingestion.database.common import USER_TAGGING_TABLE
from src.ingestion.database.common import USER_TAGGING_TABLE_USER_ID
from src.ingestion.database.common import USER_TAGGING_TABLE_CONTENT_ID
from src.ingestion.database.common import USER_TAGGING_TABLE_TAGGED_AT
from src.ingestion.database.common import USER_TAGGING_TABLE_TAG
from src.ingestion.database.common import ContentProfileEntity
from src.ingestion.database.common import ImdbContentProfileEntity
from src.ingestion.database.common import TmdbContentProfileEntity
from src.ingestion.database.common import StatelessClassJsonEncoder
from src.ingestion.database.common import UserProfileEntity
from src.ingestion.database.common import UserRatingEntity
from src.ingestion.database.common import UserTaggingEntity
from src.ingestion.database.writer import IngestionWriterInterface


class PostgresIngestionWriter(IngestionWriterInterface):
    """It encapsulates the writer operations over the PostgreSQL ingestion
    database.
    """

    def __init__(self, host: str, password: str) -> None:
        """Creates a writer with a connection to the postgres ingestion
        database.

        Args:
            host (str): IP address of the postgres database server.
            password (str): Password of the postgres user.
        """
        super().__init__(writer_name="PostgreSQL")

        conn_params = "host='{host}' dbname='{dbname}'          \
                   user='postgres' password='{password}'".  \
            format(
                host=host,
                dbname=INGESTION_DATABASE,
                password=password
            )

        self.conn = connect(conn_params)

    def WriteUserProfiles(self, users: List[UserProfileEntity]) -> None:
        query = "INSERT INTO {table_name} ({id}) VALUES %s          \
                 ON CONFLICT ({id}) DO NOTHING"\
            .format(table_name=USER_PROFILE_TABLE,
                    id=USER_PROFILE_TABLE_ID)

        cursor = self.conn.cursor()

        rows_to_insert = list()
        for user_profile in users:
            rows_to_insert.append((user_profile.user_id,))

        pge.execute_values(cur=cursor,
                           sql=query,
                           argslist=rows_to_insert,
                           template=None)
        self.conn.commit()

    def WriteContentProfiles(
            self, contents: List[ContentProfileEntity]) -> None:
        query = "INSERT INTO {table_name} ({id},                        \
                                           {title},                     \
                                           {genres},                    \
                                           {tags},                      \
                                           {imdb_id},                   \
                                           {tmdb_id}) VALUES %s         \
                 ON CONFLICT ({id})                                     \
                 DO UPDATE SET                                          \
                    {title}=excluded.{title},                           \
                    {genres}=excluded.{genres},                         \
                    {tags}=excluded.{tags},                             \
                    {imdb_id}=excluded.{imdb_id},                       \
                    {tmdb_id}=excluded.{tmdb_id}".                      \
            format(table_name=CONTENT_PROFILE_TABLE,
                   id=CONTENT_PROFILE_TABLE_ID,
                   title=CONTENT_PROFILE_TABLE_TITLE,
                   genres=CONTENT_PROFILE_TABLE_GENRES,
                   tags=CONTENT_PROFILE_TABLE_TAGS,
                   imdb_id=CONTENT_PROFILE_TABLE_IMDB_ID,
                   tmdb_id=CONTENT_PROFILE_TABLE_TMDB_ID)

        cursor = self.conn.cursor()

        rows_to_insert = list()
        for content_profile in contents:
            rows_to_insert.append((
                content_profile.content_id,
                content_profile.title,
                content_profile.genres,
                json.dumps(content_profile.tags,
                           cls=StatelessClassJsonEncoder)
                if content_profile.tags is not None else None,
                content_profile.imdb_id,
                content_profile.tmdb_id,
            ))

        pge.execute_values(cur=cursor,
                           sql=query,
                           argslist=rows_to_insert,
                           template=None)
        self.conn.commit()

    def WriteUserRatings(self, ratings: List[UserRatingEntity]) -> bool:
        query = "INSERT INTO {table_name} ({user_id},                   \
                                           {content_id},                \
                                           {rated_at},                  \
                                           {rating}) VALUES %s          \
                 ON CONFLICT ({user_id}, {content_id}, {rated_at})      \
                 DO UPDATE SET                                          \
                    {rating}=excluded.{rating}".                        \
                format(table_name=USER_RATING_TABLE,
                       user_id=USER_RATING_TABLE_USER_ID,
                       content_id=USER_RATING_TABLE_CONTENT_ID,
                       rating=USER_RATING_TABLE_RATING,
                       rated_at=USER_RATING_TABLE_RATED_AT)

        cursor = self.conn.cursor()

        rows_to_insert = list()
        for user_rating in ratings:
            rows_to_insert.append((
                user_rating.user_id,
                user_rating.content_id,
                datetime.utcfromtimestamp(user_rating.timestamp_secs),
                user_rating.rating,
            ))

        try:
            pge.execute_values(cur=cursor,
                               sql=query,
                               argslist=rows_to_insert,
                               template=None)
            self.conn.commit()
            return True
        except DatabaseError:
            self.conn.rollback()
            return False

    def WriteUserTaggings(self, tags: List[UserTaggingEntity]) -> bool:
        query = "INSERT INTO {table_name} ({user_id},                   \
                                           {content_id},                \
                                           {tag},                       \
                                           {tagged_at}) VALUES %s       \
                 ON CONFLICT ({user_id}, {content_id}, {tag})           \
                 DO UPDATE SET                                          \
                    {tagged_at}=excluded.{tagged_at}".                  \
                format(table_name=USER_TAGGING_TABLE,
                       user_id=USER_TAGGING_TABLE_USER_ID,
                       content_id=USER_TAGGING_TABLE_CONTENT_ID,
                       tag=USER_TAGGING_TABLE_TAG,
                       tagged_at=USER_TAGGING_TABLE_TAGGED_AT)

        cursor = self.conn.cursor()

        rows_to_insert = list()
        for user_tagging in tags:
            rows_to_insert.append((
                user_tagging.user_id,
                user_tagging.content_id,
                user_tagging.tag,
                datetime.utcfromtimestamp(user_tagging.timestamp_secs),
            ))

        try:
            pge.execute_values(cur=cursor,
                               sql=query,
                               argslist=rows_to_insert,
                               template=None)
            self.conn.commit()
            return True
        except DatabaseError:
            self.conn.rollback()
            return False

    def WriteContentTmdbFields(
            self, content_id: int, tmdb: TmdbContentProfileEntity) -> None:
        query = "INSERT INTO {table_name} ({id},                    \
                                           {tmdb_id},               \
                                           {primary_info},          \
                                           {credits},               \
                                           {keywords}) VALUES %s    \
                ON CONFLICT ({id})                                  \
                DO UPDATE SET                                       \
                    {tmdb_id}=excluded.{tmdb_id},                   \
                    {primary_info}=excluded.{primary_info},         \
                    {credits}=excluded.{credits},                   \
                    {keywords}=excluded.{keywords}".                \
                format(table_name=CONTENT_PROFILE_TABLE,
                       id=CONTENT_PROFILE_TABLE_ID,
                       tmdb_id=CONTENT_PROFILE_TABLE_TMDB_ID,
                       primary_info=CONTENT_PROFILE_TABLE_TMDB_PRIMARY_INFO,
                       credits=CONTENT_PROFILE_TABLE_TMDB_CREDITS,
                       keywords=CONTENT_PROFILE_TABLE_TMDB_KEYWORDS)

        cursor = self.conn.cursor()

        rows_to_insert = [
            (
                content_id,
                tmdb.tmdb_id,
                json.dumps(tmdb.primary_info)
                if tmdb.primary_info is not None else None,
                json.dumps(tmdb.credits)
                if tmdb.credits is not None else None,
                json.dumps(tmdb.keywords)
                if tmdb.keywords is not None else None,
            )
        ]

        pge.execute_values(cur=cursor,
                           sql=query,
                           argslist=rows_to_insert,
                           template="(%s,%s,%s,%s,%s)")
        self.conn.commit()

    def WriteContentImdbFields(
            self, content_id: int, imdb: ImdbContentProfileEntity) -> None:
        query = "INSERT INTO {table_name} ({id},                        \
                                           {imdb_id},                   \
                                           {primary_info}) VALUES %s    \
                ON CONFLICT ({id})                                      \
                DO UPDATE SET                                           \
                    {imdb_id}=excluded.{imdb_id},                       \
                    {primary_info}=excluded.{primary_info}".            \
                format(table_name=CONTENT_PROFILE_TABLE,
                       id=CONTENT_PROFILE_TABLE_ID,
                       imdb_id=CONTENT_PROFILE_TABLE_IMDB_ID,
                       primary_info=CONTENT_PROFILE_TABLE_IMDB_PRIMARY_INFO)

        cursor = self.conn.cursor()

        rows_to_insert = [
            (
                content_id,
                imdb.imdb_id,
                json.dumps(imdb.primary_info)
                if imdb.primary_info is not None else None,
            )
        ]

        pge.execute_values(cur=cursor,
                           sql=query,
                           argslist=rows_to_insert,
                           template="(%s,%s,%s)")
        self.conn.commit()
