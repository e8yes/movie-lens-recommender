import json
from cassandra.query import BatchStatement
from cassandra.cluster import Cluster
from datetime import datetime
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
from src.ingestion.database.common import CONTENT_PROFILE_TABLE_INGESTED_AT
from src.ingestion.database.common import USER_PROFILE_TABLE
from src.ingestion.database.common import USER_PROFILE_TABLE_ID
from src.ingestion.database.common import USER_PROFILE_TABLE_INGESTED_AT
from src.ingestion.database.common import USER_RATING_TABLE
from src.ingestion.database.common import USER_RATING_TABLE_USER_ID
from src.ingestion.database.common import USER_RATING_TABLE_CONTENT_ID
from src.ingestion.database.common import USER_RATING_TABLE_RATED_AT
from src.ingestion.database.common import USER_RATING_TABLE_RATING
from src.ingestion.database.common import USER_RATING_TABLE_INGESTED_AT
from src.ingestion.database.common import USER_TAGGING_TABLE
from src.ingestion.database.common import USER_TAGGING_TABLE_USER_ID
from src.ingestion.database.common import USER_TAGGING_TABLE_CONTENT_ID
from src.ingestion.database.common import USER_TAGGING_TABLE_TAGGED_AT
from src.ingestion.database.common import USER_TAGGING_TABLE_TAG
from src.ingestion.database.common import USER_TAGGING_TABLE_INGESTED_AT
from src.ingestion.database.common import ContentProfileEntity
from src.ingestion.database.common import ImdbContentProfileEntity
from src.ingestion.database.common import TmdbContentProfileEntity
from src.ingestion.database.common import UserProfileEntity
from src.ingestion.database.common import UserRatingEntity
from src.ingestion.database.common import UserTaggingEntity
from src.ingestion.database.common import StatelessClassJsonEncoder
from src.ingestion.database.writer import IngestionWriterInterface


class CassandraIngestionWriter(IngestionWriterInterface):
    """_summary_

    Args:
        IngestionWriterInterface (_type_): _description_
    """

    def __init__(self, contact_points: List[str]) -> None:
        super().__init__(writer_name="Cassandra")

        self.cluster = Cluster(contact_points=contact_points)
        self.session = self.cluster.connect(INGESTION_DATABASE)

    def WriteUserProfiles(self, users: List[UserProfileEntity]) -> None:
        query = "INSERT INTO {table_name}"          \
                "       ({id}, {ingested_at})"      \
                "VALUES (?, toTimestamp(now()))".   \
            format(table_name=USER_PROFILE_TABLE,
                   id=USER_PROFILE_TABLE_ID,
                   ingested_at=USER_PROFILE_TABLE_INGESTED_AT)
        stmt = self.session.prepare(query=query)

        batch = BatchStatement()
        for user in users:
            batch.add(statement=stmt,
                      parameters=(
                          user.user_id,
                      ))

        self.session.execute(batch)

    def WriteContentProfiles(
            self, contents: List[ContentProfileEntity]) -> None:
        query = "INSERT INTO {table_name} ({id},            \
                                           {title},         \
                                           {genres},        \
                                           {tags},          \
                                           {imdb_id},       \
                                           {tmdb_id},       \
                                           {ingested_at}) " \
                "VALUES (?,?,?,?,?,?,toTimestamp(now()))".  \
            format(table_name=CONTENT_PROFILE_TABLE,
                   id=CONTENT_PROFILE_TABLE_ID,
                   title=CONTENT_PROFILE_TABLE_TITLE,
                   genres=CONTENT_PROFILE_TABLE_GENRES,
                   tags=CONTENT_PROFILE_TABLE_TAGS,
                   imdb_id=CONTENT_PROFILE_TABLE_IMDB_ID,
                   tmdb_id=CONTENT_PROFILE_TABLE_TMDB_ID,
                   ingested_at=CONTENT_PROFILE_TABLE_INGESTED_AT)

        stmt = self.session.prepare(query=query)

        batch = BatchStatement()
        for content in contents:
            batch.add(
                statement=stmt,
                parameters=(
                    content.content_id,
                    content.title,
                    content.genres,
                    json.dumps(content.tags, cls=StatelessClassJsonEncoder)
                    if content.tags is not None else None,
                    content.imdb_id,
                    content.tmdb_id,
                ))

        self.session.execute(batch)

    def WriteUserRatings(self, ratings: List[UserRatingEntity]) -> bool:
        query = "INSERT INTO {table_name} ({user_id},       \
                                           {content_id},    \
                                           {rated_at},      \
                                           {rating},        \
                                           {ingested_at})"  \
                "VALUES (?,?,?,?,toTimestamp(now()))".      \
                format(table_name=USER_RATING_TABLE,
                       user_id=USER_RATING_TABLE_USER_ID,
                       content_id=USER_RATING_TABLE_CONTENT_ID,
                       rating=USER_RATING_TABLE_RATING,
                       rated_at=USER_RATING_TABLE_RATED_AT,
                       ingested_at=USER_RATING_TABLE_INGESTED_AT)

        stmt = self.session.prepare(query=query)

        batch = BatchStatement()
        for rating in ratings:
            batch.add(
                statement=stmt,
                parameters=(
                    rating.user_id,
                    rating.content_id,
                    datetime.utcfromtimestamp(rating.timestamp_secs),
                    rating.rating,
                ))

        self.session.execute(batch)

        return True

    def WriteUserTaggings(self, tags: List[UserTaggingEntity]) -> bool:
        query = "INSERT INTO {table_name} ({user_id},       \
                                           {content_id},    \
                                           {tag},           \
                                           {tagged_at},     \
                                           {ingested_at})"  \
                "VALUES (?,?,?,?,toTimestamp(now()))".      \
                format(table_name=USER_TAGGING_TABLE,
                       user_id=USER_TAGGING_TABLE_USER_ID,
                       content_id=USER_TAGGING_TABLE_CONTENT_ID,
                       tag=USER_TAGGING_TABLE_TAG,
                       tagged_at=USER_TAGGING_TABLE_TAGGED_AT,
                       ingested_at=USER_TAGGING_TABLE_INGESTED_AT)

        stmt = self.session.prepare(query=query)

        batch = BatchStatement()
        for tag in tags:
            batch.add(
                statement=stmt,
                parameters=(
                    tag.user_id,
                    tag.content_id,
                    tag.tag,
                    datetime.utcfromtimestamp(tag.timestamp_secs),
                ))

        self.session.execute(batch)

        return True

    def WriteContentTmdbFields(
            self, content_id: int, tmdb: TmdbContentProfileEntity) -> None:
        query = "INSERT INTO {table_name} ({id},            \
                                           {tmdb_id},       \
                                           {primary_info},  \
                                           {credits},       \
                                           {keywords})"     \
                "VALUES (%s,%s,%s,%s,%s)".                  \
                format(table_name=CONTENT_PROFILE_TABLE,
                       id=CONTENT_PROFILE_TABLE_ID,
                       tmdb_id=CONTENT_PROFILE_TABLE_TMDB_ID,
                       primary_info=CONTENT_PROFILE_TABLE_TMDB_PRIMARY_INFO,
                       credits=CONTENT_PROFILE_TABLE_TMDB_CREDITS,
                       keywords=CONTENT_PROFILE_TABLE_TMDB_KEYWORDS)

        self.session.execute(
            query=query,
            parameters=(
                content_id,
                tmdb.tmdb_id,
                json.dumps(tmdb.primary_info)
                if tmdb.primary_info is not None else None,
                json.dumps(tmdb.credits)
                if tmdb.credits is not None else None,
                json.dumps(tmdb.keywords)
                if tmdb.keywords is not None else None,
            ))

    def WriteContentImdbFields(
            self, content_id: int, imdb: ImdbContentProfileEntity) -> None:
        query = "INSERT INTO {table_name} ({id},            \
                                           {imdb_id},       \
                                           {primary_info})" \
                "VALUES (%s,%s,%s)".                        \
                format(table_name=CONTENT_PROFILE_TABLE,
                       id=CONTENT_PROFILE_TABLE_ID,
                       imdb_id=CONTENT_PROFILE_TABLE_IMDB_ID,
                       primary_info=CONTENT_PROFILE_TABLE_IMDB_PRIMARY_INFO)

        self.session.execute(
            query=query,
            parameters=(
                content_id,
                imdb.imdb_id,
                json.dumps(imdb.primary_info)
                if imdb.primary_info is not None else None,
            ))
