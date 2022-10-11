import json
import psycopg2 as pg
import psycopg2.extras as pge
from datetime import datetime
from typing import List, Set

from src.ingestion.database.common import *


def __CreateExternalDbEntries(table_name: str,
                              id_column_name: str,
                              ids: Set[int],
                              conn) -> None:
    query = "INSERT INTO {table_name} ({id}) VALUES %s      \
             ON CONFLICT ({id}) DO NOTHING".                \
        format(table_name=table_name,
               id=id_column_name)

    cursor = conn.cursor()

    rows_to_insert = list()
    for id in ids:
        rows_to_insert.append((id,))

    pge.execute_values(cur=cursor,
                       sql=query,
                       argslist=rows_to_insert,
                       template=None)
    conn.commit()


def WriteUserProfiles(user_profiles: List[UserProfileEntity], conn) -> None:
    """Writes the specified list of user profiles to the user_profile table.
    It overwrites existing entries.

    Args:
        user_profiles (List[UserProfile]): The list of user profiles to be
            written.
        conn (psycopg2.connection): A psycopg2 connection.
    """
    query = "INSERT INTO {table_name} ({id}) VALUES %s                      \
             ON CONFLICT ({id}) DO NOTHING".                                \
        format(table_name=USER_PROFILE_TABLE, id=USER_PROFILE_TABLE_ID)

    cursor = conn.cursor()

    rows_to_insert = list()
    for user_profile in user_profiles:
        rows_to_insert.append((user_profile.user_id,))

    pge.execute_values(cur=cursor,
                       sql=query,
                       argslist=rows_to_insert,
                       template=None)
    conn.commit()


def WriteContentProfiles(
        content_profiles: List[ContentProfileEntity],
        conn) -> None:
    """Writes the specified list of content profiles to the content_profile
    table. It overwrites existing entries. For external references to IMDB or
    TMDB, it creates placeholder entries on those tables if they have not
    already existed.

    Args:
        content_profiles (List[ContentProfileEntity]): The list of content
            profiles to write to the database table.
        conn (psycopg2.connection): A psycopg2 connection.
    """
    # Makes sure that the referenced entries from the imdb and tmdb tables exist.
    imdb_ids = set()
    tmdb_ids = set()
    for content_profile in content_profiles:
        if content_profile.imdb_id is not None:
            imdb_ids.add(content_profile.imdb_id)

        if content_profile.tmdb_id is not None:
            tmdb_ids.add(content_profile.tmdb_id)

    __CreateExternalDbEntries(table_name=IMDB_TABLE,
                              id_column_name=IMDB_TABLE_ID,
                              ids=imdb_ids,
                              conn=conn)
    __CreateExternalDbEntries(table_name=TMDB_TABLE,
                              id_column_name=TMDB_TABLE_ID,
                              ids=tmdb_ids,
                              conn=conn)

    # Writes profiles to the content_profile table.
    query = "INSERT INTO {table_name} ({id},                        \
                                       {title},                     \
                                       {genres},                    \
                                       {genome_scores},             \
                                       {tags},                      \
                                       {imdb_id},                   \
                                       {tmdb_id}) VALUES %s         \
             ON CONFLICT ({id})                                     \
             DO UPDATE SET                                          \
                {title}=excluded.{title},                           \
                {genres}=excluded.{genres},                         \
                {genome_scores}=excluded.{genome_scores},           \
                {tags}=excluded.{tags},                             \
                {imdb_id}=excluded.{imdb_id},                       \
                {tmdb_id}=excluded.{tmdb_id}".                      \
        format(table_name=CONTENT_PROFILE_TABLE,
               id=CONTENT_PROFILE_TABLE_ID,
               title=CONTENT_PROFILE_TABLE_TITLE,
               genres=CONTENT_PROFILE_TABLE_GENRES,
               genome_scores=CONTENT_PROFILE_TABLE_GENOME_SCORES,
               tags=CONTENT_PROFILE_TABLE_TAGS,
               imdb_id=CONTENT_PROFILE_TABLE_IMDB_ID,
               tmdb_id=CONTENT_PROFILE_TABLE_TMDB_ID)

    cursor = conn.cursor()

    rows_to_insert = list()
    for content_profile in content_profiles:
        rows_to_insert.append((
            content_profile.content_id,
            content_profile.title,
            content_profile.genres,
            json.dumps(content_profile.genome_scores),
            json.dumps(content_profile.tags, cls=StatelessClassJsonEncoder),
            content_profile.imdb_id,
            content_profile.tmdb_id,
        ))

    pge.execute_values(cur=cursor,
                       sql=query,
                       argslist=rows_to_insert,
                       template=None)
    conn.commit()


def WriteUserRatings(user_ratings: List[UserRatingEntity], conn) -> bool:
    """Writes the specified list of user rating feedbacks to the user_rating
    table. It overwrites existing entries. It assumes the referenced user_ids
    and content_ids exist in their respective tables. Otherwise, it rejects the
    write and no change will be applied to the user_rating table.

    Args:
        user_ratings (List[UserRatingEntity]):  The list of user ratings to
            write to the database table.
        conn (psycopg2.connection): A psycopg2 connection.

    Returns:
        bool: It returns true when all the user_ids and content_ids are valid.
            Otherwise, it returns false.
    """
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

    cursor = conn.cursor()

    rows_to_insert = list()
    for user_rating in user_ratings:
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
        conn.commit()
        return True
    except:
        conn.rollback()
        return False


def WriteUserTaggings(user_taggings: List[UserTaggingEntity], conn) -> bool:
    """Writes the specified list of user tagging feedbacks to the user_tagging
    table. It overwrites existing entries. It assumes the referenced user_ids
    and content_ids exist in their respective tables. Otherwise, it rejects the
    write and no change will be applied to the user_tagging table.

    Args:
        user_taggings (List[UserTaggingEntity]):  The list of user tags to
            write to the database table.
        conn (psycopg2.connection): A psycopg2 connection.

    Returns:
        bool: It returns true when all the user_ids and content_ids are valid.
            Otherwise, it returns false.
    """
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

    cursor = conn.cursor()

    rows_to_insert = list()
    for user_tagging in user_taggings:
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
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        return False


def WriteImdbContentProfiles(imdb_profiles: List[ImdbContentProfileEntity],
                             conn) -> None:
    """Writes the specified list of IMDB profiles to the imdb table.
    It overwrites existing entries with the content supplied.

    Args:
        imdb_profiles (List[ImdbContentProfileEntity]): the list of IMDB
            profiles to be written to the database table.
        conn (psycopg2.connection): A psycopg2 connection.
    """
    query = "INSERT INTO {table_name} ({id},                    \
                                       {primary_info},          \
                                       {ingested_at}) VALUES %s \
             ON CONFLICT ({id})                                 \
             DO UPDATE SET                                      \
                {primary_info}=excluded.{primary_info},         \
                {ingested_at}=excluded.{ingested_at}".          \
        format(table_name=IMDB_TABLE,
               id=IMDB_TABLE_ID,
               primary_info=IMDB_TABLE_PRIMARY_INFO,
               ingested_at=IMDB_TABLE_INGESTED_AT)

    cursor = conn.cursor()

    rows_to_insert = list()
    for imdb_profile in imdb_profiles:
        rows_to_insert.append((
            imdb_profile.imdb_id,
            json.dumps(imdb_profile.primary_info),
        ))

    pge.execute_values(cur=cursor,
                       sql=query,
                       argslist=rows_to_insert,
                       template="(%s,%s,CURRENT_TIMESTAMP)")
    conn.commit()


def WriteTmdbContentProfiles(tmdb_profiles: List[TmdbContentProfileEntity],
                             conn) -> None:
    """Writes the specified list of TMDB profiles to the tmdb table.
    It overwrites existing entries with the content supplied.

    Args:
        imdb_profiles (List[TmdbContentProfileEntity]): the list of TMDB
            profiles to be written to the database table.
        conn (psycopg2.connection): A psycopg2 connection.
    """
    query = "INSERT INTO {table_name} ({id},                    \
                                       {primary_info},          \
                                       {credits},               \
                                       {ingested_at}) VALUES %s \
             ON CONFLICT ({id})                                 \
             DO UPDATE SET                                      \
                {primary_info}=excluded.{primary_info},         \
                {credits}=excluded.{credits},                   \
                {ingested_at}=excluded.{ingested_at}".          \
        format(table_name=TMDB_TABLE,
               id=TMDB_TABLE_ID,
               primary_info=TMDB_TABLE_PRIMARY_INFO,
               credits=TMDB_TABLE_CREDITS,
               ingested_at=TMDB_TABLE_INGESTED_AT)

    cursor = conn.cursor()

    rows_to_insert = list()
    for tmdb_profile in tmdb_profiles:
        rows_to_insert.append((
            tmdb_profile.tmdb_id,
            json.dumps(tmdb_profile.primary_info),
            json.dumps(tmdb_profile.credits),
        ))

    pge.execute_values(cur=cursor,
                       sql=query,
                       argslist=rows_to_insert,
                       template="(%s,%s,%s,CURRENT_TIMESTAMP)")
    conn.commit()
