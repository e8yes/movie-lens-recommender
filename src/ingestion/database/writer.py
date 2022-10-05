import json
import psycopg2.extras as pge
from typing import List, Set

from src.ingestion.database.common import *


def __CreateExternalDbEntries(table_name: str,
                              id_column_name: str,
                              ids: Set[int],
                              conn):
    query = "INSERT INTO {table_name} ({id}) VALUES %s      \
             ON CONFLICT DO NOTHING".                       \
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


def WriteUserProfiles(user_profiles: List[UserProfileEntity], conn):
    """Writes the specified list of user profiles to the user_profile table.
    It overwrites existing entries.

    Args:
        user_profiles (List[UserProfile]): The list of user profiles to be
            written.
        conn (psycopg2.connection): A psycopg2 connection.
    """
    query = "INSERT INTO {table_name} ({id}) VALUES %s                      \
             ON CONFLICT (id) DO NOTHING".                                  \
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


def WriteContentProfiles(content_profiles: List[ContentProfileEntity], conn):
    """Writes the specified list of content profiles to the content_profile
    table. It overwrites existing entries.

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
             ON CONFLICT (id)                                       \
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
