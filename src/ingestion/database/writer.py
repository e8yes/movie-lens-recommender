from typing import List

from src.ingestion.database.common import ContentProfileEntity
from src.ingestion.database.common import ImdbContentProfileEntity
from src.ingestion.database.common import TmdbContentProfileEntity
from src.ingestion.database.common import UserProfileEntity
from src.ingestion.database.common import UserRatingEntity
from src.ingestion.database.common import UserTaggingEntity


class IngestionWriterInterface:
    """_summary_
    """

    def __init__(self, writer_name: str) -> None:
        self.writer_name = writer_name

    def WriteUserProfiles(self, users: List[UserProfileEntity]) -> None:
        """Writes the specified list of user profiles to the user_profile
        table. It overwrites existing entries. Also, makes sure the user_ids
        in the users array are unique.

        Args:
            users (List[UserProfile]): The list of user profiles to be
                written.
        """
        pass

    def WriteContentProfiles(
            self, contents: List[ContentProfileEntity]) -> None:
        """Writes the specified list of content profiles to the content_profile
        table. It overwrites existing entries. makes sure the content_ids
        in the contents array are unique.

        For external references to IMDB or TMDB, it retains the existing values
        in those fields even when imdb_id/tmdb_id are specified.

        Args:
            contents (List[ContentProfileEntity]):  The list of content
                profiles to write to the database table.
        """
        pass

    def WriteUserRatings(self, ratings: List[UserRatingEntity]) -> bool:
        """Writes the specified list of user rating feedbacks to the
        user_rating table. It overwrites existing entries. It assumes the
        referenced user_ids and content_ids exist in their respective tables.
        Otherwise, it rejects the write and no change will be applied to the
        user_rating table.

        Args:
            ratings (List[UserRatingEntity]): The list of user ratings to
                write to the database table.

        Returns:
            bool: It returns true when all the user_ids and content_ids are
                valid. Otherwise, it returns false.
        """
        pass

    def WriteUserTaggings(self, tags: List[UserTaggingEntity]) -> bool:
        """Writes the specified list of user tagging feedbacks to the
        user_tagging table. It overwrites existing entries. It assumes the
        referenced user_ids and content_ids exist in their respective tables.
        Otherwise, it rejects the write and no change will be applied to the
        user_tagging table.

        Args:
            user_taggings (List[UserTaggingEntity]):  The list of user tags to
                write to the database table.
            conn (psycopg2.connection): A psycopg2 connection.

        Returns:
            bool: It returns true when all the user_ids and content_ids are
                valid. Otherwise, it returns false.
        """
        pass

    def WriteContentTmdbFields(
            self, content_id: int, tmdb: TmdbContentProfileEntity) -> None:
        """Writes the specified TMDB profile of the piece of content,
        specified by the content_id, to the tmdb table. It overwrites the
        existing entry with the content supplied.

        Args:
            content_id (int): ID of the piece of content where its TMDB
                profile needs to be written.
            tmdb (TmdbContentProfileEntity): The TMDB profile to be written.
        """
        pass

    def WriteContentImdbFields(
            self, content_id: int, imdb: ImdbContentProfileEntity) -> None:
        """Writes the specified IMDB profile of the piece of content,
        specified by the content_id, to the imdb table. It overwrites the
        existing entry with the content supplied.

        Args:
            content_id (int): ID of the piece of content where its IMDB
                profile needs to be written.
            imdb (TmdbContentProfileEntity): The IMDB profile to be written.
        """
        pass
