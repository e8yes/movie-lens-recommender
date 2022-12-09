from pyspark.sql import DataFrame
from typing import Tuple

from src.ingestion.database.reader import RATING_FEEDBACK_DF_USER_ID
from src.ingestion.database.reader import RATING_FEEDBACK_DF_CONTENT_ID
from src.ingestion.database.reader import RATING_FEEDBACK_DF_RATING
from src.ingestion.database.reader import IngestionReaderInterface
from src.ingestion.database.reader import ReadRatingFeedbacks


def PartitionRatingGlobal(reader: IngestionReaderInterface) -> \
        Tuple[DataFrame, DataFrame, DataFrame]:
    """_summary_

    Returns:
        _type_: _description_
    """
    ratings = ReadRatingFeedbacks(reader=reader)
    ratings = ratings.repartition(numPartitions=100)

    ratings = ratings.select([RATING_FEEDBACK_DF_USER_ID,
                              RATING_FEEDBACK_DF_CONTENT_ID,
                              RATING_FEEDBACK_DF_RATING])
    ratings = ratings.checkpoint()
    partitions = ratings.randomSplit(weights=[0.8, 0.1, 0.1])

    return partitions[0], partitions[1], partitions[2]
