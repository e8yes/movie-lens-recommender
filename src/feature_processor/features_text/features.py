from pyspark.sql import DataFrame


def ComputeContentTextFeatures(contents: DataFrame) -> DataFrame:
    contents.show()


# def ComputeUserTextFeatures(
#         users: DataFrame,
#         user_tagging_feedbacks: DataFrame) -> DataFrame:
#     user_tagging_feedbacks.show()
