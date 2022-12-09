import os
from pyspark.sql import DataFrame, Row, SparkSession, types
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql import Row, types


def _ParseLine(line: str) -> Row:
    parts = line.split(sep=" ")

    word = parts[0]

    embedding = list()
    for i in range(1, len(parts)):
        embedding.append(float(parts[i]))

    return Row(word=word, embedding=embedding)


def LoadGloveDefinitions(path: str,
                         dimension: int,
                         spark: SparkSession) -> DataFrame:
    """Loads GloVe word embeddings definitions as a dataframe.

    Example output:
    --------------------------------------------
    | word  | embedding                        |
    --------------------------------------------
    | "the" | [0.418, 0.24968, -0.41242, ...]  |
    --------------------------------------------
    | "won" | [-1.5561, 0.86241, 0.14604, ...] |
    --------------------------------------------

    Args:
        path (str): Path to which GloVe word embeddings txt files are located.
        dimension (int): The size of word embeddins to use. Value can be 50,
            100, 200 and 300.

    Returns:
        DataFrame: See the example output above.
    """
    glove_file_name = "glove.6B.{0}d.txt".format(dimension)
    glove_path = os.path.join(path, glove_file_name)

    schema = types.StructType(
        fields=[
            types.StructField(
                name="word", dataType=types.StringType()),
            types.StructField(
                name="embedding", dataType=types.ArrayType(
                    elementType=types.FloatType()))
        ])

    return spark.                   \
        sparkContext.               \
        textFile(name=glove_path).  \
        map(_ParseLine).            \
        toDF(schema=schema)


def VectorizeContentSummaryTokens(content_tokens_idf: DataFrame,
                                  glove: DataFrame) -> DataFrame:
    """Turns word tokens into a vector by summing word embeddings of each token
    and weighing each by the token's IDF.

    Example inputs:
    content_tokens_idf
    ------------------------------
    | id | token           | idf |
    ------------------------------
    | 1  | "anna"          | 0   |
    ------------------------------
    | 1  | ","             | 0   |
    ------------------------------
    | 1  | "an"            | 0   |
    ------------------------------
    | 1  | "oh-my-god"     | 0.8 |
    ------------------------------
    | 1  | "moment"        | 0.2 |
    ------------------------------
    | 1  | "."             | 0   |
    ------------------------------

    glove
    ----------------------------------
    | word            | embedding    |
    ----------------------------------
    | "anna"          | [0, 1, 2]    |
    ----------------------------------
    | ","             | [2, 3, 4]    |
    ----------------------------------
    | "an"            | [5, 6, 7]    |
    ----------------------------------
    | "oh-my-god"     | [-1, -2, -3] |
    ----------------------------------
    | "moment"        | [1, 2, 3]    |
    ----------------------------------

    Example output:
    ---------------------------
    | id | summary            |
    ---------------------------
    | 1  | [-0.6, -1.2, -1.8] |
    ---------------------------

    Args:
        content_tokens_idf (DataFrame): See the example inputs above.
        glove (DataFrame): See the example inputs above.

    Returns:
        DataFrame: See the example output above.
    """
    interm1 = content_tokens_idf.join(
        glove, content_tokens_idf['token'] == glove['word'],
        'left').withColumn(
        'weighted_embedding', expr("""transform(embedding,x -> x*idf)"""))
    n = len(interm1.select("weighted_embedding").first()
            [0])  # dimension of the embedding
    res = interm1.groupBy("id").agg(
        array(*[sum(col("weighted_embedding")[i]) for i in range(n)]).alias("summary"))
    return res

# def VectorizeContentScoredTags(content_scored_tags: DataFrame,
#                                glove: DataFrame) -> DataFrame:
#     """Turns scored tags into a vector by summing word embeddings of each tag
#     and weighing each entry by the tag's normalized relevance score.

#     Example inputs:
#     scored tags:
#     -----------------------------------
#     | id | scored_tags                |
#     -----------------------------------
#     | 1  | {"Good": 0.9, "Bad": 0.2}  |
#     -----------------------------------

#     glove:
#     -------------------------
#     | word   | embedding    |
#     -------------------------
#     | "good" | [1, 2, 3]    |
#     -------------------------
#     | "bad"  | [-1, -2, -3] |
#     -------------------------

#     Intermediate result (normalized relevance):
#     ------------------------------------
#     | id | scored_tags                 |
#     ------------------------------------
#     | 1  | {"Good": 0.82, "Bad": 0.18} |
#     ------------------------------------

#     Example output:
#     ---------------------------
#     | id | scored_tags        |
#     ---------------------------
#     | 1  | [0.64, 1.28, 1.92] |
#     ---------------------------

#     Args:
#         content_scored_tags (DataFrame): See the example inputs above.
#         glove (DataFrame): See the example inputs above.

#     Returns:
#         DataFrame: See the example output above.
#     """
#     content_scored_tags.show()
#     glove.show()


def VectorizeUserTokens(user_tag_tokens: DataFrame,
                        glove: DataFrame) -> DataFrame:
    """Turns user tags into a vector by averaging the word embeddings of each
    tag token.

    Example inputs:
    user_tags
    -------------------------------
    | id | token                  |
    -------------------------------
    | 1  | "unlikely-friendships" |
    -------------------------------
    | 1  | ","                    |
    -------------------------------
    | 1  | "buzz"                 |
    -------------------------------
    | 1  | "lightyear"            |
    -------------------------------

    glove:
    -----------------------------------------
    | word                   | embedding    |
    -----------------------------------------
    | "unlikely-friendships" | [1, 2, 3]    |
    -----------------------------------------
    | ","                    | [4, 5, 6]    |
    -----------------------------------------
    | "buzz"                 | [7, 8, 9]    |
    -----------------------------------------
    | "lightyear"            | [10, 11, 12] |
    -----------------------------------------

    Example output:
    ------------------------
    | id | tags            |
    ------------------------
    | 1  | [5.5, 6.5, 7.5] |
    ------------------------

    Args:
        user_tag_tokens (DataFrame): See the example inputs above.
        glove (DataFrame): See the example inputs above.

    Returns:
        DataFrame: See the example output above.
    """

    interm1 = user_tag_tokens.withColumnRenamed(
        'token', 'word').join(glove, ['word'], 'left')

    # dimension of the embedding
    n = len(interm1.select("embedding").first()[0])
    res = interm1.groupBy("id").agg(
        array(*[sum(col("embedding")[i]) for i in range(n)]).alias("summary")).show()
    return res
