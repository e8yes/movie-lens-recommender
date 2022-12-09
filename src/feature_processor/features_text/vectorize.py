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


def VectorizeTokensWithIdf(tokens_idf: DataFrame,
                           glove: DataFrame,
                           output_column_name: str) -> DataFrame:
    """Turns word tokens into a vector by summing word embeddings of each token
    and weighing each by the token's IDF.

    Example inputs:
    tokens_idf
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
    | id | output             |
    ---------------------------
    | 1  | [-0.6, -1.2, -1.8] |
    ---------------------------

    Args:
        tokens_idf (DataFrame): See the example inputs above.
        glove (DataFrame): See the example inputs above.
        output_column_name (str): Name of the output column.

    Returns:
        DataFrame: See the example output above.
    """
    interm1 = tokens_idf.join(
        glove, tokens_idf['token'] == glove['word']).withColumn(
        'weighted_embedding', expr("""transform(embedding,x -> x*idf)"""))
    n = len(interm1.select("weighted_embedding").first()
            [0])  # dimension of the embedding
    res = interm1.groupBy("id").agg(array(
        *[sum(col("weighted_embedding")[i]) for i in range(n)]).alias(output_column_name))
    return res


def VectorizeTokens(tokens: DataFrame,
                    glove: DataFrame,
                    output_column_name: str) -> DataFrame:
    """Turns word tokens into a vector by averaging the word embeddings of
    each token.

    Example inputs:
    tokens
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
    | id | output          |
    ------------------------
    | 1  | [5.5, 6.5, 7.5] |
    ------------------------

    Args:
        user_tag_tokens (DataFrame): See the example inputs above.
        glove (DataFrame): See the example inputs above.

    Returns:
        DataFrame: See the example output above.
    """

    interm1 = tokens.withColumnRenamed(
        'token', 'word').join(glove, ['word'])

    # dimension of the embedding
    n = len(interm1.select("embedding").first()[0])
    res = interm1.groupBy("id").agg(
        array(*[avg(col("embedding")[i]) for i in range(n)]).alias(output_column_name))
    return res
