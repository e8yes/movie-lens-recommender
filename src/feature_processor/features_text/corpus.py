from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import json
import nltk
import re
import string
import ast
from autocorrect import Speller
from nltk.tokenize import word_tokenize
from nltk.stem.porter import PorterStemmer
from nltk.corpus import stopwords
from pyspark.sql import Row
from typing import Iterable


def CollectContentText(contents: DataFrame,
                       spell_correction: bool) -> DataFrame:
    """Collects textual information for each piece of content from the content
    dataframe.

    Example input:
    -----------------------------------------------------
    | id | title     | tmdb_primary_info                |
    -----------------------------------------------------
    | 1  | "A title" | '{                               |
    |    |           |      "overview": "An overveiw.", |
    |    |           |      "tagline": "A tagline",     |
    |    |           |  }'                              |
    -----------------------------------------------------
    | 2  | '{}'                                         |
    -----------------------------------------------------
    | 3  | '{"overview": ""}'                           |
    -----------------------------------------------------

    Example output:
    -----------------------
    | id | text           |
    -----------------------
    | 1  | "A title.      |
    |    |  An overview.  |
    |    |  A tagline."   |
    -----------------------

    Args:
        contents (DataFrame): See the example input above.
        spell_correction (bool): Whether to perform spell correction to the
            text.

    Returns:
        DataFrame: See the example output above.
    """
    usable_content = contents.\
        filter(contents["tmdb_id"].isNotNull()).\
        filter(contents["tmdb_primary_info"] != "null").\
        select(["id", "title", "tmdb_primary_info"])
    speller = Speller()

    def GetText(row: Row) -> Iterable[Row]:
        primary_info = json.loads(row["tmdb_primary_info"])
        if primary_info["overview"] is not None and \
                primary_info["overview"] != "":
            title = row['title']
            title = re.sub(r'\(\d+\)', '', title)
            overview = primary_info['overview']
            tagline = primary_info['tagline']

            if spell_correction:
                title = speller.autocorrect_sentence(title)
                overview = speller.autocorrect_sentence(overview)
                tagline = speller.autocorrect_sentence(tagline)

            text = title + "\n" + overview + "\n" + tagline

            yield Row(id=row["id"],
                      text=text)

    return usable_content.      \
        rdd.                    \
        flatMap(GetText).       \
        toDF()


def CollectContentTags(contents: DataFrame,
                       spell_correction: bool) -> DataFrame:
    """Turns the user tag JSON string into a line of string with each tag
    separated by comma.

    Example input:
    --------------------------------------------
    | id | tags                                |
    --------------------------------------------
    | 1  | '[{                                 |
    |    |      "tag": "unlikely friendships", |
    |    |      "timestamp_secs": 1438711374   |
    |    |  },                                 |
    |    |  {                                  |
    |    |      "tag": "Buzz Lightyear",       |
    |    |      "timestamp_secs": 1140447535   |
    |    |  }]'                                |
    --------------------------------------------
    | 2  | '[]'                                |
    --------------------------------------------


    Example output:
    ----------------------------------------------
    | id | text                                  |
    ----------------------------------------------
    | 1  | "unlikely friendships,Buzz Lightyear" |
    ----------------------------------------------

    Args:
        contents (DataFrame): See the example input above.
        spell_correction (bool): Whether to perform spell correction to the
            text.

    Returns:
        DataFrame: See the example output above.
    """
    usable_content = contents.\
        filter(contents["tags"] != "null").\
        select(["id", "tags"])

    speller = Speller()

    def GetTags(row: Row) -> Iterable[Row]:
        all_tags = str()

        tags = json.loads(row["tags"])
        for tag_struct in tags:
            tag_info = tag_struct["tag"]

            if spell_correction:
                tag_info = speller.autocorrect_word(tag_info)

            all_tags += tag_info + ","

        yield Row(id=row["id"],
                  text=all_tags[:-1])

    return usable_content.      \
        rdd.                    \
        flatMap(GetTags).       \
        toDF()


def CollectContentKeywords(contents: DataFrame,
                           spell_correction: bool) -> DataFrame:
    usable_content = contents.\
        filter((contents["tmdb_keywords"] != "null") &
               (contents["tmdb_keywords"] != "[]")).\
        select(["id", "tmdb_keywords"])

    speller = Speller()

    def GetKeywords(row: Row) -> Iterable[Row]:
        keywords = json.loads(row["tmdb_keywords"])["keywords"]
        all_keywords = str()

        for keyword_struct in keywords:
            keyword = keyword_struct["name"]
            if spell_correction:
                keyword = speller.autocorrect_word(keyword)

            all_keywords += keyword + ","

        yield Row(id=row["id"],
                  text=all_keywords[:-1])

    return usable_content.      \
        rdd.                    \
        flatMap(GetKeywords).   \
        toDF()


def TokenizeText(content_text: DataFrame) -> DataFrame:
    """Breaks a text string into word tokens for each piece of content.

    Example input:
    -----------------------
    | id | text           |
    -----------------------
    | 1  | "An overview." |
    -----------------------

    Example output:
    --------------------------------
    | id | index | token           |
    --------------------------------
    | 1  | 0     | "an"            |
    --------------------------------
    | 1  | 1     | "overview"      |
    --------------------------------
    | 1  | 2     | "."             |
    --------------------------------

    Args:
        content_text (DataFrame): See the example input above.

    Returns:
        DataFrame: See the example output above.
    """
    def TokenizeText(row: Row) -> Iterable[Row]:
        if "third_party/nltk_data" not in nltk.data.path:
            nltk.data.path.append("third_party/nltk_data")

        texts = row['text']
        counter = -1

        for word in word_tokenize(texts):
            word = word.lower()
            counter += 1

            yield Row(id=row["id"],
                      index=counter,
                      token=word)

    return content_text.        \
        rdd.                    \
        flatMap(TokenizeText).  \
        toDF()


def CollectTerms(content_tokens: DataFrame) -> DataFrame:
    """Collects fine grain word pieces that we care about and find their
    stemmed form. Words that we don't care about are: punctuations, stopwords
    and people names. Big word pieces are continued to be broken down by
    punctuation.

    Example input:
    ------------------------
    | id | token           |
    ------------------------
    | 1  | "anna"          |
    ------------------------
    | 1  | ","             |
    ------------------------
    | 1  | "an"            |
    ------------------------
    | 1  | "oh-my-god"     |
    ------------------------
    | 1  | "moment"        |
    ------------------------
    | 1  | "."             |
    ------------------------

    Example output:
    -----------------------------------
    | id | token           | term     |
    -----------------------------------
    | 1  | "anna"          | NULL     |
    -----------------------------------
    | 1  | ","             | NULL     |
    -----------------------------------
    | 1  | "an"            | NULL     |
    -----------------------------------
    | 1  | "oh-my-god"     | "oh"     |
    -----------------------------------
    | 1  | "oh-my-god"     | "god"    |
    -----------------------------------
    | 1  | "moment"        | "moment" |
    -----------------------------------
    | 1  | "."             | NULL     |
    -----------------------------------

    Args:
        content_tokens (DataFrame): See the example input above.

    Returns:
        Tuple[DataFrame, DataFrame]: See the example output above.
    """
    if "third_party/nltk_data" not in nltk.data.path:
        nltk.data.path.append("third_party/nltk_data")

    PUNCT = re.compile(r'[%s\s‐‘’“”–—…]+' % re.escape(string.punctuation))
    STOP_WORDS = set(stopwords.words("english"))

    def GetTerm(row: Row) -> Iterable[Row]:
        stemmer = PorterStemmer()

        words = PUNCT.split(row["token"])
        for word in words:
            word = word.lower()

            if (word != "") and \
               (word not in STOP_WORDS) and \
               (word not in string.punctuation):
                yield Row(id=row["id"],
                          token=row['token'],
                          term=stemmer.stem(word))

    res1 = content_tokens.  \
        rdd.                \
        flatMap(GetTerm).   \
        toDF()
    res = content_tokens.join(
        res1, ['id', 'token'],
        'leftouter').select(
        'id', 'token', 'term')

    return res


def ComputeIdf(content_tokens_terms: DataFrame) -> DataFrame:
    """Computes the inverse document frequency for the tokens in each piece of
    content.

    A token's IDF is determined by the maximum IDF of its composition term. For
    example, a word token "oh-my-god" consists of 2 terms: oh and god.
    The IDF of the word token is therefore:
        word_idf("oh-my-god") = max(term_idf("oh"), term_idf("god"))

    Example input:
    -----------------------------------
    | id | token           | term     |
    -----------------------------------
    | 1  | "anna"          | NULL     |
    -----------------------------------
    | 1  | ","             | NULL     |
    -----------------------------------
    | 1  | "an"            | NULL     |
    -----------------------------------
    | 1  | "oh-my-god"     | "oh"     |
    -----------------------------------
    | 1  | "oh-my-god"     | "god"    |
    -----------------------------------
    | 1  | "moment"        | "moment" |
    -----------------------------------
    | 1  | "."             | NULL     |
    -----------------------------------

    Intermediate result (term IDF):
    -----------------------------------------
    | id | token           | term     | idf |
    -----------------------------------------
    | 1  | "anna"          | NULL     | 0   |
    -----------------------------------------
    | 1  | ","             | NULL     | 0   |
    -----------------------------------------
    | 1  | "an"            | NULL     | 0   |
    -----------------------------------------
    | 1  | "oh-my-god"     | "oh"     | 0.5 |
    -----------------------------------------
    | 1  | "oh-my-god"     | "god"    | 1.2 |
    -----------------------------------------
    | 1  | "moment"        | "moment" | 0.8 |
    -----------------------------------------
    | 1  | "."             | NULL     | 0   |
    -----------------------------------------

    Example output:
    ------------------------------
    | id | token           | idf |
    ------------------------------
    | 1  | "anna"          | 0   |
    ------------------------------
    | 1  | ","             | 0   |
    ------------------------------
    | 1  | "an"            | 0   |
    ------------------------------
    | 1  | "oh-my-god"     | 1.2 |
    ------------------------------
    | 1  | "moment"        | 0.8 |
    ------------------------------
    | 1  | "."             | 0   |
    ------------------------------


    Args:
        content_tokens_terms (DataFrame): See the example input above.

    Returns:
        DataFrame: See the example output above.
    """
    N = content_tokens_terms.select(F.countDistinct('id')).collect()[0][0]
    interm = content_tokens_terms.\
        join(content_tokens_terms.
             groupby('term').
             # get term frequency
             agg(F.countDistinct('id')), ['term'], 'left')
    interm2 = interm.withColumn('idf', F.log(N/col('count(id)'))).select(
        'id', 'token', 'term', 'count(id)', 'idf')  # get idf for each term
    interm3 = interm2.groupBy(
        'id', 'token').agg(
        F.max("idf").alias('idf'))  # get idf for each token
    interm3.cache()
    # normalize idf for each document so that the sum of idf for each doc is 1.
    interm4 = interm3.groupBy('id').agg(F.sum('idf').alias('doc_total_idf'))
    interm5 = interm3.join(interm4, ['id'], 'left')
    interm5 = interm5.withColumn('idf_scaled', col(
        'idf')/col('doc_total_idf')).select('id', 'token', 'idf_scaled')
    res = interm5.withColumnRenamed('idf_scaled', 'idf').na.fill(0, ['idf'])
    return res
