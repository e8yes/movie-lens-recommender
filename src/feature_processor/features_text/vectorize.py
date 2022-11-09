from pyspark.sql import DataFrame


def LoadGloveDefinitions(path: str, dimension: int) -> DataFrame:
    """_summary_

    Args:
        path (str): _description_
        dimension (int): _description_

    Returns:
        DataFrame: _description_
    """
    pass


def VectorizeContentTokens(content_tokens: DataFrame,
                           term_idf: DataFrame,
                           glove: DataFrame) -> DataFrame:
    """_summary_

    Args:
        content_tokens (DataFrame): _description_
        term_idf (DataFrame): _description_
        glove (DataFrame): _description_

    Returns:
        DataFrame: _description_
    """
    pass


def VectorizeContentGenomeScores(content_tokens: DataFrame,
                                 term_idf: DataFrame,
                                 glove: DataFrame) -> DataFrame:
    """_summary_

    Args:
        content_tokens (DataFrame): _description_
        term_idf (DataFrame): _description_
        glove (DataFrame): _description_

    Returns:
        DataFrame: _description_
    """
    pass


def VectorizeUserTokens(user_tag_tokens: DataFrame,
                        glove: DataFrame) -> DataFrame:
    """_summary_

    Args:
        user_tag_tokens (DataFrame): _description_
        glove (DataFrame): _description_

    Returns:
        DataFrame: _description_
    """
    pass
