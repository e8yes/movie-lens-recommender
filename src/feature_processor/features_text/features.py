from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


from src.feature_processor.features_text.corpus import CollectContentText
from src.feature_processor.features_text.corpus import CollectContentTags
from src.feature_processor.features_text.corpus import CollectContentKeywords
from src.feature_processor.features_text.corpus import CollectTerms
from src.feature_processor.features_text.corpus import ComputeIdf
from src.feature_processor.features_text.corpus import TokenizeText
from src.feature_processor.features_text.vectorize import LoadGloveDefinitions
from src.feature_processor.features_text.vectorize import VectorizeTokens
from src.feature_processor.features_text.vectorize import \
    VectorizeTokensWithIdf


def ComputeContentTextFeatures(
        contents: DataFrame, spark: SparkSession) -> DataFrame:
    """_summary_

    Args:
        contents (DataFrame): _description_

    Returns:
        DataFrame: _description_
    """
    glove_100 = LoadGloveDefinitions(
        path="third_party/glove", dimension=100, spark=spark)

    content_text = CollectContentText(
        contents=contents, spell_correction=False)
    content_token = TokenizeText(content_text=content_text)
    content_token_terms = CollectTerms(content_tokens=content_token)
    content_token_idf = ComputeIdf(
        content_tokens_terms=content_token_terms)
    content_summary_embed = VectorizeTokensWithIdf(
        tokens_idf=content_token_idf,
        glove=glove_100,
        output_column_name="summary")

    content_tags = CollectContentTags(
        contents=contents, spell_correction=False)
    tag_tokens = TokenizeText(content_text=content_tags)
    content_tag_embed = VectorizeTokens(
        tokens=tag_tokens, glove=glove_100, output_column_name="tag")

    content_keywords = CollectContentKeywords(
        contents=contents, spell_correction=False)
    keywords_tokens = TokenizeText(content_text=content_keywords)
    content_keyword_embed = VectorizeTokens(
        tokens=keywords_tokens, glove=glove_100, output_column_name="keyword")

    return contents.                            \
        select("id").                           \
        join(content_summary_embed, ["id"]).    \
        join(content_tag_embed, ["id"]).        \
        join(content_keyword_embed, ["id"])
