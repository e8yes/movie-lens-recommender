""" Provides access facilities to the movie lens logs.
"""

import numpy as np
import pandas as pd
import os

DATASET_PATH = "datasets"
MOVIE_LENS_DATASET_PATH = os.path.join(DATASET_PATH, "movie_lens")
MOVIE_LENS_GENOME_SCORES = os.path.join(MOVIE_LENS_DATASET_PATH, "genome-scores.csv")
MOVIE_LENS_GENOME_TAGS = os.path.join(MOVIE_LENS_DATASET_PATH, "genome-tags.csv")
MOVIE_LENS_LINKS = os.path.join(MOVIE_LENS_DATASET_PATH, "links.csv")
MOVIE_LENS_MOVIES = os.path.join(MOVIE_LENS_DATASET_PATH, "movies.csv")
MOVIE_LENS_RATINGS = os.path.join(MOVIE_LENS_DATASET_PATH, "ratings.csv")
MOVIE_LENS_TAGS = os.path.join(MOVIE_LENS_DATASET_PATH, "tags.csv")
MOVIE_LENS_TAGS_USER_ID = "userId"
MOVIE_LENS_TAGS_MOVIE_ID = "movieId"
MOVIE_LENS_TAGS_TAG = "tag"
MOVIE_LENS_TAGS_TIMESTAMP = "timestamp"

class GenomeScoreDataset:
    """ Movie-tag relevance dataset.
    """
    def __init__(self):
        self.df_genome_scores = pd.read_csv(MOVIE_LENS_GENOME_SCORES)

    def Data(self):
        return self.df_genome_scores
    
    def MovieId(self):
        return "movieId"
    
    def TagId(self):
        return "tagId"
    
    def Relevance(self):
        return "relevance"

class GenomeTagDataset:
    """ Tags definition.
    """
    def __init__(self):
        self.df_genome_tags = pd.read_csv(MOVIE_LENS_GENOME_TAGS)
    
    def Data(self):
        return self.df_genome_tags
    
    def TagId(self):
        return "tagId"
    
    def Tag(self):
        return "tag"

class LinkDataset:
    """ Links to external movie metadata.
    """
    def __init__(self):
        self.df_links = pd.read_csv(MOVIE_LENS_LINKS)
    
    def Data(self):
        return self.df_links
    
    def MovieId(self):
        return "movieId"
    
    def ImdbId(self):
        return "imdbId"
    
    def TmdbId(self):
        return "tmdbId"

class MovieDataset:
    """ Basic movie metadata.
    """
    def __init__(self):
        self.df_movies = pd.read_csv(MOVIE_LENS_MOVIES)
    
    def Data(self):
        return self.df_movies
    
    def MovieId(self):
        return "movieId"
    
    def Title(self):
        return "title"

class RatingDataset:
    """ User-movie rating logs.
    """
    def __init__(self):
        self.df_ratings = pd.read_csv(MOVIE_LENS_RATINGS)
    
    def Data(self):
        return self.df_ratings
    
    def UserId(self):
        return "userId"
    
    def MovieId(self):
        return "movieId"
    
    def Rating(self):
        return "rating"
    
    def Timestamp(self):
        return "timestamp"

class TagDataset:
    """ User-movie tagging logs.
    """
    def __init__(self):
        self.df_tags = pd.read_csv(MOVIE_LENS_TAGS)
    
    def Data(self):
        return self.df_tags
    
    def UserId(self):
        return "userId"
    
    def MovieId(self):
        return "movieId"
    
    def Tag(self):
        return "tag"
    
    def Timestamp(self):
        return "timestamp"

class MovieLensDataset:
    """ Main wrapper of the Movie Lens dataset.
    """
    def __init__(self):
        self.genome_scores = GenomeScoreDataset()
        self.genome_tags = GenomeTagDataset()
        self.links = LinkDataset()
        self.movies = MovieDataset()
        self.ratings = RatingDataset()
        self.tags = TagDataset()
    
    def GenomeScores(self):
        return self.genome_scores
    
    def GenomeTags(self):
        return self.genome_tags
    
    def Links(self):
        return self.links
    
    def Movies(self):
        return self.movies
    
    def Ratings(self):
        return self.ratings
    
    def Tags(self):
        return self.tags
