from src.modeling.models.pointwise_ranking_model import PointwiseRankingModel
from src.modeling.models.model_matrix_fact import \
    CreateMatrixFactorizationModel
from src.modeling.models.model_featured_user_content import \
    CreateFeaturedUserContentModel


class RankingModelFactory:
    """Facilitates the creation of various ranking models.
    """

    def __init__(self,
                 model: str,
                 user_count: int,
                 content_count: int,
                 user_latent_dim: int,
                 content_latent_dim: int) -> None:
        """Constructs a ranking model factory.

        Args:
            model (str): The name of the model to create. Value can be:
                PointwiseMatrixFactorization
                PointwiseFeaturedUserContent
            user_count (int): The total number of users in the entire data set.
            content_count (int): The total number of pieces of contents in the
                entire data set.
            user_latent_dim (int): The number of dimensions to use to
                represent a user.
            content_latent_dim (int): The number of dimensions to use to
                represent a piece of content.
        """
        self.model = model
        self.user_count = user_count
        self.content_count = content_count
        self.user_latent_dim = user_latent_dim
        self.content_latent_dim = content_latent_dim

    def Create(self) -> PointwiseRankingModel:
        if self.model == "PointwiseMatrixFactorization":
            return CreateMatrixFactorizationModel(
                user_count=self.user_count,
                content_count=self.content_count,
                user_latent_dim=self.user_latent_dim,
                content_latent_dim=self.content_latent_dim)
        elif self.model == "PointwiseFeaturedUserContent":
            return CreateFeaturedUserContentModel(
                user_count=self.user_count,
                user_latent_dim=self.user_latent_dim)
        else:
            raise "Unknown model " + self.model
