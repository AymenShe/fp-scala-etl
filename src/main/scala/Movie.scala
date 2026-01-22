package ETL

case class Movie(
  id: Option[Int],
  title: List[String],
  year: Option[Int],
  runtime: Option[Int],
  genres: List[String],
  director: Option[String],
  cast: List[String],
  rating: Option[Double],
  votes: Option[Int],
  revenue: Option[Double],
  budget : Option[Double],
  language: Option[String] 
)

case class MovieStats(
  total_movies_parsed: Int,
  total_movies_valid: Int,
  parsing_errors: Int,
  duplicates_removed: Int
)

case class MovieSummary(
  title: String,
  year: Int,
  rating: Double,
  votes: Int
)

case class MovieGrossingSummary(
  title: String,
  year: Int,
  rating: Double,
  votes: Int,
  revenue: Double
)

case class ProlificDirector(
  director: String,
  count: Int
)

case class FrequentActor(
  actor: String,
  count: Int
)

case class ProfitableMovies(
  count: Int,
  average_roi: Double
)

case class AnalysisReport(
  statistics: MovieStats,
  top_10_rated: Seq[MovieSummary],
  top_10_by_votes: Seq[MovieSummary],
  highest_grossing: Seq[MovieGrossingSummary],
  most_expensive: Seq[MovieSummary],
  movies_by_decade: Map[String, Int],
  movies_by_genre: Map[String, Int],
  average_rating_by_genre: Map[String, Double],
  average_runtime_by_genre: Map[String, Double],
  most_prolific_directors: Seq[ProlificDirector],
  most_frequent_actors: Seq[FrequentActor],
  profitable_movies: ProfitableMovies
)