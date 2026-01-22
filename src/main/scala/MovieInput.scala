package ETL

case class MovieInput(
  id: Option[Int],
  title: Option[String],
  year: Option[Int],
  runtime: Option[Int],
  genres: Option[List[String]],
  director: Option[String],
  cast: Option[List[String]],
  rating: Option[Double],
  votes: Option[Int],
  revenue: Option[Double],
  budget: Option[Double],
  language: Option[String]
)
