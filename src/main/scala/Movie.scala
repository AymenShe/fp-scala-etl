package ETL

case class Movie(
  id: Int,
  title: String,
  year: Int,
  runtime: Int,
  genres: List[String],
  director: String,
  cast: List[String],
  rating: Double
  votes: Int
  revenue: Double,
  budget : Double,
  language: String 
)