package ETL

object MovieParser {
  def validate(movie: Movie): List[String] = {
    val errors = scala.collection.mutable.ListBuffer.empty[String]
    if (movie.id < 0) errors += s"id invalide: ${movie.id}"
    if (movie.title.trim.isEmpty) errors += "title manquant ou vide"
    if (movie.year < 1895 || movie.year > 2025) errors += s"year invalide: ${movie.year}"
    if (movie.runtime <= 0) errors += s"runtime invalide: ${movie.runtime}"
    if (movie.genres.isEmpty) errors += "genres manquant ou vide"
    if (movie.director.trim.isEmpty) errors += "director manquant ou vide"
    if (movie.cast.isEmpty) errors += "cast manquant ou vide"
    if (movie.rating < 0.0 || movie.rating > 10.0) errors += s"rating invalide: ${movie.rating}"
    if (movie.votes < 0) errors += s"votes invalide: ${movie.votes}"
    movie.revenue.foreach(r => if (r < 0.0) errors += s"revenue invalide: ${r}")
    movie.budget.foreach(b => if (b < 0.0) errors += s"budget invalide: ${b}")
    if (movie.language.trim.isEmpty) errors += "language manquant ou vide"
    errors.toList
  }
}
