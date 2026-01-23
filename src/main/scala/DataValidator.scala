package ETL

object DataValidator {

  /**
   * Valide un film selon les règles métier
   */
  def isValid(movie: Movie): Boolean = {
    movie.title.nonEmpty &&
    movie.year.exists(y => y >= 1895 && y <= 2025) &&
    movie.runtime.exists(_ > 0) &&
    movie.genres.nonEmpty &&
    movie.director.exists(_.nonEmpty) &&
    movie.cast.nonEmpty &&
    movie.rating.exists(r => r >= 0.0 && r <= 10.0) &&
    movie.votes.forall(_ >= 0) &&
    movie.budget.forall(_ >= 0.0) &&
    movie.revenue.forall(_ >= 0.0) &&
    movie.language.exists(_.nonEmpty)
  }

  /**
   * Supprime et filtre les films valides
   */
  def filterValid(movies: List[Movie]): List[Movie] = {
    movies
      .filter(isValid)
      .distinctBy(_.id)
  }
}