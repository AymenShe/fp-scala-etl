package ETL

object DataValidator {

  /**
   * Valide un film selon les règles métier
   */
  def isValid(movie: Movie): Boolean = {
    movie.title.nonEmpty &&
    movie.year >= 1895 && movie.year <= 2025 &&
    movie.runtime > 0 &&
    movie.genres.nonEmpty &&
    movie.director.nonEmpty &&
    movie.cast.nonEmpty &&
    movie.rating >= 0.0 && movie.rating <= 10.0 &&
    movie.votes >= 0 &&
    movie.budget.forall(_ >= 0.0) &&
    movie.revenue.forall(_ >= 0.0) &&
    movie.language.nonEmpty
  }

  /**
   * Supprime et filtre les films valides
   */
  def filterValid(movies: List[Movie]): List[Movie] = {
    movies
      .filter(isValid)
      // distinct pour garder 1 doulon par id
      .distinctBy(_.id)
  }
}