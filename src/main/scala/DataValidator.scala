package ETL

object DataValidator {

  /**
   * Valide un film selon les règles métier
   */
  def isValid(movie: Movie): Boolean = {
    // TODO: Vérifier que :
    //   - title est non vide
    //   - year est non vide entre 1895 et 2025
    //   - runtime est > 0
    //   - genres est non vide
    //   - director est non vide
    //   - cast est non vide
    //   - rating est entre 0.0 et 10.0
    //   - votes est >= 0
    //   - revenue est >= 0.0
    //   - budget est >= 0.0 et non null
    //   - language est non vide
    movie.title.nonEmpty &&
    movie.year >= 1895 && movie.year <= 2025 &&
    movie.runtime > 0 &&
    movie.genres.nonEmpty &&
    movie.director.nonEmpty &&
    movie.cast.nonEmpty &&
    movie.rating >= 0.0 && movie.rating <= 10.0 &&
    movie.votes >= 0 &&
    movie.budget >= 0.0 &&
    movie.revenue >= 0.0 &&
    movie.language.nonEmpty
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