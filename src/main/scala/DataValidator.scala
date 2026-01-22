package ETL

object DataValidator {

  /**
   * Valide un film selon les règles métier
   */
  def isValid(movie: Movie): Boolean = {
    // TODO: Vérifier que :
    //   - title est non vide
    //   - year est non vide et > 1800
    //   - runtime est > 0
    //   - genres est non vide
    //   - director est non vide
    //   - cast est non vide
    //   - rating est entre 0.0 et 10.0
    //   - votes est >= 0
    //   - revenue est >= 0.0
    //   - language est non vide
    movie.title.nonEmpty &&
    movie.year > 1800 &&
    movie.runtime > 0 &&
    movie.genres.nonEmpty &&
    movie.director.nonEmpty &&
    movie.cast.nonEmpty &&
    movie.rating >= 0.0 && movie.rating <= 10.0 &&
    movie.votes >= 0 &&
    movie.revenue >= 0.0 &&
    movie.language.nonEmpty
  }

  /**
   * Filtre les films valides
   */
  def filterValid(movies: List[Movie]): List[Movie] = {
    // TODO: Utiliser filter avec isValid
    movies.filter(isValid) 
  }
}