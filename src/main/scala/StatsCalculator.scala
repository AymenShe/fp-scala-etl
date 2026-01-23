package ETL

object StatsCalculator {

  /**
   * Statistiques générales de parsing et déduplication
   */
  def calculateStats(movies: List[Movie]): MovieStats = {
    val totalParsed = movies.length
    val validMovies = DataValidator.filterValid(movies)
    val totalValid = validMovies.length
    val parsingErrors = totalParsed - totalValid
    val distinctByIdCount = movies.map(_.id).distinct.length
    val duplicatesRemoved = totalParsed - distinctByIdCount

    val stats = MovieStats(
      total_movies_parsed = totalParsed,
      total_movies_valid = totalValid,
      parsing_errors = parsingErrors,
      duplicates_removed = duplicatesRemoved
    )
    // // Logs basiques du pipeline ETL
    // println(s"[ETL] Entrées lues: ${stats.total_movies_parsed}")
    // println(s"[ETL] Entrées valides: ${stats.total_movies_valid}")
    // println(s"[ETL] Erreurs: ${stats.parsing_errors}")
    // println(s"[ETL] Doublons supprimés: ${stats.duplicates_removed}")
    stats
  }

  /**
   * Top N films les mieux notés (filtre par `minVotes`)
   */
  def topRated(movies: List[Movie], n: Int = 10, minVotes: Int = 10000): Seq[MovieSummary] = {
    DataValidator
      .filterValid(movies)
      .filter(_.votes >= minVotes)
      .sortBy(m => (-m.rating, -m.votes))
      .take(n)
      .map(m => MovieSummary(m.title, m.year, m.rating, m.votes))
  }

  /**
   * Top N films par nombre de votes
   */
  def topByVotes(movies: List[Movie], n: Int = 10): Seq[MovieSummary] = {
    DataValidator
      .filterValid(movies)
      .sortBy(m => -m.votes)
      .take(n)
      .map(m => MovieSummary(m.title, m.year, m.rating, m.votes))
  }

  /**
   * Top N box-office (par revenue)
   */
  def highestGrossing(movies: List[Movie], n: Int = 10): Seq[MovieGrossingSummary] = {
    DataValidator
      .filterValid(movies)
      .filter(_.revenue > 0.0)
      .sortBy(m => -m.revenue)
      .take(n)
      .map(m => MovieGrossingSummary(m.title, m.year, m.rating, m.votes, m.revenue))
  }

  /**
   * Top N budgets
   */
  def mostExpensive(movies: List[Movie], n: Int = 10): Seq[MovieBudgetSummary] = {
    DataValidator
      .filterValid(movies)
      .filter(_.budget > 0.0)
      .sortBy(m => -m.budget)
      .take(n)
      .map(m => MovieBudgetSummary(m.title, m.year, m.rating, m.votes, m.budget))
  }

  /**
   * Compte des films par décennie ("1990s", "2000s", ...)
   */
  def moviesByDecade(movies: List[Movie]): Map[String, Int] = {
    def decadeLabel(y: Int): String = {
      val base = (y / 10) * 10
      s"${base}s"
    }
    DataValidator
      .filterValid(movies)
      .groupBy(m => decadeLabel(m.year))
      .view
      .mapValues(_.length)
      .toMap
  }

  /**
   * Compte des films par genre (chaque film peut appartenir à plusieurs genres)
   */
  def moviesByGenre(movies: List[Movie]): Map[String, Int] = {
    DataValidator
      .filterValid(movies)
      .flatMap(_.genres)
      .groupBy(identity)
      .view
      .mapValues(_.length)
      .toMap
  }

  /**
   * Note moyenne par genre
   */
  def averageRatingByGenre(movies: List[Movie]): Map[String, Double] = {
    val byGenre = DataValidator
      .filterValid(movies)
      .flatMap(m => m.genres.map(g => (g, m.rating)))
      .groupBy(_._1)

    byGenre.view.mapValues { xs =>
      val ratings = xs.map(_._2)
      ratings.sum / ratings.length
    }.toMap
  }

  /**
   * Durée moyenne par genre
   */
  def averageRuntimeByGenre(movies: List[Movie]): Map[String, Double] = {
    val byGenre = DataValidator
      .filterValid(movies)
      .flatMap(m => m.genres.map(g => (g, m.runtime)))
      .groupBy(_._1)

    byGenre.view.mapValues { xs =>
      val runtimes = xs.map(_._2.toDouble)
      runtimes.sum / runtimes.length
    }.toMap
  }

  /**
   * Top réalisateurs les plus prolifiques
   */
  def mostProlificDirectors(movies: List[Movie], top: Int = 5): Seq[ProlificDirector] = {
    DataValidator
      .filterValid(movies)
      .groupBy(_.director)
      .view
      .mapValues(_.length)
      .toMap
      .toSeq
      .sortBy{ case (_, count) => -count }
      .take(top)
      .map{ case (dir, count) => ProlificDirector(dir, count) }
  }

  /**
   * Top acteurs les plus fréquents
   */
  def mostFrequentActors(movies: List[Movie], top: Int = 5): Seq[FrequentActor] = {
    DataValidator
      .filterValid(movies)
      .flatMap(_.cast)
      .groupBy(identity)
      .view
      .mapValues(_.length)
      .toMap
      .toSeq
      .sortBy{ case (_, count) => -count }
      .take(top)
      .map{ case (actor, count) => FrequentActor(actor, count) }
  }

  /**
   * Compte des films rentables et ROI moyen (revenue/budget)
   */
  def profitableMovies(movies: List[Movie]): ProfitableMovies = {
    val valid = DataValidator.filterValid(movies)
    val withBudgetAndRevenue = valid.filter(m => m.budget > 0.0 && m.revenue > 0.0)
    val profitable = withBudgetAndRevenue.filter(m => m.revenue > m.budget)
    val count = profitable.length
    val averageRoi = if (profitable.isEmpty) 0.0
      else profitable.map(m => m.revenue / m.budget).sum / count
    ProfitableMovies(count, averageRoi)
  }

  /**
   * Assemble tous les résultats dans un `AnalysisReport`
   */
  def calculateResults(movies: List[Movie]): AnalysisReport = {
    AnalysisReport(
      statistics = calculateStats(movies),
      top_10_rated = topRated(movies, 10),
      top_10_by_votes = topByVotes(movies, 10),
      highest_grossing = highestGrossing(movies, 10),
      most_expensive = mostExpensive(movies, 10),
      movies_by_decade = moviesByDecade(movies),
      movies_by_genre = moviesByGenre(movies),
      average_rating_by_genre = averageRatingByGenre(movies),
      average_runtime_by_genre = averageRuntimeByGenre(movies),
      most_prolific_directors = mostProlificDirectors(movies, 5),
      most_frequent_actors = mostFrequentActors(movies, 5),
      profitable_movies = profitableMovies(movies)
    )
  }
}