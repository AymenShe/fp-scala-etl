package ETL

object StatsCalculator {
  private def round2(d: Double): Double =
    BigDecimal(d).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

  /**
   * Statistiques générales de parsing et déduplication
   */
  def calculateStats(load: LoadResult): MovieStats = {
    val validMovies = load.movies.filter(DataValidator.isValid)
    val totalValidDistinct = validMovies.distinctBy(_.id).length
    val validationErrors = load.movies.length - validMovies.length
    val parsingErrors = load.decodingErrors + validationErrors
    val duplicatesRemoved = validMovies.length - validMovies.map(_.id).distinct.length

    MovieStats(
      total_movies_parsed = load.totalEntries,
      total_movies_valid = totalValidDistinct,
      parsing_errors = parsingErrors,
      duplicates_removed = duplicatesRemoved
    )
  }

  /**
   * Top N films les mieux notés (filtre par `minVotes`)
   */
  def topRated(movies: List[Movie], n: Int = 10, minVotes: Int = 10000): Seq[MovieSummary] = {
    DataValidator
      .filterValid(movies)
      .filter(m => m.votes >= minVotes)
      .sortBy(m => (-m.rating, -m.votes))
      .take(n)
      .map(m => MovieSummary(
        m.title,
        m.year,
        round2(m.rating),
        m.votes
      ))
  }

  /**
   * Top N films par nombre de votes
   */
  def topByVotes(movies: List[Movie], n: Int = 10): Seq[MovieSummary] = {
    DataValidator
      .filterValid(movies)
      .sortBy(m => -m.votes)
      .take(n)
      .map(m => MovieSummary(
        m.title,
        m.year,
        round2(m.rating),
        m.votes
      ))
  }

  /**
   * Top N box-office (par revenue)
   */
  def highestGrossing(movies: List[Movie], n: Int = 10): Seq[MovieGrossingSummary] = {
    DataValidator
      .filterValid(movies)
      .filter(m => m.revenue.exists(_ > 0.0))
      .sortBy(m => -m.revenue.getOrElse(0.0))
      .take(n)
      .map(m => MovieGrossingSummary(
        m.title,
        m.year,
        round2(m.rating),
        m.votes,
        round2(m.revenue.getOrElse(0.0))
      ))
  }

  /**
   * Top N budgets
   */
  def mostExpensive(movies: List[Movie], n: Int = 10): Seq[MovieSummary] = {
    DataValidator
      .filterValid(movies)
      .filter(m => m.budget.exists(_ > 0.0))
      .sortBy(m => -m.budget.getOrElse(0.0))
      .take(n)
      .map(m => MovieSummary(
        m.title,
        m.year,
        round2(m.rating),
        m.votes
      ))
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
      .map(m => decadeLabel(m.year))
      .groupBy(identity)
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
    val pairs: List[(String, Double)] = DataValidator
      .filterValid(movies)
      .flatMap(m => m.genres.map(g => (g, m.rating)))
    val byGenre = pairs.groupBy(_._1)
    byGenre.view.mapValues { xs =>
      val ratings = xs.map(_._2)
      round2(ratings.sum / ratings.length)
    }.toMap
  }

  /**
   * Durée moyenne par genre
   */
  def averageRuntimeByGenre(movies: List[Movie]): Map[String, Double] = {
    val pairs: List[(String, Double)] = DataValidator
      .filterValid(movies)
      .flatMap(m => m.genres.map(g => (g, m.runtime.toDouble)))
    val byGenre = pairs.groupBy(_._1)
    byGenre.view.mapValues { xs =>
      val runtimes = xs.map(_._2)
      round2(runtimes.sum / runtimes.length)
    }.toMap
  }

  /**
   * Top réalisateurs les plus prolifiques
   */
  def mostProlificDirectors(movies: List[Movie], top: Int = 5): Seq[ProlificDirector] = {
    DataValidator
      .filterValid(movies)
      .map(_.director)
      .groupBy(identity)
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
    val withBudgetAndRevenue = valid.filter(m => m.budget.exists(_ > 0.0) && m.revenue.exists(_ > 0.0))
    val profitable = withBudgetAndRevenue.filter(m => m.revenue.get > m.budget.get)
    val count = profitable.length
    val averageRoi = if (profitable.isEmpty) 0.0
      else profitable.map(m => m.revenue.get / m.budget.get).sum / count
    ProfitableMovies(count, round2(averageRoi))
  }

  /**
   * Assemble tous les résultats dans un `AnalysisReport`
   */
  def calculateResults(load: LoadResult): AnalysisReport = {
    val movies = load.movies
    AnalysisReport(
      statistics = calculateStats(load),
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