package ETL

object MovieParser {
  private def round2(d: Double): Double =
    BigDecimal(d).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

  def fromInput(input: MovieInput): Either[List[String], Movie] = {
    val errors = scala.collection.mutable.ListBuffer.empty[String]

    // Champs optionnels (ne pas compter comme erreur s'ils sont absents)
    val idOpt = input.id.filter(_ >= 0)
    if (input.id.exists(_ < 0)) errors += s"id invalide: ${input.id.get}"

    val votesOpt = input.votes.filter(_ >= 0)
    if (input.votes.exists(_ < 0)) errors += s"votes invalide: ${input.votes.get}"

    val revenueOpt = input.revenue.filter(_ >= 0.0)
    if (input.revenue.exists(_ < 0.0)) errors += s"revenue invalide: ${input.revenue.get}"

    val budgetOpt = input.budget.filter(_ >= 0.0)
    if (input.budget.exists(_ < 0.0)) errors += s"budget invalide: ${input.budget.get}"

    // Champs requis
    val title = input.title.getOrElse("")
    if (title.isEmpty) errors += "title manquant ou vide"

    val year = input.year.getOrElse(-1)
    if (year < 1895 || year > 2025) errors += s"year invalide: $year"

    val runtime = input.runtime.getOrElse(-1)
    if (runtime <= 0) errors += s"runtime invalide: $runtime"

    val genres = input.genres.getOrElse(Nil)
    if (genres.isEmpty) errors += "genres manquant ou vide"

    val director = input.director.getOrElse("")
    if (director.isEmpty) errors += "director manquant ou vide"

    val cast = input.cast.getOrElse(Nil)
    if (cast.isEmpty) errors += "cast manquant ou vide"

    val rating = input.rating.getOrElse(-1.0)
    if (rating < 0.0 || rating > 10.0) errors += s"rating invalide: ${round2(rating)}"

    val language = input.language.getOrElse("")
    if (language.isEmpty) errors += "language manquant ou vide"

    if (errors.nonEmpty) Left(errors.toList)
    else Right(
      Movie(
        id = idOpt,
        title = List(title),
        year = Some(year),
        runtime = Some(runtime),
        genres = genres,
        director = Some(director),
        cast = cast,
        rating = Some(rating),
        votes = votesOpt,
        revenue = revenueOpt,
        budget = budgetOpt,
        language = Some(language)
      )
    )
  }
}
