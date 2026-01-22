package ETL

object MovieParser {
  def fromInput(input: MovieInput): Either[List[String], Movie] = {
    val errors = scala.collection.mutable.ListBuffer.empty[String]

    val id = input.id.getOrElse({ errors += "id manquant"; -1 })
    val title = input.title.getOrElse({ errors += "title manquant ou vide"; "" })
    val year = input.year.getOrElse({ errors += "year manquant"; -1 })
    val runtime = input.runtime.getOrElse({ errors += "runtime manquant"; -1 })
    val genres = input.genres.getOrElse({ errors += "genres manquant"; Nil })
    val director = input.director.getOrElse({ errors += "director manquant ou vide"; "" })
    val cast = input.cast.getOrElse({ errors += "cast manquant"; Nil })
    val rating = input.rating.getOrElse({ errors += "rating manquant"; -1.0 })
    val votes = input.votes.getOrElse({ errors += "votes manquant"; -1 })
    val revenue = input.revenue.getOrElse({ errors += "revenue manquant"; -1.0 })
    val budget = input.budget.getOrElse({ errors += "budget manquant"; -1.0 })
    val language = input.language.getOrElse({ errors += "language manquant ou vide"; "" })

    // Règles métier (détaillées pour messages clairs)
    if (title.isEmpty) errors += "title vide"
    if (year < 1895 || year > 2025) errors += s"year invalide: $year"
    if (runtime <= 0) errors += s"runtime invalide: $runtime"
    if (genres.isEmpty) errors += "genres vide"
    if (director.isEmpty) errors += "director vide"
    if (cast.isEmpty) errors += "cast vide"
    if (rating < 0.0 || rating > 10.0) errors += s"rating invalide: $rating"
    if (votes < 0) errors += s"votes invalide: $votes"
    if (revenue < 0.0) errors += s"revenue invalide: $revenue"
    if (budget < 0.0) errors += s"budget invalide: $budget"
    if (language.isEmpty) errors += "language vide"

    if (errors.nonEmpty) Left(errors.toList)
    else Right(
      Movie(
        id = id,
        title = title,
        year = year,
        runtime = runtime,
        genres = genres,
        director = director,
        cast = cast,
        rating = rating,
        votes = votes,
        revenue = revenue,
        budget = budget,
        language = language
      )
    )
  }
}
