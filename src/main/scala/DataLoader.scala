package ETL

import io.circe.generic.auto._
import io.circe.parser._
import scala.io.Source
import scala.util.{Try, Success, Failure}

object DataLoader {

  /**
   * Lit un fichier JSON propre et parse directement les movies
   */
  def loadMovies(filename: String): Either[String, List[Movie]] = {
    val file = Try {
      val source = Source.fromFile(filename)
      try source.mkString
      finally source.close()
    }
    file match {
      case Success(content) =>
        io.circe.parser.decode[List[Movie]](content).left.map(err => s"Erreur de parsing JSON: ${err.getMessage}")
      case Failure(exception) => Left(s"Erreur de lecture du fichier: ${exception.getMessage}")
    }
  }

  /**
   * Lit un fichier JSON potentiellement "dirty" et retourne les films valides + erreurs détaillées par entrée
   */
  def loadMoviesDetailed(filename: String): Either[String, (List[Movie], List[ParsingError])] = {
    val file = Try {
      val source = Source.fromFile(filename)
      try source.mkString
      finally source.close()
    }
    file match {
      case Success(content) =>
        parse(content) match {
          case Left(err) => Left(s"Erreur de parsing JSON: ${err.getMessage}")
          case Right(json) =>
            json.asArray match {
              case None => Left("Le fichier JSON ne contient pas un tableau de films")
              case Some(arr) =>
                val results = arr.zipWithIndex.map { case (elem, idx) =>
                  elem.as[MovieInput] match {
                    case Right(mi) =>
                      MovieParser.fromInput(mi) match {
                        case Right(movie) => Right(movie)
                        case Left(messages) => Left(ParsingError(idx, mi.title.getOrElse(""), messages))
                      }
                    case Left(df) =>
                      Left(ParsingError(idx, "", List(df.getMessage)))
                  }
                }
                val movies = results.collect { case Right(m) => m }.toList
                val errors = results.collect { case Left(e) => e }.toList
                Right((movies, errors))
            }
        }
      case Failure(exception) => Left(s"Erreur de lecture du fichier: ${exception.getMessage}")
    }
  }
}