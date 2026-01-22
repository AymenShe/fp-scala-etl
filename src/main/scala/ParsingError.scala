package ETL

case class ParsingError(
  index: Int,
  titleHint: String,
  messages: List[String]
)
