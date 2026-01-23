package project-etl

import munit.FunSuite
import Main._
import java.nio.file.{Files, Paths}
import java.io.File

  test("readPersonFromFile: lit un fichier") {
    val result = readJsonFile("data/data_clean.json")
    println(result)
  }