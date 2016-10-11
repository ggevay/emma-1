package org.emmalanguage.test.schema

object Movies {
  case class ImdbYear(year: Int)
  case class FilmFestWinner(year: Int, title: String, director: String, country: String)
}
