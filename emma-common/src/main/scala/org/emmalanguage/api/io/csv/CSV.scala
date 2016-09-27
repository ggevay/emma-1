package org.emmalanguage
package api.io.csv

import api.io.Format

/** CSV format. */
object CSV extends Format {

  /** Supported CSV format options. */
  case class Config
  (
    sep: Char
  )

}
