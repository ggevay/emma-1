package org.emmalanguage
package api.io

/** An abstract trait implemented by all supported formats. */
trait Format {

  /** Abstracts the available configuration options for this format. */
  type Config

}
