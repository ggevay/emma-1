/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage
package runtime

import com.typesafe.scalalogging.slf4j.Logger
import org.apache.flink.core.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import java.net.URI
import java.util.UUID

/** Provides paths for persisting DataSets, and deletes all the files automatically on exit. */
private[emmalanguage] object TempResultsManager {

  private val log = Logger(LoggerFactory.getLogger(this.getClass))

  private val sessionID: UUID = UUID.randomUUID

  // get the path where we will place persisted DataSets
  private val sessionDir = {
    val tempDir = sys.props.getOrElse("emma.persist.dir", s"file:///${sys.props("java.io.tmpdir")}/emma/persist") + "/"
    new URI(tempDir).normalize()
      .resolve(s"$sessionID/")
  }

  private val fs = FileSystem.get(sessionDir)

  // initialization
  {
    log.info(s"Initializing persist directory for $sessionID at $sessionDir")
    fs.mkdirs(new Path(sessionDir))
  }

  // cleanup
  sys.addShutdownHook {
    log.info(s"Removing temp results for session $sessionID")
    fs.delete(new Path(sessionDir), true)
  }

  def newTempFilePath(): String = {
    val uuid = UUID.randomUUID()
    sessionDir.resolve(uuid.toString).toString
  }
}
