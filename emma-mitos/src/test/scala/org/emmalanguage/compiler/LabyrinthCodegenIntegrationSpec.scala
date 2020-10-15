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
package compiler

class mitosCodegenIntegrationSpec extends BaseCodegenIntegrationSpec
  with mitosCompilerAware
  with mitosAware {

  import compiler._

  def withBackendContext[T](f: Env => T): T =
    withDefaultFlinkStreamEnv(f)


  "test simple" in withBackendContext(implicit env => {
    verify(u.reify {
      val xs = 1
    })
  })
}
