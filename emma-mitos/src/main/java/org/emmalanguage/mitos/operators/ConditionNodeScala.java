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

package org.emmalanguage.mitos.operators;

import org.emmalanguage.mitos.util.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConditionNodeScala extends SingletonBagOperator<scala.Boolean, Unit> {

	protected static final Logger LOG = LoggerFactory.getLogger(ConditionNodeScala.class);

	private final int[] trueBranchBbIds;
	private final int[] falseBranchBbIds;

	public ConditionNodeScala(int trueBranchBbId, int falseBranchBbId) {
		this(new int[]{trueBranchBbId}, new int[]{falseBranchBbId});
	}

	public ConditionNodeScala(int[] trueBranchBbIds, int[] falseBranchBbIds) {
		this.trueBranchBbIds = trueBranchBbIds;
		this.falseBranchBbIds = falseBranchBbIds;
	}

	@Override
	public void pushInElement(scala.Boolean e, int logicalInputId) {
		super.pushInElement(e, logicalInputId);
		out.appendToCfl(scala.Boolean.unbox(e) ? trueBranchBbIds : falseBranchBbIds);
	}
}
