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

package org.emmalanguage.labyrinth.operators;

public class Union<T> extends BagOperator<T, T> {

    private final boolean[] open = new boolean[2];

    @Override
    public void openInBag(int logicalInputId) {
        super.openInBag(logicalInputId);
        open[0] = true;
        open[1] = true;
    }

    @Override
    public void pushInElement(T e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        out.collectElement(e);
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        assert open[inputId];
        open[inputId] = false;
        if (!open[0] && !open[1]) {
            out.closeBag();
        }
    }
}
