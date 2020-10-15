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

package org.emmalanguage.mitos.jobs;

import org.emmalanguage.mitos.operators.IncMap;
import org.emmalanguage.mitos.CFLConfig;
import org.emmalanguage.mitos.ElementOrEvent;
import org.emmalanguage.mitos.KickoffSource;
import org.emmalanguage.mitos.LabyNode;
import org.emmalanguage.mitos.LabySource;
import org.emmalanguage.mitos.operators.AssertEquals;
import org.emmalanguage.mitos.operators.ConditionNode;
import org.emmalanguage.mitos.operators.SmallerThan;
import org.emmalanguage.mitos.partitioners.Always0;
import org.emmalanguage.mitos.partitioners.Random;
import org.emmalanguage.mitos.util.TupleIntInt;
import org.emmalanguage.mitos.util.Unit;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.emmalanguage.mitos.util.Util;

import java.util.Arrays;

/**
 * // BB 0
 * i = 1
 * do {
 *     // BB 1
 *     i = i + 1
 * } while (i < 100)
 * // BB 2
 * assert i == 100
 */

public class SimpleCF {

	private static TypeSerializer<Integer> integerSer = TypeInformation.of(Integer.class).createSerializer(new ExecutionConfig());
	private static TypeSerializer<Boolean> booleanSer = TypeInformation.of(Boolean.class).createSerializer(new ExecutionConfig());

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		Configuration cfg = new Configuration();
//		cfg.setLong("taskmanager.network.numberOfBuffers", 16384);
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(40, cfg);

		//env.getConfig().setParallelism(1);

		final int n = Integer.parseInt(args[0]);

		PojoTypeInfo.registerCustomSerializer(ElementOrEvent.class, new ElementOrEvent.ElementOrEventSerializerFactory());
		PojoTypeInfo.registerCustomSerializer(TupleIntInt.class, TupleIntInt.TupleIntIntSerializer.class);

		CFLConfig.getInstance().terminalBBId = 2;
		KickoffSource kickoffSrc = new KickoffSource(0,1);
		//KickoffSource kickoffSrc = new KickoffSource(0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2);
		env.addSource(kickoffSrc).addSink(new DiscardingSink<>());


		Integer[] input = new Integer[]{1};

		LabySource<Integer> inputBag = new LabySource<>(env.fromCollection(Arrays.asList(input)), 0, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}));

		LabyNode<Integer, Integer> phi =
				LabyNode.phi("phi", 1, new Random<>(env.getParallelism()), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
						.addInput(inputBag, false);

		LabyNode<Integer, Integer> inced =
				new LabyNode<>("inc-map", new IncMap(), 1, new Random<>(env.getParallelism()), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Integer>>(){}))
						.addInput(phi, true, false);

		phi.addInput(inced, false, true);

		LabyNode<Integer, Boolean> smallerThan =
				new LabyNode<>("smaller-than", new SmallerThan(n), 1, new Always0<>(1), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Boolean>>(){}))
						.addInput(inced, true, false)
						.setParallelism(1);

		LabyNode<Boolean, Unit> exitCond =
				new LabyNode<>("exit-cond", new ConditionNode(1,2), 1, new Always0<>(1), booleanSer, TypeInformation.of(new TypeHint<ElementOrEvent<Unit>>(){}))
						.addInput(smallerThan, true, false)
						.setParallelism(1);

		LabyNode<Integer, Unit> assertEquals =
				new LabyNode<>("Check i == " + n, new AssertEquals<>(n), 2, new Always0<>(1), integerSer, TypeInformation.of(new TypeHint<ElementOrEvent<Unit>>(){}))
					.addInput(inced, false, true)
					.setParallelism(1);


		LabyNode.translateAll(env);

		System.out.println(env.getExecutionPlan());
		Util.executeWithCatch(env);
	}
}
