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

package org.emmalanguage.labyrinth.jobsnolaby;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.LongValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;


public class ClickCountDiffsNoJoin {

	private static final Logger LOG = LoggerFactory.getLogger(ClickCountDiffsNoJoin.class);

	// /home/ggevay/Dropbox/cfl_testdata/ClickCount 4
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//env.getConfig().setParallelism(1);

		env.getConfig().enableObjectReuse();

		String pref = args[0] + "/";
		String yesterdayCountsTmpFilename = pref + "tmp/yesterdayCounts";
		String todayCountsTmpFilename = pref + "tmp/todayCounts";
		FileSystem fs = FileSystem.get(new URI(pref));

		final int days = Integer.parseInt(args[1]); // 365
		for (int day = 1; day <= days; day++) {

			LOG.info("### Day " + day);

			DataSet<Tuple1<LongValue>> visits = env.readCsvFile(pref + "in/clickLog_" + day)
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(LongValue.class);

			DataSet<Tuple2<LongValue, LongValue>> counts = visits.map(new MapFunction<Tuple1<LongValue>, Tuple2<LongValue, LongValue>>() {

				Tuple2<LongValue, LongValue> reuse = Tuple2.of(new LongValue(-1),new LongValue(1));

				@Override
				public Tuple2<LongValue, LongValue> map(Tuple1<LongValue> value) throws Exception {
					reuse.f0 = value.f0;
					return reuse;
				}
			}).groupBy(0).sum(1);

			if (day != 1) {

				DataSet<Tuple2<LongValue, LongValue>> yesterdayCounts = env.readCsvFile(yesterdayCountsTmpFilename).types(LongValue.class, LongValue.class);

				DataSet<Tuple1<LongValue>> diffs = counts.fullOuterJoin(yesterdayCounts).where(0).equalTo(0).with(new JoinFunction<Tuple2<LongValue,LongValue>, Tuple2<LongValue,LongValue>, Tuple1<LongValue>>() {

					Tuple2<LongValue, LongValue> nulla = Tuple2.of(new LongValue(0),new LongValue(0));

					Tuple1<LongValue> reuse = Tuple1.of(new LongValue(-1));

					@Override
					public Tuple1<LongValue> join(Tuple2<LongValue, LongValue> first, Tuple2<LongValue, LongValue> second) throws Exception {
						if (first == null) {
							first = nulla;
						}
						if (second == null) {
							second = nulla;
						}
						reuse.f0.setValue(Math.abs(first.f1.getValue() - second.f1.getValue()));
						return reuse;
					}
				});

				diffs.sum(0).map(new MapFunction<Tuple1<LongValue>, String>() {
					@Override
					public String map(Tuple1<LongValue> integerTuple1) throws Exception {
						return integerTuple1.f0.toString();
					}
				}).setParallelism(1).writeAsText(pref + "out_nojoin_flink/diff_" + day, FileSystem.WriteMode.OVERWRITE);
			}

			// Workaround for https://issues.apache.org/jira/browse/FLINK-1268
			fs.delete(new Path(todayCountsTmpFilename), true);

			counts.writeAsCsv(todayCountsTmpFilename, FileSystem.WriteMode.OVERWRITE);

			env.execute();

			// yesterdayPRTmpFilename and todayPRTmpFilename has to be different, because the reading and writing can overlap
			fs.delete(new Path(yesterdayCountsTmpFilename), true);
			fs.rename(new Path(todayCountsTmpFilename), new Path(yesterdayCountsTmpFilename));
		}
	}
}
