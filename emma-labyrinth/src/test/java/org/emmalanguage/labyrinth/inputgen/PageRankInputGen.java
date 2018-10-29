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

package org.emmalanguage.labyrinth.inputgen;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

/**
 * Creates numDays smaller graphs from a large graph, by filtering out edges randomly
 *
 * args: path, numDays, clicksPerDayRatio
 * Note: clicksPerDayRatio is actually the reciprocal, so 10 means every 10th edge is chosen
 *
 * random fullGraph-hoz meg ket arg kell: numVertices, numEdges
 */
public class PageRankInputGen {

    public static void main(String[] args) throws Exception {
        final String pref = args[0] + "/";

        if (args.length == 3) {
            generate(pref, Integer.parseInt(args[1]), Integer.parseInt(args[2]), false, -1, -1);
        } else {
            generate(pref, Integer.parseInt(args[1]), Integer.parseInt(args[2]), true, Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        }
    }

    public static void generate(String pref, int numDays, int clicksPerDayRatio, boolean randomFullGraph, int numVertices, int numEdges) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String fullGraphPath = pref + "/fullGraph";
        String inputPath = pref + "/input";

        DataSet<Tuple2<Integer, Integer>> fullGraph;

        if (!randomFullGraph) {
            fullGraph = env.readCsvFile(fullGraphPath)
                    .fieldDelimiter("\t")
                    .lineDelimiter("\n")
                    .types(Integer.class, Integer.class);
        } else {
            fullGraph = env.fromCollection(new RandomGraphIterator(numVertices, numEdges), TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}));
        }

        int day = 1;
        final int blockSize = 10;
        for (int i = 0; i < numDays / blockSize; i++) {
            for (int j = 0; j < blockSize; j++) {
                doDay(fullGraph, clicksPerDayRatio, inputPath, day);
                day++;
            }
            env.execute();
        }
        int j;
        for (j = 0; j < numDays % blockSize; j++) {
            doDay(fullGraph, clicksPerDayRatio, inputPath, day);
            day++;
        }
        if (j > 0) {
            env.execute();
        }
    }

    private static void doDay(DataSet<Tuple2<Integer, Integer>> fullGraph, int clicksPerDayRatio, String inputPath, Integer day) {
        DataSet<Tuple2<Integer, Integer>> filtered = fullGraph.filter(new RichFilterFunction<Tuple2<Integer, Integer>>() {

            Random rnd;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                rnd = new Random();
            }

            @Override
            public boolean filter(Tuple2<Integer, Integer> value) throws Exception {
                return rnd.nextInt(clicksPerDayRatio) == 0;
            }
        })
                .setParallelism(1);

        filtered.writeAsCsv(inputPath + "/" + day.toString(), "\n", "\t", FileSystem.WriteMode.OVERWRITE);
    }

    private static class RandomGraphIterator implements Iterator<Tuple2<Integer, Integer>>, Serializable {

        private int n;
        private final int numVertices;
        private final Random rnd = new Random();

        public RandomGraphIterator(int numVertices, int numEdges) {
            this.numVertices = numVertices;
            this.n = numEdges;
        }

        @Override
        public boolean hasNext() {
            return n > 0;
        }

        @Override
        public Tuple2<Integer, Integer> next() {
            Tuple2<Integer, Integer> ret = Tuple2.of(rnd.nextInt(numVertices), rnd.nextInt(numVertices));
            n--;
            return ret;
        }
    }

    // =============================================
    // Single-threaded version (allows for setting the random seed)
    public static void generateWithSeed(String pref, int numDays, int clicksPerDayRatio, int numVertices, int numEdges, int rndSeed) throws Exception {
        System.out.print("Generating PageRank input data... ");

        Random rnd = new Random(rndSeed);

        String inputPath = pref + "/input";
        boolean res = new File(inputPath).mkdirs();
        if (!res)
            throw new RuntimeException();

        ArrayList<Tuple2<Integer, Integer>> fullGraph = new ArrayList<>();
        for (int i = 0; i < numEdges; i++) {
            fullGraph.add(Tuple2.of(rnd.nextInt(numVertices), rnd.nextInt(numVertices)));
        }

        for (int day = 1; day <= numDays; day++) {
            Path dayPath = Paths.get(inputPath + "/" + Integer.toString(day));
            PrintWriter writer = new PrintWriter(Files.newBufferedWriter(dayPath, StandardCharsets.UTF_8));
            for (Tuple2<Integer, Integer> e: fullGraph) {
                if (rnd.nextInt(clicksPerDayRatio) == 0) {
                    writer.println(Integer.toString(e.f0) + '\t' + e.f1);
                }
            }
            writer.close();
        }

        System.out.println("Done.");
    }

    static public void checkLabyOut(String path, int numDays, double[] expected) throws IOException {
        System.out.print("Checking PageRank output... ");

        for (int day = 2; day <= numDays; day++) {
            String actString = ClickCountDiffsInputGen.readFile(path + "/out/diff_" + Integer.toString(day), StandardCharsets.UTF_8);
            double act = Double.parseDouble(actString.trim());
            double exp = expected[day - 2];
            if (act != exp) {
                throw new RuntimeException("PageRankDiffs output is incorrect on day " + day + ". actual: " + act + ", expected: " + exp);
            }
        }

        System.out.println("Correct!");
    }
}
