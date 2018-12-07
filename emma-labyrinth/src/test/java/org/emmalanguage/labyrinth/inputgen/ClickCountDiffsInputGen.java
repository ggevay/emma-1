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

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

public class ClickCountDiffsInputGen {

    /**
     * args: path, numProducts, clicksPerDayRatio
     */
    public static void main(String[] args) throws Exception {
        String pref = args[0] + "/";
        long numProducts = Long.parseLong(args[1]);
        double clicksPerDayRatio = Double.parseDouble(args[2]);
        int numDays = Integer.parseInt(args[3]);
        generate(numProducts, numDays, pref, new Random(), clicksPerDayRatio);
    }

    public static String generate(long numProducts, int numDays, String pref, Random rnd, double clicksPerDayRatio) throws Exception {

        pref = pref + Long.toString(numProducts) + "/";

        final long numClicksPerDay = (long)(numProducts * clicksPerDayRatio);

        final String pageAttributesFile = pref + "in/pageAttributes.tsv";

        FileSystem fs = FileSystem.get(new URI(pref));
        fs.mkdirs(new Path(pref + "in"));
        fs.mkdirs(new Path(pref + "out"));
        fs.mkdirs(new Path(pref + "tmp"));

        LongBigArrayBigList prodIDs = new LongBigArrayBigList(numProducts);
        PrintWriter wr1 = new PrintWriter(fs.create(new Path(pageAttributesFile), FileSystem.WriteMode.OVERWRITE));
        int j = 0;
        for (long i=0; i<numProducts; i++) {
            long prodID = rnd.nextLong();
            int type = rnd.nextInt(2);
            prodIDs.add(prodID);
            wr1.write(Long.toString(prodID) + "\t" + Integer.toString(type) + "\n");
            if (j++ == 1000000) {
                System.out.println(i);
                j = 0;
            }
        }
        wr1.close();

        for (int day = 1; day <= numDays; day++) {
            System.out.println(day);
            String file = pref + "in/clickLog_" + day;
            PrintWriter wr2 = new PrintWriter(fs.create(new Path(file), FileSystem.WriteMode.OVERWRITE));
            for (int i=0; i<numClicksPerDay; i++) {
                long click = prodIDs.getLong(nextLong(rnd, numProducts));
                wr2.write(Long.toString(click) + "\n");
            }
            wr2.close();
        }

        return pref;
    }

    // https://stackoverflow.com/questions/2546078/java-random-long-number-in-0-x-n-range
    private static long nextLong(Random rng, long n) {
        // error checking and 2^x checking removed for simplicity.
        long bits, val;
        do {
            bits = (rng.nextLong() << 1) >>> 1;
            val = bits % n;
        } while (bits-val+(n-1) < 0L);
        return val;
    }

    // Compares out between the Laby version and the Nolaby (Flink) version.
    static public void checkOut(String path, int numDays) throws IOException {
        for (int i = 2; i <= numDays; i++) {

            String labyString = readFile(path + "/out/diff_" + Long.toString(i), StandardCharsets.UTF_8);
            long laby = Long.parseLong(labyString.trim());

            String flinkString = readFile(path + "/out_flink/diff_" + Long.toString(i), StandardCharsets.UTF_8);
            long flink = Long.parseLong(flinkString.trim());

            String scalaString = readFile(path + "/out_scala/diff_" + Long.toString(i), StandardCharsets.UTF_8);
            long scala = Long.parseLong(scalaString.trim());

            if (laby != flink) {
                throw new RuntimeException("ClickCountDiffs output is incorrect on day " + i);
            }

            if (flink != scala) {
                throw new RuntimeException("ClickCountDiffs_scala output is incorrect on day " + i);
            }
        }
    }


    public static String readFile(String path, Charset encoding) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }
}
