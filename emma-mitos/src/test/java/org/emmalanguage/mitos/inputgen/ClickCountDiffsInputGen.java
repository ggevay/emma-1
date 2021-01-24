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

package org.emmalanguage.mitos.inputgen;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

public class ClickCountDiffsInputGen {

    /**
     * args: path, numProducts, clicksPerDayRatio
     */
    public static void main(String[] args) throws Exception {
        final String pref = args[0] + "/";
        generate(Integer.parseInt(args[1]), 365, pref, new Random(), Double.parseDouble(args[2]));
    }

    public static String generate(int numProducts, int numDays, String pref, Random rnd, double clicksPerDayRatio) throws IOException {

        pref = pref + numProducts + "/";

        final int numClicksPerDay = (int)(numProducts * clicksPerDayRatio);

        final String pageAttributesFile = pref + "in/pageAttributes.tsv";

        new File(pref + "in").mkdirs();
        new File(pref + "out").mkdirs();
        new File(pref + "tmp").mkdirs();

        Writer wr1 = new FileWriter(pageAttributesFile);
        int j = 0;
        for (int i=0; i<numProducts; i++) {
            int type = rnd.nextInt(2);
            wr1.write(i + "\t" + type + "\n");
            if (++j == 1000000) {
                System.out.println(i);
                j = 0;
            }
        }
        wr1.close();

        for (int day = 1; day <= numDays; day++) {
            System.out.println(day);
            Writer wr2 = new FileWriter(pref + "in/clickLog_" + day);
            for (int i=0; i<numClicksPerDay; i++) {
                int click = rnd.nextInt(numProducts);
                wr2.write(click + "\n");
            }
            wr2.close();
        }

        return pref;
    }

//    static public void checkLabyOut(String path, int numDays, int[] expected) throws IOException {
//        for (int i = 2; i <= numDays; i++) {
//            String actString = readFile(path + "/out/laby/diff_" + i, StandardCharsets.UTF_8);
//            int act = Integer.parseInt(actString.trim());
//            if (act != expected[i - 2]) {
//                throw new RuntimeException("ClickCountDiffs output is incorrect on day " + i);
//            }
//        }
//    }
//
//    static public void checkNocflOut(String path, int numDays, int[] expected) throws IOException {
//        for (int i = 2; i <= numDays; i++) {
//            String actString = readFile(path + "/out/flinksep/diff_" + i, StandardCharsets.UTF_8);
//            int act = Integer.parseInt(actString.trim());
//            if (act != expected[i - 2]) {
//                throw new RuntimeException("ClickCountDiffs output is incorrect on day " + i);
//            }
//        }
//    }

    public static final String outPrefLaby = "/out/laby/diff_";
    public static final String outPrefFlinkSep = "/out/flinksep/diff_";

    static public void checkOutput(String path, String outPref, int numDays, int[] expected) throws IOException {
        for (int i = 2; i <= numDays; i++) {
            String actString = readFile(path + outPref + i, StandardCharsets.UTF_8);
            int act = Integer.parseInt(actString.trim());
            if (act != expected[i - 2]) {
                throw new RuntimeException("ClickCountDiffs output is incorrect on day " + i);
            }
        }
    }

    private static String readFile(String path, Charset encoding)
            throws IOException
    {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }
}
