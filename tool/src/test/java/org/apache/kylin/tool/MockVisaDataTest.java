/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.tool;

import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * Created by xiefan on 17-3-14.
 */
public class MockVisaDataTest {

    private static final String CARDINALITY = "CARDINALITY_KEY";

    private static final String COL_TYPE = "COL_TYPE";

    private static final String COL_NUM = "COL_NUE";

    private static final Random rand = new Random(System.currentTimeMillis());

    private static char[] charAndnum = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();

    private String[] colNames = {"s_1_t", "i_2_v", "s_3_t", "i_4_m", "s_5_s", "s_6_s", "i_7_t",
            "s_8_s", "s_9_t", "s_10_s", "s_11_s", "i_12_v", "i_13_m", "s_14_t", "i_15_m"};
    //actual type of col 4 , col 15 is double

    private List<List<Object>> allData;

    private Properties properties;

    private final String propFile = "src/test/resources/mock_data.properties";

    @Before
    public void before() throws Exception {
        allData = new ArrayList<>();
        properties = new Properties();
        properties.load(new FileInputStream(propFile));
        for (String col : colNames) {
            Map<String, String> parse = parse(col);
            int cardinality = getCardinality(parse.get(CARDINALITY));
            String colNum = parse.get(COL_NUM);
            List<Object> range = new ArrayList<>();
            switch (parse.get(COL_TYPE)) {
                case "s":   //varchar
                    for (int i = 0; i < cardinality; i++) {
                        range.add(mockRandShortStr(rand.nextInt(5)));
                    }
                    allData.add(range);
                    break;
                case "i":  //integer
                    if (colNum.equals("4") || colNum.equals("15")) { //handle special col
                        for (int i = 0; i < cardinality; i++) {
                            range.add(rand.nextInt(1000));
                        }
                    } else {
                        for (int i = 0; i < cardinality; i++) {
                            range.add(rand.nextInt());
                        }
                    }
                    allData.add(range);
                    break;
                case "d":
                    for (int i = 0; i < cardinality; i++) {
                        range.add(rand.nextDouble());
                    }
                    allData.add(range);
                    break;
                default:
                    throw new Exception("Error. Unrecognized datatype");
            }
        }
    }

    @Test
    public void testParse() throws Exception {
        System.out.println(parse(colNames[0]));
        System.out.println(parse(colNames[3]));
        System.out.println(parse(colNames[6]));
        System.out.println(parse(colNames[14]));
    }

    @Test
    public void printRange() throws Exception {
        System.out.println(allData.get(0));
        System.out.println(allData.get(3));
        System.out.println(allData.get(6));
        System.out.println(allData.get(14));
    }

    @Test
    public void testMockOneRow() throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println(mockOneRow());
        }
    }

    @Test
    public void genDataIntoFile() throws Exception {
        String dir = getFilePath();
        int rowNum = getRowNum();
        String file = dir + "output_short_str" + rowNum;
        System.out.println(file);
        File f = new File(file);
        if (f.exists())
            f.delete();
        f.createNewFile();
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f)), 4096);
        for (int i = 0; i < rowNum; i++) {
            bw.write(mockOneRow() + "\n");
        }
        bw.flush();
        bw.close();
    }

    private String mockOneRow() {
        StringBuilder sb = new StringBuilder();
        for (List<Object> range : allData) {
            Object data = range.get(rand.nextInt(range.size()));
            sb.append(data.toString() + "\t");
        }
        return sb.toString();
    }


    private static Map<String, String> parse(String colName) throws Exception {
        Map<String, String> r = new HashMap<>();
        String[] strs = colName.split("_");
        if (strs.length != 3)
            throw new Exception("Parse error.Pls check col name");
        r.put(COL_TYPE, strs[0]);
        r.put(COL_NUM, strs[1]);
        r.put(CARDINALITY, strs[2]);
        return r;
    }

    private static int getCardinality(String cardDesc) throws Exception {
        switch (cardDesc) {
            case "t": //tiny
                return 20;
            case "s": //small
                return 100;
            case "m": //medium
                return 1000;
            case "h": //high
                return 10000;
            case "v": //very high
                return 100000;
            case "u": //ultra higt
                throw new Exception("Unhandle cardinality");
            default:
                throw new Exception("Unhandle cardinality");
        }
    }

    private String getFilePath() throws Exception {
        return properties.getProperty("file_path");
    }

    private int getRowNum() throws Exception {
        String str = properties.getProperty("row_num");
        return Integer.parseInt(str);
    }

    private static String mockRandShortStr(int length) {
        length = length + 1; //ensure not empty
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(charAndnum[rand.nextInt(charAndnum.length)]);
        }
        return sb.toString();
    }

}
