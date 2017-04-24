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

package org.apache.kylin.engine.mr.steps;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.dict.IDictionaryBuilder;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
public class FactDistinctColumnsReducer extends KylinReducer<SelfDefineSortableKey, Text, NullWritable, Text> {

    private static final Logger logger = LoggerFactory.getLogger(FactDistinctColumnsReducer.class);

    private List<TblColRef> columnList;
    private List<Long> baseCuboidRowCountInMappers;
    protected Map<Long, HLLCounter> cuboidHLLMap = null;
    protected long baseCuboidId;
    protected CubeDesc cubeDesc;
    private long totalRowsBeforeMerge = 0;
    private int samplingPercentage;
    private TblColRef col = null;
    private boolean isStatistics = false;
    private KylinConfig cubeConfig;
    private int uhcReducerCount;
    private Map<Integer, Integer> reducerIdToColumnIndex = new HashMap<>();
    private int taskId;
    private boolean isPartitionCol = false;
    private int rowCount = 0;
    private long factTableRowCount = 0;

    //local build dict
    private boolean buildDictInReducer;
    private IDictionaryBuilder builder;
    private long timeMaxValue = Long.MIN_VALUE;
    private long timeMinValue = Long.MAX_VALUE;
    public static final String DICT_FILE_POSTFIX = ".rldict";
    public static final String PARTITION_COL_INFO_FILE_POSTFIX = ".pci";


    private MultipleOutputs mos;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        Configuration conf = context.getConfiguration();
        mos = new MultipleOutputs(context);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeConfig = cube.getConfig();
        cubeDesc = cube.getDescriptor();
        columnList = CubeManager.getInstance(config).getAllDictColumnsOnFact(cubeDesc);

        boolean collectStatistics = Boolean.parseBoolean(conf.get(BatchConstants.CFG_STATISTICS_ENABLED));
        int numberOfTasks = context.getNumReduceTasks();
        taskId = context.getTaskAttemptID().getTaskID().getId();

        uhcReducerCount = cube.getConfig().getUHCReducerCount();
        initReducerIdToColumnIndex(config);

        if (collectStatistics && (taskId == numberOfTasks - 1)) {
            // hll
            isStatistics = true;
            baseCuboidRowCountInMappers = Lists.newArrayList();
            cuboidHLLMap = Maps.newHashMap();
            samplingPercentage = Integer.parseInt(context.getConfiguration().get(BatchConstants.CFG_STATISTICS_SAMPLING_PERCENT));
            logger.info("Reducer " + taskId + " handling stats");
        } else if (collectStatistics && (taskId == numberOfTasks - 2)) {
            // partition col
            isPartitionCol = true;
            col = cubeDesc.getModel().getPartitionDesc().getPartitionDateColumnRef();
            if (col == null) {
                logger.info("No partition col. This reducer will do nothing");
            } else {
                logger.info("Reducer " + taskId + " handling partition col " + col.getIdentity());
            }
        } else {
            // normal col
            col = columnList.get(reducerIdToColumnIndex.get(taskId));
            Preconditions.checkNotNull(col);

            // local build dict
            buildDictInReducer = config.isBuildDictInReducerEnabled();
            if (cubeDesc.getDictionaryBuilderClass(col) != null) { // only works with default dictionary builder
                buildDictInReducer = false;
            }
            if (config.getUHCReducerCount() > 1) {
                int[] uhcIndex = CubeManager.getInstance(config).getUHCIndex(cubeDesc);
                int colIndex = reducerIdToColumnIndex.get(taskId);
                if (uhcIndex[colIndex] == 1)
                    buildDictInReducer = false; //for UHC columns, this feature should be disabled
            }
            if (buildDictInReducer) {
                builder = DictionaryGenerator.newDictionaryBuilder(col.getType());
                builder.init(null, 0);
            }
            logger.info("Reducer " + taskId + " handling column " + col + ", buildDictInReducer=" + buildDictInReducer);
        }
    }

    private void initReducerIdToColumnIndex(KylinConfig config) throws IOException {
        int[] uhcIndex = CubeManager.getInstance(config).getUHCIndex(cubeDesc);
        int count = 0;
        for (int i = 0; i < uhcIndex.length; i++) {
            reducerIdToColumnIndex.put(count * (uhcReducerCount - 1) + i, i);
            if (uhcIndex[i] == 1) {
                for (int j = 1; j < uhcReducerCount; j++) {
                    reducerIdToColumnIndex.put(count * (uhcReducerCount - 1) + j + i, i);
                }
                count++;
            }
        }
    }

    @Override
    public void doReduce(SelfDefineSortableKey skey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text key = skey.getText();
        if (isStatistics) {
            // for hll
            if (isMapperRowCounter(key)) {
                for (Text value : values) {
                    long count = Bytes.toLong(value.getBytes());
                    factTableRowCount += count;
                }
            } else {
                long cuboidId = Bytes.toLong(key.getBytes(), 2, Bytes.SIZEOF_LONG);
                for (Text value : values) {
                    HLLCounter hll = new HLLCounter(cubeConfig.getCubeStatsHLLPrecision());
                    ByteBuffer bf = ByteBuffer.wrap(value.getBytes(), 0, value.getLength());
                    hll.readRegisters(bf);

                    totalRowsBeforeMerge += hll.getCountEstimate();

                    if (cuboidId == baseCuboidId) {
                        baseCuboidRowCountInMappers.add(hll.getCountEstimate());
                    }

                    if (cuboidHLLMap.get(cuboidId) != null) {
                        cuboidHLLMap.get(cuboidId).merge(hll);
                    } else {
                        cuboidHLLMap.put(cuboidId, hll);
                    }
                }
            }
        } else if (isPartitionCol) {
            // partition col
            String value = Bytes.toString(key.getBytes(), 1, key.getLength() - 1);
            logAFewRows(value);
            long time = DateFormat.stringToMillis(value);
            timeMinValue = Math.min(timeMinValue, time);
            timeMaxValue = Math.max(timeMaxValue, time);
        } else {
            // normal col
            if (buildDictInReducer) {
                String value = Bytes.toString(key.getBytes(), 1, key.getLength() - 1);
                logAFewRows(value);
                builder.addValue(value);
            } else {
                byte[] keyBytes = Bytes.copy(key.getBytes(), 1, key.getLength() - 1);
                // output written to baseDir/colName/-r-00000 (etc)
                String fileName = col.getIdentity() + "/";
                mos.write(BatchConstants.CFG_OUTPUT_COLUMN, NullWritable.get(), new Text(keyBytes), fileName);
            }
        }

        rowCount++;
    }

    private boolean isMapperRowCounter(Text key) {
        byte mark = key.getBytes()[1];
        if (mark == FactDistinctColumnsMapper.MARK_FOR_FACT_TABLE_ROW_COUNT)
            return true;
        return false;
    }

    private void logAFewRows(String value) {
        if (rowCount < 10) {
            logger.info("Received value: " + value);
        }
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        if (isStatistics) {
            //output the hll info;
            List<Long> allCuboids = Lists.newArrayList();
            allCuboids.addAll(cuboidHLLMap.keySet());
            Collections.sort(allCuboids);

            logMapperAndCuboidStatistics(allCuboids); // for human check
            outputStatistics(allCuboids);
        } else if (isPartitionCol) {
            // partition col
            outputPartitionInfo();
        } else {
            // normal col
            if (buildDictInReducer) {
                Dictionary<String> dict = builder.build();
                outputDict(col, dict);
            }
        }

        mos.close();
    }

    private void outputPartitionInfo() throws IOException, InterruptedException {
        if (col != null) {
            // output written to baseDir/colName/colName.pci-r-00000 (etc)
            String partitionFileName = col.getIdentity() + "/" + col.getName() + PARTITION_COL_INFO_FILE_POSTFIX;

            mos.write(BatchConstants.CFG_OUTPUT_PARTITION, NullWritable.get(), new LongWritable(timeMinValue), partitionFileName);
            mos.write(BatchConstants.CFG_OUTPUT_PARTITION, NullWritable.get(), new LongWritable(timeMaxValue), partitionFileName);
            logger.info("write partition info for col : " + col.getName() + "  minValue:" + timeMinValue + " maxValue:" + timeMaxValue);
        }
    }

    private void outputDict(TblColRef col, Dictionary<String> dict) throws IOException, InterruptedException {
        // output written to baseDir/colName/colName.rldict-r-00000 (etc)
        String dictFileName = col.getIdentity() + "/" + col.getName() + DICT_FILE_POSTFIX;

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream outputStream = new DataOutputStream(baos);) {
            outputStream.writeUTF(dict.getClass().getName());
            dict.write(outputStream);

            mos.write(BatchConstants.CFG_OUTPUT_DICT, NullWritable.get(), new BytesWritable(baos.toByteArray()), dictFileName);
        }
    }

    private void outputStatistics(List<Long> allCuboids) throws IOException, InterruptedException {
        // output written to baseDir/statistics/statistics-r-00000 (etc)
        String statisticsFileName = BatchConstants.CFG_OUTPUT_STATISTICS + "/" + BatchConstants.CFG_OUTPUT_STATISTICS;

        ByteBuffer valueBuf = ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE);

        // mapper overlap ratio at key -1
        long grandTotal = 0;
        for (HLLCounter hll : cuboidHLLMap.values()) {
            grandTotal += hll.getCountEstimate();
        }
        double mapperOverlapRatio = grandTotal == 0 ? 0 : (double) totalRowsBeforeMerge / grandTotal;
        mos.write(BatchConstants.CFG_OUTPUT_STATISTICS, new LongWritable(-1), new BytesWritable(Bytes.toBytes(mapperOverlapRatio)), statisticsFileName);

        // mapper number at key -2
        mos.write(BatchConstants.CFG_OUTPUT_STATISTICS, new LongWritable(-2), new BytesWritable(Bytes.toBytes(baseCuboidRowCountInMappers.size())), statisticsFileName);

        // mapper total row count at key -3
        mos.write(BatchConstants.CFG_OUTPUT_STATISTICS, new LongWritable(-3), new BytesWritable(Bytes.toBytes(factTableRowCount)), statisticsFileName);

        // sampling percentage at key 0
        mos.write(BatchConstants.CFG_OUTPUT_STATISTICS, new LongWritable(0L), new BytesWritable(Bytes.toBytes(samplingPercentage)), statisticsFileName);

        for (long i : allCuboids) {
            valueBuf.clear();
            cuboidHLLMap.get(i).writeRegisters(valueBuf);
            valueBuf.flip();
            mos.write(BatchConstants.CFG_OUTPUT_STATISTICS, new LongWritable(i), new BytesWritable(valueBuf.array(), valueBuf.limit()), statisticsFileName);
        }
    }

    private void logMapperAndCuboidStatistics(List<Long> allCuboids) throws IOException {
        logger.info("Total cuboid number: \t" + allCuboids.size());
        logger.info("Samping percentage: \t" + samplingPercentage);
        logger.info("The following statistics are collected based on sampling data.");
        logger.info("Number of Mappers: " + baseCuboidRowCountInMappers.size());

        for (int i = 0; i < baseCuboidRowCountInMappers.size(); i++) {
            if (baseCuboidRowCountInMappers.get(i) > 0) {
                logger.info("Base Cuboid in Mapper " + i + " row count: \t " + baseCuboidRowCountInMappers.get(i));
            }
        }

        long grantTotal = 0;
        for (long i : allCuboids) {
            grantTotal += cuboidHLLMap.get(i).getCountEstimate();
            logger.info("Cuboid " + i + " row count is: \t " + cuboidHLLMap.get(i).getCountEstimate());
        }

        logger.info("Sum of all the cube segments (before merge) is: \t " + totalRowsBeforeMerge);
        logger.info("After merge, the cube has row count: \t " + grantTotal);
        if (grantTotal > 0) {
            logger.info("The mapper overlap ratio is: \t" + totalRowsBeforeMerge / grantTotal);
        }
    }

}
