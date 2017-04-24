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

package org.apache.kylin.engine.mr.common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.SumHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * This should be in cube module. It's here in engine-mr because currently stats
 * are saved as sequence files thus a hadoop dependency.
 */
public class CubeStatsReader {

    private static final Logger logger = LoggerFactory.getLogger(CubeStatsReader.class);

    final CubeSegment seg;
    final int samplingPercentage;
    final int mapperNumberOfFirstBuild; // becomes meaningless after merge
    final double mapperOverlapRatioOfFirstBuild; // becomes meaningless after merge
    final Map<Long, HLLCounter> cuboidRowEstimatesHLL;
    final CuboidScheduler cuboidScheduler;

    boolean isUseNewEstimateAlgorithm = false;

    private long factTableRowCount;


    public CubeStatsReader(CubeSegment cubeSegment, KylinConfig kylinConfig) throws IOException {
        ResourceStore store = ResourceStore.getStore(kylinConfig);
        cuboidScheduler = new CuboidScheduler(cubeSegment.getCubeDesc());
        String statsKey = cubeSegment.getStatisticsResourcePath();
        File tmpSeqFile = writeTmpSeqFile(store.getResource(statsKey).inputStream);
        Reader reader = null;

        try {
            Configuration hadoopConf = HadoopUtil.getCurrentConfiguration();

            Path path = new Path(HadoopUtil.fixWindowsPath("file://" + tmpSeqFile.getAbsolutePath()));
            Option seqInput = SequenceFile.Reader.file(path);
            reader = new SequenceFile.Reader(hadoopConf, seqInput);

            int percentage = 100;
            int mapperNumber = 0;
            double mapperOverlapRatio = 0;
            Map<Long, HLLCounter> counterMap = Maps.newHashMap();

            LongWritable key = (LongWritable) ReflectionUtils.newInstance(reader.getKeyClass(), hadoopConf);
            BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), hadoopConf);
            while (reader.next(key, value)) {
                if (key.get() == 0L) {
                    percentage = Bytes.toInt(value.getBytes());
                } else if (key.get() == -1) {
                    mapperOverlapRatio = Bytes.toDouble(value.getBytes());
                } else if (key.get() == -2) {
                    mapperNumber = Bytes.toInt(value.getBytes());
                } else if (key.get() == -3) {
                    this.isUseNewEstimateAlgorithm = true;
                    factTableRowCount = Bytes.toLong(value.getBytes());
                } else if (key.get() > 0) {
                    HLLCounter hll = new HLLCounter(kylinConfig.getCubeStatsHLLPrecision());
                    ByteArray byteArray = new ByteArray(value.getBytes());
                    hll.readRegisters(byteArray.asBuffer());
                    counterMap.put(key.get(), hll);
                }
            }

            this.seg = cubeSegment;
            this.samplingPercentage = percentage;
            this.mapperNumberOfFirstBuild = mapperNumber;
            this.mapperOverlapRatioOfFirstBuild = mapperOverlapRatio;
            this.cuboidRowEstimatesHLL = counterMap;

        } finally {
            IOUtils.closeStream(reader);
            tmpSeqFile.delete();
        }
    }

    private File writeTmpSeqFile(InputStream inputStream) throws IOException {
        File tempFile = File.createTempFile("kylin_stats_tmp", ".seq");
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(tempFile);
            org.apache.commons.io.IOUtils.copy(inputStream, out);
        } finally {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(out);
        }
        return tempFile;
    }

    public Map<Long, Long> getCuboidRowEstimatesHLL() {
        return getCuboidRowCountMapFromSampling(cuboidRowEstimatesHLL, samplingPercentage, isUseNewEstimateAlgorithm, factTableRowCount);
    }

    // return map of Cuboid ID => MB
    public Map<Long, Double> getCuboidSizeMap() {
        return getCuboidSizeMapFromRowCount(seg, getCuboidRowEstimatesHLL());
    }

    public double estimateCubeSize() {
        return SumHelper.sumDouble(getCuboidSizeMap().values());
    }

    public int getMapperNumberOfFirstBuild() {
        return mapperNumberOfFirstBuild;
    }

    public double getMapperOverlapRatioOfFirstBuild() {
        return mapperOverlapRatioOfFirstBuild;
    }

    public static Map<Long, Long> getCuboidRowCountMapFromSampling(Map<Long, HLLCounter> hllcMap, int samplingPercentage) {
        return getCuboidRowCountMapFromSampling(hllcMap, samplingPercentage, false, 0);
    }

    public static Map<Long, Long> getCuboidRowCountMapFromSampling(Map<Long, HLLCounter> hllcMap, int samplingPercentage, boolean isUseNewEstimateAlgorithm, long factTableRowCount) {
        Map<Long, Long> cuboidRowCountMap = Maps.newHashMap();
        if (isUseNewEstimateAlgorithm) {
            for (Map.Entry<Long, HLLCounter> entry : hllcMap.entrySet()) {
                long cuboid = entry.getKey();
                long countEstimate = entry.getValue().getCountEstimate();
                double percentage = samplingPercentage * 1.0 / 100;
                double a = factTableRowCount * percentage * percentage;
                long countEstimate2 = 0;
                if (a != 0) {
                    countEstimate2 = (long) Math.floor(countEstimate * countEstimate / a);
                }
                cuboidRowCountMap.put(cuboid, Math.max(countEstimate, countEstimate2));
            }
        } else {
            for (Map.Entry<Long, HLLCounter> entry : hllcMap.entrySet()) {
                // No need to adjust according sampling percentage. Assumption is that data set is far
                // more than cardinality. Even a percentage of the data should already see all cardinalities.
                cuboidRowCountMap.put(entry.getKey(), entry.getValue().getCountEstimate());
            }
        }
        return cuboidRowCountMap;
    }

    public static Map<Long, Double> getCuboidSizeMapFromRowCount(CubeSegment cubeSegment, Map<Long, Long> rowCountMap) {
        final CubeDesc cubeDesc = cubeSegment.getCubeDesc();
        final List<Integer> rowkeyColumnSize = Lists.newArrayList();
        final long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        final Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        final List<TblColRef> columnList = baseCuboid.getColumns();
        final CubeDimEncMap dimEncMap = cubeSegment.getDimensionEncodingMap();

        for (int i = 0; i < columnList.size(); i++) {
            rowkeyColumnSize.add(dimEncMap.get(columnList.get(i)).getLengthOfEncoding());
        }

        Map<Long, Double> sizeMap = Maps.newHashMap();
        for (Map.Entry<Long, Long> entry : rowCountMap.entrySet()) {
            sizeMap.put(entry.getKey(), estimateCuboidStorageSize(cubeSegment, entry.getKey(), entry.getValue(), baseCuboidId, rowkeyColumnSize));
        }
        return sizeMap;
    }

    /**
     * Estimate the cuboid's size
     *
     * @return the cuboid size in M bytes
     */
    private static double estimateCuboidStorageSize(CubeSegment cubeSegment, long cuboidId, long rowCount, long baseCuboidId, List<Integer> rowKeyColumnLength) {

        int rowkeyLength = cubeSegment.getRowKeyPreambleSize();
        KylinConfig kylinConf = cubeSegment.getConfig();

        long mask = Long.highestOneBit(baseCuboidId);
        long parentCuboidIdActualLength = (long) Long.SIZE - Long.numberOfLeadingZeros(baseCuboidId);
        for (int i = 0; i < parentCuboidIdActualLength; i++) {
            if ((mask & cuboidId) > 0) {
                rowkeyLength += rowKeyColumnLength.get(i); //colIO.getColumnLength(columnList.get(i));
            }
            mask = mask >> 1;
        }

        // add the measure length
        int normalSpace = rowkeyLength;
        int countDistinctSpace = 0;
        for (MeasureDesc measureDesc : cubeSegment.getCubeDesc().getMeasures()) {
            DataType returnType = measureDesc.getFunction().getReturnDataType();
            if (measureDesc.getFunction().getExpression().equals(FunctionDesc.FUNC_COUNT_DISTINCT)) {
                countDistinctSpace += returnType.getStorageBytesEstimate();
            } else {
                normalSpace += returnType.getStorageBytesEstimate();
            }
        }

        double cuboidSizeRatio = kylinConf.getJobCuboidSizeRatio();
        double cuboidSizeMemHungryRatio = kylinConf.getJobCuboidSizeCountDistinctRatio();
        double ret = (1.0 * normalSpace * rowCount * cuboidSizeRatio + 1.0 * countDistinctSpace * rowCount * cuboidSizeMemHungryRatio) / (1024L * 1024L);
        logger.debug("Cuboid " + cuboidId + " has " + rowCount + " rows, each row size is " + (normalSpace + countDistinctSpace) + " bytes." + " Total size is " + ret + "M.");
        return ret;
    }

    private void print(PrintWriter out) {
        Map<Long, Long> cuboidRows = getCuboidRowEstimatesHLL();
        Map<Long, Double> cuboidSizes = getCuboidSizeMap();
        List<Long> cuboids = new ArrayList<Long>(cuboidRows.keySet());
        Collections.sort(cuboids);

        out.println("============================================================================");
        out.println("Statistics of " + seg);
        out.println();
        out.println("Cube statistics hll precision: " + cuboidRowEstimatesHLL.values().iterator().next().getPrecision());
        out.println("Total cuboids: " + cuboidRows.size());
        out.println("Total estimated rows: " + SumHelper.sumLong(cuboidRows.values()));
        out.println("Total estimated size(MB): " + SumHelper.sumDouble(cuboidSizes.values()));
        out.println("Sampling percentage:  " + samplingPercentage);
        out.println("Mapper overlap ratio: " + mapperOverlapRatioOfFirstBuild);
        out.println("Mapper number: " + mapperNumberOfFirstBuild);
        printKVInfo(out);
        printCuboidInfoTreeEntry(cuboidRows, cuboidSizes, out);
        out.println("----------------------------------------------------------------------------");
    }

    //return MB
    public double estimateLayerSize(int level) {
        List<List<Long>> layeredCuboids = cuboidScheduler.getCuboidsByLayer();
        Map<Long, Double> cuboidSizeMap = getCuboidSizeMap();
        double ret = 0;
        for (Long cuboidId : layeredCuboids.get(level)) {
            ret += cuboidSizeMap.get(cuboidId);
        }

        logger.info("Estimating size for layer {}, all cuboids are {}, total size is {}", level, StringUtils.join(layeredCuboids.get(level), ","), ret);
        return ret;
    }

    public List<Long> getCuboidsByLayer(int level) {
        List<List<Long>> layeredCuboids = cuboidScheduler.getCuboidsByLayer();
        return layeredCuboids.get(level);
    }

    private void printCuboidInfoTreeEntry(Map<Long, Long> cuboidRows, Map<Long, Double> cuboidSizes, PrintWriter out) {
        long baseCuboid = Cuboid.getBaseCuboidId(seg.getCubeDesc());
        int dimensionCount = Long.bitCount(baseCuboid);
        printCuboidInfoTree(-1L, baseCuboid, cuboidScheduler, cuboidRows, cuboidSizes, dimensionCount, 0, out);
    }

    private void printKVInfo(PrintWriter writer) {
        Cuboid cuboid = Cuboid.getBaseCuboid(seg.getCubeDesc());
        RowKeyEncoder encoder = new RowKeyEncoder(seg, cuboid);
        for (TblColRef col : cuboid.getColumns()) {
            writer.println("Length of dimension " + col + " is " + encoder.getColumnLength(col));
        }
    }

    private static void printCuboidInfoTree(long parent, long cuboidID, final CuboidScheduler scheduler, Map<Long, Long> cuboidRows, Map<Long, Double> cuboidSizes, int dimensionCount, int depth, PrintWriter out) {
        printOneCuboidInfo(parent, cuboidID, cuboidRows, cuboidSizes, dimensionCount, depth, out);

        List<Long> children = scheduler.getSpanningCuboid(cuboidID);
        Collections.sort(children);

        for (Long child : children) {
            printCuboidInfoTree(cuboidID, child, scheduler, cuboidRows, cuboidSizes, dimensionCount, depth + 1, out);
        }
    }

    private static void printOneCuboidInfo(long parent, long cuboidID, Map<Long, Long> cuboidRows, Map<Long, Double> cuboidSizes, int dimensionCount, int depth, PrintWriter out) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < depth; i++) {
            sb.append("    ");
        }
        String cuboidName = Cuboid.getDisplayName(cuboidID, dimensionCount);
        sb.append("|---- Cuboid ").append(cuboidName);

        long rowCount = cuboidRows.get(cuboidID);
        double size = cuboidSizes.get(cuboidID);
        sb.append(", est row: ").append(rowCount).append(", est MB: ").append(formatDouble(size));

        if (parent != -1) {
            sb.append(", shrink: ").append(formatDouble(100.0 * cuboidRows.get(cuboidID) / cuboidRows.get(parent))).append("%");
        }

        out.println(sb.toString());
    }

    private static String formatDouble(double input) {
        return new DecimalFormat("#.##").format(input);
    }

    public static void main(String[] args) throws IOException {
        System.out.println("CubeStatsReader is used to read cube statistic saved in metadata store");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        CubeInstance cube = CubeManager.getInstance(config).getCube(args[0]);
        List<CubeSegment> segments = cube.getSegments();

        PrintWriter out = new PrintWriter(System.out);
        for (CubeSegment seg : segments) {
            try {
                new CubeStatsReader(seg, config).print(out);
            } catch (Exception e) {
                logger.info("CubeStatsReader for Segment {} failed, skip it.", seg.getName());
            }
        }
        out.flush();
    }

}
