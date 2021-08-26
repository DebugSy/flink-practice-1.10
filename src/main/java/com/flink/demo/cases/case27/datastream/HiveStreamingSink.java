package com.flink.demo.cases.case27.datastream;

import com.flink.demo.cases.case27.datastream.bucket.HiveBucketAssigner;
import com.flink.demo.cases.case27.datastream.bucket.ProjectionBulkFactory;
import com.flink.demo.cases.case27.datastream.bucket.StreamingFileWriter;
import com.flink.demo.cases.case27.datastream.parquet.ParquetRowBuilder;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;

import javax.annotation.Nullable;
import java.io.File;
import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class HiveStreamingSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(5));
        env.setRestartStrategy(RestartStrategies.noRestart());

        RowTypeInfo rowTypeInfo = (RowTypeInfo) UrlClickRowDataSource.USER_CLICK_TYPEINFO;

        SingleOutputStreamOperator source = env.addSource(new UrlClickRowDataSource())
                .returns(rowTypeInfo)
                .name("url click source");

        HiveConf hiveConf = createHiveConf("src/main/resources");
        hiveConf.set(HiveCatalogValidator.CATALOG_HIVE_VERSION, HiveShimLoader.getHiveVersion());
        JobConf jobConf = new JobConf(hiveConf);
        String hiveVersion = jobConf.get(HiveCatalogValidator.CATALOG_HIVE_VERSION);
        HiveShim hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);

        String[] partitions = {"time_col"};
        String[] fieldNames = rowTypeInfo.getFieldNames();
        // 去除分区字段后的数据字段
        String[] parquetFieldNames = new String[]{"userId", "username", "url", "clickTime", "rank_num", "uuid", "date_col"};
        String[] parquetFieldTypes = new String[]{"int", "string", "string", "timestamp", "int", "string", "string"};
        List<String> columnList = Arrays.asList(fieldNames);
        int[] partitionIndexes = Arrays.stream(partitions)
                .mapToInt(columnList::indexOf)
                .toArray();
        List<Integer> partitionIndexList = Arrays.stream(partitionIndexes).boxed().collect(Collectors.toList());
        int[] nonPartitionIndexes = IntStream.range(0, fieldNames.length)
                .filter(c -> !partitionIndexList.contains(c))
                .toArray();

        //TODO 去除flink table依赖
        Optional<BulkWriter.Factory<Row>> bulkFactory =
                createBulkWriterFactory(parquetFieldNames, parquetFieldTypes, hiveVersion, jobConf, "parquet");
        HiveBucketAssigner bucketAssigner = new HiveBucketAssigner(rowTypeInfo, partitions);

        Path path = new Path("hdfs://mycluster/apps/hive/warehouse/shiy.db/url_click_sink_v2");
        StreamingFileSink.BucketsBuilder<Row, String, ? extends StreamingFileSink.BucketsBuilder<Row, ?, ?>> builder =
                StreamingFileSink.forBulkFormat(
                        path,
                        new ProjectionBulkFactory(bulkFactory.get(), nonPartitionIndexes))
                        .withBucketAssigner(bucketAssigner)
                        .withRollingPolicy(OnCheckpointRollingPolicy.build());
        log.info("Hive streaming sink: Use native parquet&orc writer.");

        StreamingFileWriter fileWriter = new StreamingFileWriter(
                TimeUnit.SECONDS.toMillis(5),
                builder);
        DataStream<StreamingFileCommitter.CommitMessage> writerStream = source.transform(
                StreamingFileWriter.class.getSimpleName(),
                TypeExtractor.createTypeInfo(StreamingFileCommitter.CommitMessage.class),
                fileWriter).setParallelism(source.getParallelism());

        String database = "shiy";
        String table = "url_click_sink_v2";

        StreamingFileCommitter committer = new StreamingFileCommitter(
                database,
                table,
                hiveVersion,
                path,
                Arrays.asList(partitions),
                new org.apache.flink.configuration.Configuration());
        writerStream
                .transform(StreamingFileCommitter.class.getSimpleName(), Types.VOID, committer)
                .setParallelism(1)
                .setMaxParallelism(1);

        env.execute();
    }

    private static HiveConf createHiveConf(@Nullable String hiveConfDir) {
        log.info("Setting hive conf dir as {}", hiveConfDir);

        try {
            HiveConf.setHiveSiteLocation(
                    hiveConfDir == null ?
                            null : Paths.get(hiveConfDir, "hive-site.xml").toUri().toURL());
        } catch (MalformedURLException e) {
            throw new CatalogException(
                    String.format("Failed to get hive-site.xml from %s", hiveConfDir), e);
        }

        // create HiveConf from hadoop configuration
        Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(new org.apache.flink.configuration.Configuration());

        // Add mapred-site.xml. We need to read configurations like compression codec.
        for (String possibleHadoopConfPath : HadoopUtils.possibleHadoopConfPaths(new org.apache.flink.configuration.Configuration())) {
            File mapredSite = new File(new File(possibleHadoopConfPath), "mapred-site.xml");
            if (mapredSite.exists()) {
                hadoopConf.addResource(new org.apache.hadoop.fs.Path(mapredSite.getAbsolutePath()));
                break;
            }
        }
        return new HiveConf(hadoopConf, HiveConf.class);
    }

    private static Optional<BulkWriter.Factory<Row>> createBulkWriterFactory(String[] fieldNames,
                                                                             String[] fieldTypes,
                                                                             String hiveVersion,
                                                                             JobConf jobConf,
                                                                             String format) {
        Configuration formatConf = new Configuration(jobConf);
        if (format.equals("parquet")) {
            return Optional.of(ParquetRowBuilder.createWriterFactory(
                    fieldNames, fieldTypes, formatConf, hiveVersion.startsWith("3.")));
        } else if (format.equals("orc")) {
//            TypeDescription typeDescription = OrcSplitReaderUtil.logicalTypeToOrcType(formatType);
//            return Optional.of(hiveShim.createOrcBulkWriterFactory(
//                    formatConf, typeDescription.toString(), formatTypes));
            return Optional.empty();
        } else {
            return Optional.empty();
        }
    }

}
