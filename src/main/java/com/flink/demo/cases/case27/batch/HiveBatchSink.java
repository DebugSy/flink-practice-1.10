package com.flink.demo.cases.case27.batch;

import com.flink.demo.cases.case27.datastream.metastore.HiveTableMetaStoreFactory;
import com.flink.demo.cases.common.datasource.OutOfOrderRowDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.mapred.JobConf;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.UUID;

@Slf4j
public class HiveBatchSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());

        RowTypeInfo rowTypeInfo = (RowTypeInfo) UrlClickRowDataSource.USER_CLICK_TYPEINFO;

        SingleOutputStreamOperator source = env.addSource(new OutOfOrderRowDataSource())
                .returns(rowTypeInfo)
                .name("url click source");

        String[] fieldNames = rowTypeInfo.getFieldNames();
        String[] fieldTypes = new String[]{"int", "string", "string", "timestamp", "int", "string", "string", "string"};
        String[] partitions = {};

        HiveConf hiveConf = createHiveConf("src/main/resources");
        hiveConf.set(HiveCatalogValidator.CATALOG_HIVE_VERSION, HiveShimLoader.getHiveVersion());
        JobConf jobConf = new JobConf(hiveConf);
        String hiveVersion = jobConf.get(HiveCatalogValidator.CATALOG_HIVE_VERSION);
        HiveShim hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);

        String database = "shiy";
        String tableName = "url_click_sink_v2";
        HiveMetastoreClientWrapper client = HiveMetastoreClientFactory.create(
                new HiveConf(jobConf, HiveConf.class), hiveVersion);
        Table table = client.getTable(database, tableName);
        StorageDescriptor sd = table.getSd();
        HiveTableMetaStoreFactory msFactory = new HiveTableMetaStoreFactory(
                jobConf, hiveVersion, database, tableName);

        Class hiveOutputFormatClz = hiveShim.getHiveOutputFormatClass(
                Class.forName(sd.getOutputFormat()));
        HiveWriterFactory recordWriterFactory = new HiveWriterFactory(
                jobConf,
                hiveOutputFormatClz,
                sd.getSerdeInfo(),
                fieldNames,
                fieldTypes,
                partitions,
                HiveReflectionUtils.getTableMetadata(hiveShim, table),
                hiveShim,
                false);

        String extension = Utilities.getFileExtension(jobConf, false,
                (HiveOutputFormat<?, ?>) hiveOutputFormatClz.newInstance());
        OutputFileConfig outputFileConfig = OutputFileConfig.builder()
                .withPartPrefix("part-" + UUID.randomUUID().toString())
                .withPartSuffix(extension == null ? "" : extension)
                .build();

        FileSystemOutputFormat.Builder builder = new FileSystemOutputFormat.Builder();
        builder.setDynamicGrouped(false);
        builder.setAllColumns(fieldNames);
        builder.setPartitionColumns(partitions);
        builder.setFormatFactory(new HiveOutputFormatFactory(recordWriterFactory));
        builder.setMetaStoreFactory(msFactory);
        builder.setOverwrite(false);
//        builder.setStaticPartitions(new LinkedHashMap<>());
        builder.setTempPath(new org.apache.flink.core.fs.Path(toStagingDir(sd.getLocation(), jobConf)));
        builder.setOutputFileConfig(outputFileConfig);

        source.writeUsingOutputFormat(builder.build())
                .setParallelism(1);

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

    // get a staging dir associated with a final dir
    private static String toStagingDir(String finalDir, Configuration conf) throws IOException {
        String res = finalDir;
        if (!finalDir.endsWith(Path.SEPARATOR)) {
            res += Path.SEPARATOR;
        }
        // TODO: may append something more meaningful than a timestamp, like query ID
        res += ".staging_" + System.currentTimeMillis();
        Path path = new Path(res);
        FileSystem fs = path.getFileSystem(conf);
        Preconditions.checkState(fs.exists(path) || fs.mkdirs(path), "Failed to create staging dir " + path);
        fs.deleteOnExit(path);
        return res;
    }

}
