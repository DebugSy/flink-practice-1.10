package com.flink.demo.cases.case27.table;

import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.hive.HiveTableFactory;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.factories.TableSinkFactoryContextImpl;
import org.apache.flink.table.sinks.TableSink;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_DELAY;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_POLICY_KIND;

public class HiveTableSinkTraining {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(5));
        env.setRestartStrategy(RestartStrategies.noRestart());

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator source = env.addSource(new UrlClickRowDataSource())
                .returns(UrlClickRowDataSource.USER_CLICK_TYPEINFO)
                .name("url click source");

        TableSchema schema = TableSchema.builder()
                .field("userId", DataTypes.INT())
                .field("username", DataTypes.STRING())
                .field("url", DataTypes.STRING())
                .field("clickTime", DataTypes.TIMESTAMP(9))
                .field("rank_num", DataTypes.INT())
                .field("uuid", DataTypes.STRING())
                .field("date_col", DataTypes.STRING())
                .field("time_col", DataTypes.STRING())
                .build();

        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogConfig.IS_GENERIC, String.valueOf(false));
        properties.put("connector", "COLLECTION");
        properties.put(SINK_PARTITION_COMMIT_POLICY_KIND.key(), "metastore,success-file");
        properties.put(SINK_PARTITION_COMMIT_DELAY.key(), "60000");

        HiveCatalog catalog = HiveTestUtils.createHiveCatalog();
        catalog.open();
        String database = "shiy";
        String table = "url_click_sink";
        ObjectPath objectPath = new ObjectPath(database, table);
        CatalogTable catalogTable = new CatalogTableImpl(schema, Arrays.asList("time_col"), properties, "csv table");
        catalog.createTable(objectPath, catalogTable, true);
        Optional<TableFactory> opt = catalog.getTableFactory();
        HiveTableFactory tableFactory = (HiveTableFactory) opt.get();
        TableSink tableSink = tableFactory.createTableSink(new TableSinkFactoryContextImpl(
                ObjectIdentifier.of(catalog.getName(), database, table),
                catalogTable,
                new Configuration(),
                false));
        tEnv.registerTableSink("sink", tableSink);

        tEnv.createTemporaryView("source", source);
        Table sourceTable = tEnv.from("source");
        sourceTable.executeInsert("sink");

        env.execute();
    }

}
