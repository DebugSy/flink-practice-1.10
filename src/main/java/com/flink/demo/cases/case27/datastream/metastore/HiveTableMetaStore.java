package com.flink.demo.cases.case27.datastream.metastore;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.thrift.TException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Optional;

public class HiveTableMetaStore implements AutoCloseable, Serializable {

    private final String hiveVersion;
    private final String database;
    private final String tableName;

    private HiveMetastoreClientWrapper client;
    private StorageDescriptor sd;

    public HiveTableMetaStore(String database, String tableName, String hiveVersion) {
        this.hiveVersion = hiveVersion;
        this.database = database;
        this.tableName = tableName;
        client = HiveMetastoreClientFactory.create(
                new HiveConf(HiveConf.class), hiveVersion);
        try {
            sd = client.getTable(database, tableName).getSd();
        } catch (TException e) {
            throw new RuntimeException("Failed to query Hive metaStore", e);
        }
    }

    public Path getLocationPath() {
        return new Path(sd.getLocation());
    }

    public Optional<Path> getPartition(
            LinkedHashMap<String, String> partSpec) throws Exception {
        try {
            return Optional.of(new Path(client.getPartition(
                    database,
                    tableName,
                    new ArrayList<>(partSpec.values()))
                    .getSd().getLocation()));
        } catch (NoSuchObjectException ignore) {
            return Optional.empty();
        }
    }

    public void createOrAlterPartition(LinkedHashMap<String, String> partitionSpec, Path partitionPath) throws Exception {
        Partition partition;
        try {
            partition = client.getPartition(database, tableName, new ArrayList<>(partitionSpec.values()));
        } catch (NoSuchObjectException e) {
            createPartition(partitionSpec, partitionPath);
            return;
        }
        alterPartition(partitionSpec, partitionPath, partition);
    }

    private void createPartition(LinkedHashMap<String, String> partSpec, Path path) throws Exception {
        StorageDescriptor newSd = new StorageDescriptor(sd);
        newSd.setLocation(path.toString());
        Partition partition = HiveTableUtil.createHivePartition(database, tableName,
                new ArrayList<>(partSpec.values()), newSd, new HashMap<>());
        partition.setValues(new ArrayList<>(partSpec.values()));
        client.add_partition(partition);
    }

    private void alterPartition(LinkedHashMap<String, String> partitionSpec, Path partitionPath,
                                Partition currentPartition) throws Exception {
        StorageDescriptor partSD = currentPartition.getSd();
        // the following logic copied from Hive::alterPartitionSpecInMemory
        partSD.setOutputFormat(sd.getOutputFormat());
        partSD.setInputFormat(sd.getInputFormat());
        partSD.getSerdeInfo().setSerializationLib(sd.getSerdeInfo().getSerializationLib());
        partSD.getSerdeInfo().setParameters(sd.getSerdeInfo().getParameters());
        partSD.setBucketCols(sd.getBucketCols());
        partSD.setNumBuckets(sd.getNumBuckets());
        partSD.setSortCols(sd.getSortCols());
        partSD.setLocation(partitionPath.toString());
        client.alter_partition(database, tableName, currentPartition);
    }

    public void close() {
        client.close();
    }

}
