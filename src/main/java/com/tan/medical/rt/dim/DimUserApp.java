package com.tan.medical.rt.dim;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class DimUserApp {

    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "tanbs");

        Configuration conf = new Configuration();
        // basic setting from https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/config/#basic-setup
        conf.setInteger("parallelism.default", 4);
        // checkpoint setting from https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/config/#checkpointing
        conf.setLong("execution.checkpointing.interval", 20 * 1000L);
        conf.setString("execution.checkpointing.mode", "EXACTLY_ONCE");
        conf.setLong("execution.checkpointing.timeout", 20 * 1000L);
        conf.setLong("execution.checkpointing.min-pause", 20 * 1000L);
        conf.setInteger("execution.checkpointing.max-concurrent-checkpoints", 1);
        conf.setInteger("execution.checkpointing.tolerable-failed-checkpoints", 3);
        conf.setString("execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION");
        // state backend setting from https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/config/#checkpoints-and-state-backends
        conf.setString("state.backend", "hashmap");
        conf.setString("state.checkpoint-storage", "filesystem");
        conf.setString("state.checkpoints.dir", "hdfs://hadoop102:8020/meiotds/chk/mkt/dwd");

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .withConfiguration(conf)
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql("CREATE CATALOG medical_hive WITH (\n" +
                "  'type' = 'paimon',\n" +
                "  'metastore' = 'hive',\n" +
                "  'uri' = 'thrift://hadoop103:9083',\n" +
                "  'warehouse' = 'hdfs://hadoop102:8020/warehouse/medical'\n" +
                ");");

        tableEnv.executeSql("USE CATALOG medical_hive;");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS dim_user\n" +
                "(\n" +
                "    `id`        STRING COMMENT '用户ID',\n" +
                "    `email`     STRING COMMENT '电邮',\n" +
                "    `telephone` STRING COMMENT '电话',\n" +
                "    `username`  STRING COMMENT '用户名',\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'bucket' = '4',\n" +
                "    'bucket-key' = 'id',\n" +
                "    'merge-engine' = 'deduplicate'\n" +
                ");");

        tableEnv.executeSql("INSERT INTO dim_user\n" +
                "SELECT\n" +
                "    CAST(id AS STRING) AS id,\n" +
                "    CAST(email AS STRING) AS email,\n" +
                "    CAST(telephone AS STRING) AS telephone,\n" +
                "    CAST(username AS STRING) AS username\n" +
                "FROM ods_user;");

    }

}
