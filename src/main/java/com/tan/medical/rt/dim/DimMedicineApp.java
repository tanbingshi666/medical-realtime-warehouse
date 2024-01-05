package com.tan.medical.rt.dim;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class DimMedicineApp {

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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS dim_medicine\n" +
                "(\n" +
                "    `id`            STRING COMMENT '药品ID',\n" +
                "    `approval_code` STRING COMMENT '药物批号',\n" +
                "    `dose_type`     STRING COMMENT '剂量',\n" +
                "    `name`          STRING COMMENT '药品名称',\n" +
                "    `name_en`       STRING COMMENT '英文名称',\n" +
                "    `price`         DECIMAL(19, 2) COMMENT '药品价格',\n" +
                "    `specs`         STRING COMMENT '规格',\n" +
                "    `trade_name`    STRING COMMENT '商品名',\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'bucket' = '1',\n" +
                "    'bucket-key' = 'id',\n" +
                "    'merge-engine' = 'deduplicate'\n" +
                ");");

        tableEnv.executeSql("INSERT INTO dim_medicine\n" +
                "SELECT\n" +
                "    CAST(id AS STRING) AS id,\n" +
                "    CAST(approval_code AS STRING) AS approval_code,\n" +
                "    CAST(dose_type AS STRING) AS dose_type,\n" +
                "    CAST(name AS STRING) AS name,\n" +
                "    CAST(name_en AS STRING) AS name_en,\n" +
                "    price,\n" +
                "    CAST(specs AS STRING) AS specs,\n" +
                "    CAST(trade_name AS STRING) AS trade_name\n" +
                "FROM ods_medicine;");

    }

}
