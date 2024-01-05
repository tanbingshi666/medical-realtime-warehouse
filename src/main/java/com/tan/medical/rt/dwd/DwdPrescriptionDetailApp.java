package com.tan.medical.rt.dwd;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class DwdPrescriptionDetailApp {

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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS dwd_prescription_detail (\n" +
                "  `id` STRING NOT NULL,\n" +
                "  `create_time` TIMESTAMP(6) COMMENT '创建时间',\n" +
                "  `update_time` TIMESTAMP(6) COMMENT '修改时间',\n" +
                "  `count`     STRING COMMENT '剂量',\n" +
                "  `instruction` STRING   COMMENT '服用说明',\n" +
                "  `medicine_id` STRING   COMMENT '药品',\n" +
                "  `approval_code` STRING COMMENT '药物批号',\n" +
                "  `name`          STRING COMMENT '药品名称',\n" +
                "  `name_en`       STRING COMMENT '英文名称',\n" +
                "  `price`         DECIMAL(19, 2) COMMENT '药品价格',\n" +
                "  `specs`         STRING COMMENT '规格',\n" +
                "  `trade_name`    STRING COMMENT '商品名',\n" +
                "  `prescription_id` STRING COMMENT '所属处方',\n" +
                "  `dt` STRING COMMENT '分区',\n" +
                "  PRIMARY KEY (dt, id) NOT ENFORCED\n" +
                ") PARTITIONED BY (dt) WITH (\n" +
                "    -- 指定2个bucket\n" +
                "    'bucket' = '2',\n" +
                "    -- 分bucket字段\n" +
                "    'bucket-key' = 'id',\n" +
                "    -- 记录排序字段\n" +
                "    'sequence.field' = 'update_time',\n" +
                "    -- 选择 full-compaction ,在compaction后产生完整的changelog\n" +
                "    'changelog-producer' = 'full-compaction',\n" +
                "    -- compaction 间隔时间\n" +
                "    'changelog-producer.compaction-interval' = '1 min'\n" +
                ");");

        tableEnv.executeSql("INSERT INTO dwd_prescription_detail\n" +
                "SELECT\n" +
                "    prescription_detail.id AS id,\n" +
                "    prescription_detail.create_time AS create_time,\n" +
                "    prescription_detail.update_time AS update_time,\n" +
                "    prescription_detail.`count` AS `count`,\n" +
                "    prescription_detail.instruction AS instruction,\n" +
                "    prescription_detail.medicine_id AS medicine_id,\n" +
                "    t_medicine.approval_code AS approval_code,\n" +
                "    t_medicine.name AS name,\n" +
                "    t_medicine.name_en AS name_en,\n" +
                "    t_medicine.price AS price,\n" +
                "    t_medicine.specs AS specs,\n" +
                "    t_medicine.trade_name AS trade_name,\n" +
                "    prescription_detail.prescription_id AS prescription_id,\n" +
                "    prescription_detail.dt AS dt\n" +
                "FROM\n" +
                "( SELECT\n" +
                "    CAST(id AS STRING) AS id,\n" +
                "    create_time,\n" +
                "    update_time,\n" +
                "    CAST(`count` AS STRING) AS `count`,\n" +
                "    CAST(instruction AS STRING) AS instruction,\n" +
                "    CAST(medicine_id AS STRING) AS medicine_id,\n" +
                "    CAST(prescription_id AS STRING) AS prescription_id,\n" +
                "    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS dt,\n" +
                "    PROCTIME() AS proc_time\n" +
                "FROM ods_prescription_detail ) AS prescription_detail\n" +
                "LEFT JOIN dim_medicine /*+ OPTIONS('rocksdb.compression.type'='NO_COMPRESSION') */\n" +
                "FOR SYSTEM_TIME AS OF prescription_detail.proc_time AS t_medicine\n" +
                "ON prescription_detail.medicine_id = t_medicine.id;");

    }

}
