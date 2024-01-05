package com.tan.medical.rt.dwd;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class DwdPatientRegisterApp {

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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS dwd_patient_register\n" +
                "(\n" +
                "    `id`          STRING COMMENT '患者ID',\n" +
                "    `create_time` TIMESTAMP(6) COMMENT '创建时间',\n" +
                "    `update_time` TIMESTAMP(6) COMMENT '修改时间',\n" +
                "    `birthday`    STRING COMMENT '出生日期',\n" +
                "    `gender_code` STRING COMMENT '性别编码',\n" +
                "    `gender_name` STRING COMMENT '性别',\n" +
                "    `name`        STRING COMMENT '姓名',\n" +
                "    `user_id`     STRING COMMENT '所属用户ID',\n" +
                "    `dt`          STRING COMMENT '分区',\n" +
                "    PRIMARY KEY (dt, id) NOT ENFORCED\n" +
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

        tableEnv.executeSql("INSERT INTO dwd_patient_register\n" +
                "SELECT\n" +
                "    CAST(t_patient.id AS STRING) AS id,\n" +
                "    t_patient.create_time AS create_time,\n" +
                "    t_patient.update_time AS update_time,\n" +
                "    CAST(t_patient.birthday AS STRING) AS birthday,\n" +
                "    CAST(t_patient.gender AS STRING) AS gender_code,\n" +
                "    CAST(t_dict.`value` AS STRING) AS gender_name,\n" +
                "    CAST(t_patient.name AS STRING) AS name,\n" +
                "    CAST(t_patient.user_id AS STRING) AS user_id,\n" +
                "    DATE_FORMAT(t_patient.create_time, 'yyyy-MM-dd') AS dt\n" +
                "FROM\n" +
                "( SELECT\n" +
                "    id,\n" +
                "    create_time,\n" +
                "    update_time,\n" +
                "    birthday,\n" +
                "    gender,\n" +
                "    name,\n" +
                "    user_id,\n" +
                "    PROCTIME() AS proc_time\n" +
                "FROM ods_patient$audit_log where rowkind = '+I' ) AS t_patient\n" +
                "LEFT JOIN ods_dict /*+ OPTIONS('rocksdb.compression.type'='NO_COMPRESSION') */\n" +
                "FOR SYSTEM_TIME AS OF t_patient.proc_time AS t_dict\n" +
                "ON t_patient.gender = t_dict.id;");

    }

}
