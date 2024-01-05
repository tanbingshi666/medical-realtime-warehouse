package com.tan.medical.rt.dim;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class DimHospitalApp {

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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS dim_hospital\n" +
                "(\n" +
                "    `id`              STRING COMMENT '医院ID',\n" +
                "    `address`         STRING COMMENT '地址',\n" +
                "    `alias`           STRING COMMENT '医院别名',\n" +
                "    `bed_num`         BIGINT COMMENT '病床数量',\n" +
                "    `city`            STRING COMMENT '所在城市',\n" +
                "    `department_num`  BIGINT COMMENT '科室数量',\n" +
                "    `district`        STRING COMMENT '所属区县',\n" +
                "    `establish_time`  STRING COMMENT '建立时间',\n" +
                "    `health_care_num` BIGINT COMMENT '医护人数',\n" +
                "    `insurance`       STRING COMMENT '是否医保',\n" +
                "    `level`           STRING COMMENT '医院级别，一级甲等，二级甲等....',\n" +
                "    `name`            STRING COMMENT '医院名称',\n" +
                "    `province`        STRING COMMENT '所属省（直辖市）',\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'bucket' = '1',\n" +
                "    'bucket-key' = 'id',\n" +
                "    'merge-engine' = 'deduplicate'\n" +
                ");");

        tableEnv.executeSql("INSERT INTO dim_hospital\n" +
                "SELECT \n" +
                "       CAST(id AS STRING) AS id,\n" +
                "       CAST(address AS STRING) AS address,\n" +
                "       CAST(alias AS STRING) AS alias,\n" +
                "       CAST(bed_num AS BIGINT) AS bed_num,\n" +
                "       CAST(city AS STRING) AS city,\n" +
                "       CAST(department_num AS BIGINT) AS department_num,\n" +
                "       CAST(district AS STRING) AS district,\n" +
                "       CAST(establish_time AS STRING) AS establish_time,\n" +
                "       CAST(health_care_num AS BIGINT) AS health_care_num,\n" +
                "       CAST(insurance AS STRING) AS insurance,\n" +
                "       CAST(level AS STRING) AS level,\n" +
                "       CAST(name AS STRING) AS name,\n" +
                "       CAST(province AS STRING) AS province\n" +
                "FROM ods_hospital;");

    }

}
