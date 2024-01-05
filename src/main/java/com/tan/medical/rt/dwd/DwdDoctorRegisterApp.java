package com.tan.medical.rt.dwd;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class DwdDoctorRegisterApp {

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

        tableEnv.executeSql("drop table dwd_doctor_register");
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS dwd_doctor_register\n" +
                "(\n" +
                "    `id`               STRING COMMENT '医生ID',\n" +
                "    `create_time`      TIMESTAMP(6) COMMENT '创建时间',\n" +
                "    `update_time`      TIMESTAMP(6) COMMENT '修改时间',\n" +
                "    `birthday`         STRING COMMENT '出生日期',\n" +
                "    `consultation_fee` DECIMAL(19, 2) COMMENT '就诊费用',\n" +
                "    `gender_code`      STRING COMMENT '性别编码：101.男 102.女',\n" +
                "    `gender_name`      STRING COMMENT '性别',\n" +
                "    `name`             STRING COMMENT '姓名',\n" +
                "    `specialty_code`   STRING COMMENT '专业编码：详情见字典表5xx条目',\n" +
                "    `specialty_name`   STRING COMMENT '专业名称',\n" +
                "    `title_code`       STRING COMMENT '职称编码：301. 医士 302. 医师 303. 主治医师 304. 副主任医师 305. 主任医师',\n" +
                "    `title_name`       STRING COMMENT '职称名称',\n" +
                "    `hospital_id`      STRING COMMENT '所属医院',\n" +
                "    `dt`               STRING COMMENT '分区',\n" +
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

        tableEnv.executeSql("INSERT INTO dwd_doctor_register\n" +
                "SELECT\n" +
                "    CAST(t_join_specialty.id AS STRING) AS id,\n" +
                "    t_join_specialty.create_time AS create_time,\n" +
                "    t_join_specialty.update_time AS update_time,\n" +
                "    CAST(t_join_specialty.birthday AS STRING) AS birthday,\n" +
                "    t_join_specialty.consultation_fee AS consultation_fee,\n" +
                "    CAST(t_join_specialty.gender_code AS STRING) AS gender_code,\n" +
                "    CAST(t_join_specialty.gender_name AS STRING) AS gender_name,\n" +
                "    CAST(t_join_specialty.name AS STRING) AS name,\n" +
                "    CAST(t_join_specialty.specialty_code AS STRING) AS specialty_code,\n" +
                "    CAST(t_join_specialty.specialty_name AS STRING) AS specialty_name,\n" +
                "    CAST(t_join_specialty.title AS STRING) AS title_code,\n" +
                "    CAST(t_dict3.`value` AS STRING) AS title_name,\n" +
                "    CAST(t_join_specialty.hospital_id AS STRING) AS hospital_id,\n" +
                "    DATE_FORMAT(t_join_specialty.create_time, 'yyyy-MM-dd') AS dt\n" +
                "FROM\n" +
                "( SELECT\n" +
                "    t_join_gender.id AS id,\n" +
                "    t_join_gender.create_time AS create_time,\n" +
                "    t_join_gender.update_time AS update_time,\n" +
                "    t_join_gender.birthday AS birthday,\n" +
                "    t_join_gender.consultation_fee AS consultation_fee,\n" +
                "    t_join_gender.gender_code AS gender_code,\n" +
                "    t_join_gender.gender_name AS gender_name,\n" +
                "    t_join_gender.name AS name,\n" +
                "    t_join_gender.specialty AS specialty_code,\n" +
                "    t_dict2.`value` AS specialty_name,\n" +
                "    t_join_gender.title AS title,\n" +
                "    t_join_gender.hospital_id AS hospital_id,\n" +
                "    t_join_gender.proc_time AS proc_time\n" +
                "FROM\n" +
                "( SELECT\n" +
                "    gender_doc.id AS id,\n" +
                "    gender_doc.create_time AS create_time,\n" +
                "    gender_doc.update_time AS update_time,\n" +
                "    gender_doc.birthday AS birthday,\n" +
                "    gender_doc.consultation_fee AS consultation_fee,\n" +
                "    gender_doc.gender AS gender_code,\n" +
                "    t_dict.`value` AS gender_name,\n" +
                "    gender_doc.name AS name,\n" +
                "    gender_doc.specialty AS specialty,\n" +
                "    gender_doc.title AS title,\n" +
                "    gender_doc.hospital_id AS hospital_id,\n" +
                "    gender_doc.proc_time AS proc_time\n" +
                "FROM\n" +
                "( SELECT\n" +
                "    id,\n" +
                "    create_time,\n" +
                "    update_time,\n" +
                "    birthday,\n" +
                "    consultation_fee,\n" +
                "    gender,\n" +
                "    `name`,\n" +
                "    specialty,\n" +
                "    title,\n" +
                "    hospital_id,\n" +
                "    PROCTIME() AS proc_time\n" +
                "FROM ods_doctor$audit_log where rowkind = '+I') AS gender_doc\n" +
                "LEFT JOIN ods_dict /*+ OPTIONS('rocksdb.compression.type'='NO_COMPRESSION') */\n" +
                "FOR SYSTEM_TIME AS OF gender_doc.proc_time AS t_dict\n" +
                "ON gender_doc.gender = t_dict.id ) AS t_join_gender\n" +
                "LEFT JOIN ods_dict /*+ OPTIONS('rocksdb.compression.type'='NO_COMPRESSION') */\n" +
                "FOR SYSTEM_TIME AS OF t_join_gender.proc_time AS t_dict2\n" +
                "ON t_join_gender.specialty = t_dict2.id ) AS t_join_specialty\n" +
                "LEFT JOIN ods_dict /*+ OPTIONS('rocksdb.compression.type'='NO_COMPRESSION') */\n" +
                "FOR SYSTEM_TIME AS OF t_join_specialty.proc_time AS t_dict3\n" +
                "ON t_join_specialty.title = t_dict3.id;");

    }

}
