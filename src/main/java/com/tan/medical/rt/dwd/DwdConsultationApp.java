package com.tan.medical.rt.dwd;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class DwdConsultationApp {

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

        tableEnv.executeSql("drop table dwd_consultation");
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS dwd_consultation \n" +
                "(\n" +
                "  `id`                           STRING NOT NULL,\n" +
                "  `create_time`                  TIMESTAMP(6)  COMMENT '创建时间',\n" +
                "  `update_time`                  TIMESTAMP(6)  COMMENT '修改时间',\n" +
                "  `consultation_fee`             DECIMAL(19,2) COMMENT '咨询费用',\n" +
                "  `description`                  STRING COMMENT '病情描述',\n" +
                "  `diagnosis`                    STRING COMMENT '诊断',\n" +
                "  `rating`                       TINYINT COMMENT '评分 1-5分',\n" +
                "  `review`                       STRING COMMENT '评价',\n" +
                "  `status`                       SMALLINT COMMENT '状态 201.未填写病情描述 202.未支付 203.已支付 204.正在诊断 205.诊断结束 206.已出处方 207.已评价',\n" +
                "  `doctor_id`                    STRING COMMENT '接诊医生编码',\n" +
                "  `doctor_name`                  STRING COMMENT '接诊医生名称',\n" +
                "  `doctor_birthday`              STRING COMMENT '接诊医生出生日期',\n" +
                "  `doctor_gender_code`           STRING COMMENT '接诊医生性别编码',\n" +
                "  `doctor_gender_name`           STRING COMMENT '接诊医生性别名称',\n" +
                "  `doctor_specialty_code`        STRING COMMENT '接诊医生专业编码',\n" +
                "  `doctor_specialty_name`        STRING COMMENT '接诊医生专业名称',\n" +
                "  `doctor_title_code`            STRING COMMENT '接诊医生职称编码',\n" +
                "  `doctor_title_name`            STRING COMMENT '接诊医生职称名称',\n" +
                "  `hospital_id`                  STRING COMMENT '接诊医院编码',\n" +
                "  `hospital_name`                STRING COMMENT '接诊医院名称',\n" +
                "  `hospital_establish_time`      STRING COMMENT '接诊医院建立日期',\n" +
                "  `hospital_level`               STRING COMMENT '接诊医院等级',\n" +
                "  `hospital_province`            STRING COMMENT '接诊医院所处市级',\n" +
                "  `hospital_city`                STRING COMMENT '接诊医院所处城市',\n" +
                "  `hospital_district`            STRING COMMENT '接诊医院所处县级',\n" +
                "  `patient_id`                   STRING COMMENT '就诊人编码',\n" +
                "  `patient_name`                 STRING COMMENT '就诊人名称',\n" +
                "  `patient_birthday`             STRING COMMENT '就诊人出生日期',\n" +
                "  `patient_gender_code`          STRING COMMENT '就诊人性别编码',\n" +
                "  `patient_gender_name`          STRING COMMENT '就诊人性别名称',\n" +
                "  `user_id`                      STRING COMMENT '用户',\n" +
                "  `dt`                           STRING COMMENT '分区',\n" +
                "  `hour`                         BIGINT COMMENT '小时',\n" +
                "  `minute`                       BIGINT COMMENT '分钟',\n" +
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

        tableEnv.executeSql("INSERT INTO dwd_consultation\n" +
                "SELECT\n" +
                "   /*+ LOOKUP('table'='dim_hospital', 'async'='true', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s','max-attempts'='30'),\n" +
                "       LOOKUP('table'='dim_patient', 'async'='true', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s','max-attempts'='30') */\n" +
                "    t_join_doctor.id                       AS id,\n" +
                "    t_join_doctor.create_time              AS create_time,\n" +
                "    t_join_doctor.update_time              AS update_time,\n" +
                "    t_join_doctor.consultation_fee         AS consultation_fee,\n" +
                "    t_join_doctor.`description`            AS `description`,\n" +
                "    t_join_doctor.diagnosis                AS diagnosis,\n" +
                "    t_join_doctor.rating                   AS rating,\n" +
                "    t_join_doctor.review                   AS review,\n" +
                "    t_join_doctor.status                   AS status,\n" +
                "    t_join_doctor.doctor_id                AS doctor_id,\n" +
                "    t_join_doctor.doctor_name              AS doctor_name,\n" +
                "    t_join_doctor.doctor_birthday          AS doctor_birthday,\n" +
                "    t_join_doctor.doctor_gender_code       AS doctor_gender_code,\n" +
                "    t_join_doctor.doctor_gender_name       AS doctor_gender_name,   \n" +
                "    t_join_doctor.doctor_specialty_code    AS doctor_specialty_code,\n" +
                "    t_join_doctor.doctor_specialty_name    AS doctor_specialty_name,\n" +
                "    t_join_doctor.doctor_title_code        AS doctor_title_code,\n" +
                "    t_join_doctor.doctor_title_name        AS doctor_title_name, \n" +
                "    t_join_doctor.hospital_id              AS hospital_id,\n" +
                "    t_hospital.name                        AS hospital_name,\n" +
                "    t_hospital.establish_time              AS hospital_establish_time,\n" +
                "    t_hospital.level                       AS hospital_level,\n" +
                "    t_hospital.province                    AS hospital_province,\n" +
                "    t_hospital.city                        AS hospital_city,\n" +
                "    t_hospital.district                    AS hospital_district,\n" +
                "    t_join_doctor.patient_id               AS patient_id,\n" +
                "    t_patient.name                         AS patient_name,\n" +
                "    t_patient.birthday                     AS patient_birthday,\n" +
                "    t_patient.gender_code                  AS patient_gender_code,\n" +
                "    t_patient.gender_name                  AS patient_gender_name,\n" +
                "    t_join_doctor.user_id                  AS user_id,\n" +
                "    DATE_FORMAT(t_join_doctor.create_time, 'yyyy-MM-dd') AS dt,\n" +
                "    HOUR(t_join_doctor.create_time)                      AS `hour`,\n" +
                "    MINUTE(t_join_doctor.create_time)                    AS `minute`          \n" +
                "FROM\n" +
                "( SELECT\n" +
                "    /*+ LOOKUP('table'='dim_doctor', 'async'='true', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s','max-attempts'='30') */\n" +
                "    t_consultation.id                  AS id,\n" +
                "    t_consultation.create_time         AS create_time,\n" +
                "    t_consultation.update_time         AS update_time,\n" +
                "    t_consultation.consultation_fee    AS consultation_fee,\n" +
                "    t_consultation.`description`       AS `description`,\n" +
                "    t_consultation.diagnosis           AS diagnosis,\n" +
                "    t_consultation.rating              AS rating,\n" +
                "    t_consultation.review              AS review,\n" +
                "    t_consultation.status              AS status,\n" +
                "    t_consultation.doctor_id           AS doctor_id,\n" +
                "    t_doctor.name                      AS doctor_name,\n" +
                "    t_doctor.birthday                  AS doctor_birthday,\n" +
                "    t_doctor.gender_code               AS doctor_gender_code,\n" +
                "    t_doctor.gender                    AS doctor_gender_name,   \n" +
                "    t_doctor.specialty_code            AS doctor_specialty_code,\n" +
                "    t_doctor.specialty_name            AS doctor_specialty_name,\n" +
                "    t_doctor.title_code                AS doctor_title_code,\n" +
                "    t_doctor.title_name                AS doctor_title_name,                     \n" +
                "    t_doctor.hospital_id               AS hospital_id,\n" +
                "    t_consultation.patient_id          AS patient_id,\n" +
                "    t_consultation.user_id             AS user_id,\n" +
                "    t_consultation.proc_time           AS proc_time\n" +
                "FROM\n" +
                "( SELECT\n" +
                "    CAST(id AS STRING) AS id,\n" +
                "    create_time,\n" +
                "    update_time,\n" +
                "    consultation_fee,\n" +
                "    CAST(`description` AS STRING)      AS `description`,\n" +
                "    CAST(diagnosis AS STRING)          AS diagnosis,\n" +
                "    CAST(rating AS TINYINT)            AS rating,\n" +
                "    CAST(review AS STRING)             AS review,\n" +
                "    CAST(status AS SMALLINT)           AS status,\n" +
                "    CAST(doctor_id AS STRING)          AS doctor_id,\n" +
                "    CAST(patient_id AS STRING)         AS patient_id,\n" +
                "    CAST(user_id AS STRING)            AS user_id,\n" +
                "    PROCTIME() AS proc_time  \n" +
                "FROM ods_consultation ) t_consultation\n" +
                "LEFT JOIN dim_doctor /*+ OPTIONS('rocksdb.compression.type'='NO_COMPRESSION') */\n" +
                "FOR SYSTEM_TIME AS OF t_consultation.proc_time AS t_doctor\n" +
                "ON t_consultation.doctor_id = t_doctor.id ) t_join_doctor\n" +
                "LEFT JOIN dim_hospital /*+ OPTIONS('rocksdb.compression.type'='NO_COMPRESSION') */\n" +
                "FOR SYSTEM_TIME AS OF t_join_doctor.proc_time AS t_hospital\n" +
                "ON t_join_doctor.hospital_id = t_hospital.id\n" +
                "LEFT JOIN dim_patient /*+ OPTIONS('rocksdb.compression.type'='NO_COMPRESSION') */\n" +
                "FOR SYSTEM_TIME AS OF t_join_doctor.proc_time AS t_patient\n" +
                "ON t_join_doctor.patient_id = t_patient.id;");

    }

}
