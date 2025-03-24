create database if not exists gmall;
use gmall;

drop table if exists ods_log;
CREATE EXTERNAL TABLE ods_log
(
    `line` string
)
    PARTITIONED BY (`dt` string) -- 按照时间创建分区
    STORED AS -- 指定存储方式，读数据采用 LzoTextInputFormat；
        INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION '/warehouse/gmall/ods/ods_log'; -- 指定数据在 hdfs 上的存储位置

load data inpath '/flume/events/2025-03-20' into table ods_log partition (dt = '2025-03-20');
load data inpath '/flume/events/2025-03-21' into table ods_log partition (dt = '2025-03-21');
load data inpath '/flume/events/2025-03-22' into table ods_log partition (dt = '2025-03-22');
select * from ods_log;


DROP TABLE IF EXISTS dwd_page_log;
CREATE EXTERNAL TABLE dwd_page_log
(
    `area_code`      STRING COMMENT '地区编码',
    `brand`          STRING COMMENT '手机品牌',
    `channel`        STRING COMMENT '渠道',
    `is_new`         STRING COMMENT '是否首次启动',
    `model`          STRING COMMENT '手机型号',
    `mid_id`         STRING COMMENT '设备 id',
    `os`             STRING COMMENT '操作系统',
    `user_id`        STRING COMMENT '会员 id',
    `version_code`   STRING COMMENT 'app 版本号',
    `during_time`    BIGINT COMMENT '持续时间毫秒',
    `page_item`      STRING COMMENT '目标 id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`   STRING COMMENT '上页类型',
    `page_id`        STRING COMMENT '页面 ID ',
    `source_type`    STRING COMMENT '来源类型',
    `ts`             bigint
) COMMENT '页面日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_page_log'
    TBLPROPERTIES ('parquet.compression' = 'lzo');

insert overwrite table dwd_page_log partition (dt = '2025-03-20')
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.page.during_time'),
       get_json_object(line, '$.page.item'),
       get_json_object(line, '$.page.item_type'),
       get_json_object(line, '$.page.last_page_id'),
       get_json_object(line, '$.page.page_id'),
       get_json_object(line, '$.page.source_type'),
       get_json_object(line, '$.ts')
from ods_log
where dt = '2025-03-20'
  and get_json_object(line, '$.page') is not null;

select * from dwd_page_log;


DROP TABLE IF EXISTS dwd_display_log;
CREATE EXTERNAL TABLE dwd_display_log
(
    `area_code`      STRING COMMENT '地区编码',
    `brand`          STRING COMMENT '手机品牌',
    `channel`        STRING COMMENT '渠道',
    `is_new`         STRING COMMENT '是否首次启动',
    `model`          STRING COMMENT '手机型号',
    `mid_id`         STRING COMMENT '设备 id',
    `os`             STRING COMMENT '操作系统',
    `user_id`        STRING COMMENT '会员 id',
    `version_code`   STRING COMMENT 'app 版本号',
    `during_time`    BIGINT COMMENT 'app 版本号',
    `page_item`      STRING COMMENT '目标 id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`   STRING COMMENT '上页类型',
    `page_id`        STRING COMMENT '页面 ID ',
    `source_type`    STRING COMMENT '来源类型',
    `ts`             BIGINT COMMENT 'app 版本号',
    `display_type`   STRING COMMENT '曝光类型',
    `item`           STRING COMMENT '曝光对象 id ',
    `item_type`      STRING COMMENT 'app 版本号',
    `order`          BIGINT COMMENT '曝光顺序',
    `pos_id`         BIGINT COMMENT '曝光位置'
) COMMENT '曝光日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_display_log'
    TBLPROPERTIES ('parquet.compression' = 'lzo');

DROP TABLE IF EXISTS dws_visitor_action_daycount;
CREATE EXTERNAL TABLE dws_visitor_action_daycount
(
    `mid_id`       STRING COMMENT '设备 id',
    `brand`        STRING COMMENT '设备品牌',
    `model`        STRING COMMENT '设备型号',
    `is_new`       STRING COMMENT '是否首次访问',
    `channel`      ARRAY<STRING> COMMENT '渠道',
    `os`           ARRAY<STRING> COMMENT '操作系统',
    `area_code`    ARRAY<STRING> COMMENT '地区 ID',
    `version_code` ARRAY<STRING> COMMENT '应用版本',
    `visit_count`  BIGINT COMMENT '访问次数',
    `page_stats`
                   ARRAY<STRUCT<page_id:STRING,page_count:BIGINT,during_time:BIGINT>> COMMENT '页面访问统计'
) COMMENT '每日设备行为表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dws/dws_visitor_actiion_daycount'
    TBLPROPERTIES ("parquet.compression" = "lzo");