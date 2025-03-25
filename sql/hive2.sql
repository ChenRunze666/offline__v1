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

create function explode_json_array as
    'com.bw.gmall.ExplodeJSONArray' using jar
    'hdfs://cdh01/user/hive/jars/UDF1.0-SNAPSHOT.jar';


insert overwrite table dws_visitor_action_daycount partition (dt = '2025-03-22')
select t1.mid_id,
       t1.brand,
       t1.model,
       t1.is_new,
       t1.channel,
       t1.os,
       t1.area_code,
       t1.version_code,
       t1.visit_count,
       t3.page_stats
from (select mid_id,
             brand,
             model,
             `if`(array_contains(collect_set(is_new), '0'), '0', '1') is_new,
             collect_set(channel)                                     channel,
             collect_set(os)                                          os,
             collect_set(area_code)                                   area_code,
             collect_set(version_code)                                version_code,
             sum(if(last_page_id is null, 1, 0))                      visit_count
      from dwd_page_log
      where dt = '2025-03-22'
        and last_page_id is null
      group by mid_id, model, brand) t1
         join
     (select mid_id,
             brand,
             model,
             collect_set(named_struct('page_id', page_id, 'page_count', page_count, 'during_time',
                                      during_time)) page_stats
      from (select mid_id,
                   brand,
                   model,
                   page_id,
                   count(*)         page_count,
                   sum(during_time) during_time
            from dwd_page_log
            where dt = '2025-03-22'
            group by mid_id, model, brand, page_id) t2
      group by mid_id, brand, model) t3
     on t1.mid_id = t3.mid_id
         and t1.brand = t3.brand
         and t1.model = t3.model;

select * from dws_visitor_action_daycount;

--DWT
DROP TABLE IF EXISTS dwt_visitor_topic;
CREATE EXTERNAL TABLE dwt_visitor_topic
(
    `mid_id`                   STRING COMMENT '设备 id',
    `brand`                    STRING COMMENT '手机品牌',
    `model`                    STRING COMMENT '手机型号',
    `channel`                  ARRAY<STRING> COMMENT '渠道',
    `os`                       ARRAY<STRING> COMMENT '操作系统',
    `area_code`                ARRAY<STRING> COMMENT '地区 ID',
    `version_code`             ARRAY<STRING> COMMENT '应用版本',
    `visit_date_first`         STRING COMMENT '首次访问时间',
    `visit_date_last`          STRING COMMENT '末次访问时间',
    `visit_last_1d_count`      BIGINT COMMENT '最近 1 日访问次数',
    `visit_last_1d_day_count`  BIGINT COMMENT '最近 1 日访问天数',
    `visit_last_7d_count`      BIGINT COMMENT '最近 7 日访问次数',
    `visit_last_7d_day_count`  BIGINT COMMENT '最近 7 日访问天数',
    `visit_last_30d_count`     BIGINT COMMENT '最近 30 日访问次数',
    `visit_last_30d_day_count` BIGINT COMMENT '最近 30 日访问天数',
    `visit_count`              BIGINT COMMENT '累积访问次数',
    `visit_day_count`          BIGINT COMMENT '累积访问天数'
) COMMENT '设备主题宽表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwt/dwt_visitor_topic'
    TBLPROPERTIES ("parquet.compression" = "lzo");

insert overwrite table dwt_visitor_topic partition(dt='2025-03-22')
select nvl(1d_ago.mid_id, old.mid_id),
       nvl(1d_ago.brand, old.brand),
       nvl(1d_ago.model, old.model),
       nvl(1d_ago.channel, old.channel),
       nvl(1d_ago.os, old.os),
       nvl(1d_ago.area_code, old.area_code),
       nvl(1d_ago.version_code, old.version_code),
       case
           when old.mid_id is null and 1d_ago.is_new = 1 then '2025-03-22'
           when old.mid_id is null and 1d_ago.is_new = 0 then '2024-12-12'
           else old.visit_date_first
           end,
       `if`(1d_ago.mid_id is not null, '2025-03-22', old.visit_date_last),
       nvl(1d_ago.visit_count, 0),
       `if`(1d_ago.mid_id is null, 0, 1),
       nvl(old.visit_last_7d_count, 0) + nvl(1d_ago.visit_count, 0) - nvl(7d_ago.visit_count, 0),
       nvl(old.visit_last_7d_count, 0) + `if`(1d_ago.mid_id is null, 0, 1) - `if`(7d_ago.mid_id is null, 0, 1),
       nvl(old.visit_last_30d_count, 0) + nvl(1d_ago.visit_count, 0) - nvl(30d_ago.visit_count, 0),
       nvl(old.visit_last_30d_day_count, 0) + if(1d_ago.mid_id is null, 0, 1) - if(30d_ago.mid_id is null, 0, 1),
       nvl(old.visit_count, 0) + nvl(1d_ago.visit_count, 0),
       nvl(old.visit_day_count, 0) + if(1d_ago.mid_id is null, 0, 1)
from (select mid_id,
             brand,
             model,
             channel,
             os,
             area_code,
             version_code,
             visit_date_first,
             visit_date_last,
             visit_last_1d_count,
             visit_last_1d_day_count,
             visit_last_7d_count,
             visit_last_7d_day_count,
             visit_last_30d_count,
             visit_last_30d_day_count,
             visit_count,
             visit_day_count
      from dwt_visitor_topic
      where dt = date_add('2025-03-22', -1)) old
         full outer join
     (select mid_id,
             brand,
             model,
             is_new,
             channel,
             os,
             area_code,
             version_code,
             visit_count
      from dws_visitor_action_daycount
      where dt = '2025-03-22') 1d_ago
     on old.mid_id = 1d_ago.mid_id
         left join
     (select mid_id,
             brand,
             model,
             is_new,
             channel,
             os,
             area_code,
             version_code,
             visit_count
      from dws_visitor_action_daycount
      where dt >= date_add('2025-03-22', -6)) 7d_ago
     on old.mid_id = 7d_ago.mid_id
         left join
     (select mid_id,
             brand,
             model,
             is_new,
             channel,
             os,
             area_code,
             version_code,
             visit_count
      from dws_visitor_action_daycount
      where dt >= date_add('2025-03-22', -29)) 30d_ago
     on old.mid_id = 30d_ago.mid_id;

--ADS
DROP TABLE IF EXISTS ads_visit_stats;
CREATE EXTERNAL TABLE ads_visit_stats
(
    `dt`               STRING COMMENT '统计日期',
    `is_new`           STRING COMMENT '新老标识,1:新,0:老',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近 1 天,7:最近 7 天,30:最近 30 天',
    `channel`          STRING COMMENT '渠道',
    `uv_count`         BIGINT COMMENT '日活(访问人数)',
    `duration_sec`     BIGINT COMMENT '页面停留总时长',
    `avg_duration_sec` BIGINT COMMENT '一次会话，页面停留平均时长,单位为描述',
    `page_count`       BIGINT COMMENT '页面总浏览数',
    `avg_page_count`   BIGINT COMMENT '一次会话，页面平均浏览数',
    `sv_count`         BIGINT COMMENT '会话次数',
    `bounce_count`     BIGINT COMMENT '跳出数',
    `bounce_rate`      DECIMAL(16, 2) COMMENT '跳出率'
) COMMENT '访客统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_visit_stats/';

insert overwrite table ads_visit_stats
select *
from ads_visit_stats
union
select '2025-03-22'                                                           dt,
       is_new,
       recent_days,
       channel,
       count(distinct (mid_id))                                               uv_count,
       cast(sum(duration) / 1000 as bigint)                                   duration_sec,
       cast(avg(duration) / 1000 as bigint)                                   avg_duration_sec,
       sum(page_count)                                                        page_count,
       cast(avg(page_count) as bigint)                                        avg_page_count,
       count(*)                                                               sv_count,
       sum(if(page_count = 1, 1, 0))                                          bounce_count,
       cast(sum(if(page_count = 1, 1, 0)) / count(*) * 100 as decimal(16, 2)) bounce_rate
from (select session_id,
             mid_id,
             is_new,
             recent_days,
             channel,
             count(*)         page_count,
             sum(during_time) duration
      from (select mid_id,
                   channel,
                   recent_days,
                   is_new,
                   last_page_id,
                   page_id,
                   during_time,
                   concat(mid_id, '-', last_value(if(last_page_id is null, ts, null), true)
                       over (partition by recent_days,mid_id order
                                                      by ts)) session_id
            from (select mid_id,
                         channel,
                         last_page_id,
                         page_id,
                         during_time,
                         ts,
                         recent_days,
                         if(visit_date_first >= date_add('2025-03-22', -recent_days + 1), '1',
                            '0') is_new
                  from (select t1.mid_id,
                               t1.channel,
                               t1.last_page_id,
                               t1.page_id,
                               t1.during_time,
                               t1.dt,
                               t1.ts,
                               t2.visit_date_first
                        from (select mid_id,
                                     channel,
                                     last_page_id,
                                     page_id,
                                     during_time,
                                     dt,
                                     ts
                              from dwd_page_log
                              where dt >= date_add('2025-03-22', -30)) t1
                                 left join
                             (select mid_id,
                                     visit_date_first
                              from dwt_visitor_topic
                              where dt = '2025-03-22') t2
                             on t1.mid_id = t2.mid_id) t3 lateral view explode(Array(1, 7, 30)) tmp as
                           recent_days
                  where dt >= date_add('2025-03-22', -recent_days + 1)) t4) t5
      group by session_id, mid_id, is_new, recent_days, channel) t6
group by is_new, recent_days, channel;

--页面路径分析
DROP TABLE IF EXISTS ads_page_path;
CREATE EXTERNAL TABLE ads_page_path
(
    `dt`          STRING COMMENT '统计日期',
    `recent_days` BIGINT COMMENT '最近天数,1:最近 1 天,7:最近 7 天,30:最近 30 天',
    `source`      STRING COMMENT '跳转起始页面 ID',
    `target`      STRING COMMENT '跳转终到页面 ID',
    `path_count`  BIGINT COMMENT '跳转次数'
) COMMENT '页面浏览路径'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_page_path/';

insert overwrite table ads_page_path
select *
from ads_page_path
union
select '2025-03-22',
       recent_days,
       source,
       target,
       count(*)
from (select recent_days,
             concat('step-', step, ':', source)     source,
             concat('step-', step + 1, ':', target) target
      from (select recent_days,
                   page_id                                 source,
                   lead(page_id, 1, null) over (partition by
                       recent_days,session_id order by ts) target,
                    row_number() over (partition by recent_days,session_id
                       order by ts)                        step
            from (select recent_days,
                         last_page_id,
                         page_id,
                         ts,
                         concat(mid_id, '-', last_value(if(last_page_id is null, ts, null), true)
                             over (partition by mid_id,recent_days order
                                                            by ts)) session_id
                  from dwd_page_log lateral view explode(Array(1, 7, 30)) tmp as recent_days
                  where dt >= date_add('2025-03-22', -30)
                    and dt >= date_add('2025-03-22', -recent_days + 1)) t2) t3) t4
group by recent_days, source, target;

select * from ads_page_path;