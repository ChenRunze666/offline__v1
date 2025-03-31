--每个节点可以有一万个分区
set hive.exec.max.dynamic.partitions.pernode=10000;
--设置最大分区数量
set hive.exec.max.dynamic.partitions=100000;
--创建最大文件数量
set hive.exec.max.created.files=150000;
--开启中间结果压缩
set hive.exec.compress.intermediate=true;
--开启最终结果压缩 map端
set hive.exec.compress.output=true;
--写入时压缩生效 reduce端
set hive.exec.orc.compression.strategy=COMPRESSION;

create database if not exists dev_offline_Ecommerce_v2;

use dev_offline_Ecommerce_v2;

SHOW CREATE DATABASE dev_offline_ecommerce_v2;


------01
--4
--商品基础表
CREATE external TABLE if not exists  dev_offline_Ecommerce_v2.ods_product (
    product_id        BIGINT COMMENT '商品ID',
    product_name      STRING COMMENT '商品名称',
    store_id          BIGINT COMMENT '店铺ID',
    category_id       BIGINT COMMENT '类目ID',
    create_time       string COMMENT '创建时间',
    update_time       string COMMENT '更新时间'
) COMMENT '商品基础信息表'
PARTITIONED BY (ds string COMMENT '日期分区')
stored as orc
location '/warehouse/dev_offline_ecommerce_v2/ods/ods_product/'
tblproperties ('orc.compress'='snappy');

--用户行为日志表
CREATE external TABLE if not exists  dev_offline_Ecommerce_v2.ods_user_action_log (
    user_id           BIGINT COMMENT '用户ID',
    product_id        BIGINT COMMENT '商品ID',
    action_type       STRING COMMENT '行为类型(visit/payment/favor/cart/order)',
    action_time       string COMMENT '行为时间',
    session_id        STRING COMMENT '会话ID',
    page_stay_time    BIGINT COMMENT '页面停留时长(ms)',
    referer_page      STRING COMMENT '来源页面'
) COMMENT '用户行为日志表'
PARTITIONED BY (ds string COMMENT '日期分区')
stored as orc
location '/warehouse/dev_offline_ecommerce_v2/ods/ods_user_action_log/'
tblproperties ('orc.compress'='snappy');

--订单主表
CREATE external TABLE if not exists  dev_offline_Ecommerce_v2.ods_order (
    order_id          BIGINT COMMENT '订单ID',
    user_id           BIGINT COMMENT '用户ID',
    order_time        string COMMENT '下单时间',
    payment_time      string COMMENT '支付时间',
    total_amount      DECIMAL(10,2) COMMENT '订单总金额',
    order_status      INT COMMENT '订单状态',
    user_type         STRING COMMENT '用户类型(new/old)'
) COMMENT '订单主表'
PARTITIONED BY (ds string COMMENT '日期分区')
stored as orc
location '/warehouse/dev_offline_ecommerce_v2/ods/ods_order/'
tblproperties ('orc.compress'='snappy');

--退款记录表
CREATE external TABLE if not exists  dev_offline_Ecommerce_v2.ods_refund (
    refund_id         BIGINT COMMENT '退款ID',
    order_id          BIGINT COMMENT '订单ID',
    refund_amount     DECIMAL(10,2) COMMENT '退款金额',
    refund_time       string COMMENT '退款时间',
    refund_type       STRING COMMENT '退款类型(full/partial)'
) COMMENT '退款记录表'
PARTITIONED BY (ds string COMMENT '日期分区')
stored as orc
location '/warehouse/dev_offline_ecommerce_v2/ods/ods_refund/'
tblproperties ('orc.compress'='snappy');

------------------------02-----------------------------
--4
-- 商品库存表（记录SKU库存信息）
CREATE external TABLE if not exists  dev_offline_ecommerce_v2.ods_product_inventory (
    sku_id           BIGINT COMMENT 'SKU ID',
    product_id       BIGINT COMMENT '商品ID',
    stock            INT COMMENT '当前库存',
    update_time      string COMMENT '更新时间'
) COMMENT '商品库存表'
PARTITIONED BY (ds string COMMENT '日期分区')
stored as orc
location '/warehouse/dev_offline_ecommerce_v2/ods/ods_product_inventory/'
tblproperties ('orc.compress'='snappy');

-- 流量来源分类表（细化流量来源类型）
CREATE external TABLE if not exists  dev_offline_ecommerce_v2.ods_traffic_source (
    source_id        BIGINT COMMENT '来源ID',
    source_type      VARCHAR(255) COMMENT '来源类型（如手淘搜索、效果广告等）',
    source_detail    VARCHAR(255) COMMENT '来源详情'
) COMMENT '流量来源分类表'
PARTITIONED BY (ds string COMMENT '日期分区')
stored as orc
location '/warehouse/dev_offline_ecommerce_v2/ods/ods_traffic_source/'
tblproperties ('orc.compress'='snappy');

-- 搜索关键词记录表
CREATE external TABLE if not exists  dev_offline_ecommerce_v2.ods_search_keyword_log (
    keyword         VARCHAR(255) COMMENT '搜索词',
    user_id         BIGINT COMMENT '用户ID',
    product_id      BIGINT COMMENT '商品ID',
    search_time     string COMMENT '搜索时间',
    session_id      VARCHAR(255) COMMENT '会话ID'
) COMMENT '搜索关键词日志表'
PARTITIONED BY (ds string COMMENT '日期分区')
stored as orc
location '/warehouse/dev_offline_ecommerce_v2/ods/ods_search_keyword_log/'
tblproperties ('orc.compress'='snappy');

-- 价格力商品信息表
CREATE external TABLE if not exists  dev_offline_ecommerce_v2.ods_price_force_product (
    product_id          BIGINT COMMENT '商品ID',
    price_force_star    BIGINT COMMENT '价格力星级（1-5星）',
    coupon_price        DECIMAL(10,2) COMMENT '普惠券后价',
    force_warning       VARCHAR(255) COMMENT '预警状态（如低价格力、低商品力）',
    update_time         string COMMENT '更新时间'
) COMMENT '价格力商品信息表'
PARTITIONED BY (ds string COMMENT '日期分区')
stored as orc
location '/warehouse/dev_offline_ecommerce_v2/ods/ods_price_force_product/'
tblproperties ('orc.compress'='snappy');