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


--商品基础表
CREATE external TABLE dev_offline_Ecommerce_v2.ods_product (
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
CREATE TABLE dev_offline_Ecommerce_v2.ods_user_action_log (
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
CREATE TABLE dev_offline_Ecommerce_v2.ods_order (
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

--订单明细表
CREATE TABLE dev_offline_Ecommerce_v2.ods_order_detail (
    order_id          BIGINT COMMENT '订单ID',
    product_id        BIGINT COMMENT '商品ID',
    quantity          INT COMMENT '购买数量',
    price             DECIMAL(10,2) COMMENT '商品单价'
) COMMENT '订单明细表'
PARTITIONED BY (ds string COMMENT '日期分区')
stored as orc
location '/warehouse/dev_offline_ecommerce_v2/ods/ods_order_detail/'
tblproperties ('orc.compress'='snappy');

--退款记录表
CREATE TABLE dev_offline_Ecommerce_v2.ods_refund (
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

--用户基础表
CREATE TABLE dev_offline_Ecommerce_v2.ods_user (
    user_id           BIGINT COMMENT '用户ID',
    register_time     string COMMENT '注册时间'
) COMMENT '用户基础表'
PARTITIONED BY (ds string COMMENT '日期分区')
stored as orc
location '/warehouse/dev_offline_ecommerce_v2/ods/ods_user/'
tblproperties ('orc.compress'='snappy');
