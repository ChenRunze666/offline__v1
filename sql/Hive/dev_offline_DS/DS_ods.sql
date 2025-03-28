--每个节点可以有一万个分区
set
hive.exec.max.dynamic.partitions.pernode=10000;
--设置最大分区数量
set
hive.exec.max.dynamic.partitions=100000;
--创建最大文件数量
set
hive.exec.max.created.files=150000;
--开启中间结果压缩
set
hive.exec.compress.intermediate=true;
--开启最终结果压缩 map端
set
hive.exec.compress.output=true;
--写入时压缩生效 reduce端
set
hive.exec.orc.compression.strategy=COMPRESSION;


use
dev_dianshang;

-- ods_order_info表，存储订单信息
CREATE TABLE IF NOT EXISTS ods_order_info
(
    order_id
    STRING,
    user_id
    STRING,
    product_id
    STRING,
    order_time
    TIMESTAMP,
    payment_time
    TIMESTAMP,
    payment_amount
    DECIMAL
(
    10,
    2
),
    payment_type STRING,
    is_refund TINYINT
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- ods_product_info表，存储商品信息
CREATE TABLE IF NOT EXISTS ods_product_info
(
    product_id
    STRING,
    product_name
    STRING,
    category_id
    STRING,
    price
    DECIMAL
(
    10,
    2
),
    stock INT
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- ods_user_behavior表，存储用户行为信息
CREATE TABLE IF NOT EXISTS ods_user_behavior
(
    behavior_id
    STRING,
    user_id
    STRING,
    product_id
    STRING,
    behavior_type
    STRING,
    behavior_time
    TIMESTAMP,
    page_time
    INT
)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;