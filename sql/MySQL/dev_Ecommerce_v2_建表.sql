create database if not exists dev_offline_ecommerce_v2;
use dev_offline_ecommerce_v2;
SHOW CREATE DATABASE dev_offline_ecommerce_v2;
ALTER DATABASE dev_offline_ecommerce_v2 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

# --商品基础表
CREATE TABLE dev_offline_ecommerce_v2.product
(
    product_id   BIGINT COMMENT '商品ID',
    product_name varchar(255) COMMENT '商品名称',
    store_id     BIGINT COMMENT '店铺ID',
    category_id  BIGINT COMMENT '类目ID',
    create_time  DATE COMMENT '创建时间',
    update_time  DATE COMMENT '更新时间'
) COMMENT '商品基础信息表';

# --用户行为日志表
CREATE TABLE dev_offline_ecommerce_v2.user_action_log
(
    user_id        BIGINT COMMENT '用户ID',
    product_id     BIGINT COMMENT '商品ID',
    action_type    varchar(255) COMMENT '行为类型(visit/payment/favor/cart/order)',
    action_time    DATE COMMENT '行为时间',
    session_id     varchar(255) COMMENT '会话ID',
    page_stay_time BIGINT COMMENT '页面停留时长(ms)',
    referer_page   varchar(255) COMMENT '来源页面'
) COMMENT '用户行为日志表';

# --订单主表
CREATE TABLE dev_offline_ecommerce_v2.order
(
    order_id     BIGINT COMMENT '订单ID',
    user_id      BIGINT COMMENT '用户ID',
    order_time   DATE COMMENT '下单时间',
    payment_time DATE COMMENT '支付时间',
    total_amount DECIMAL(10, 2) COMMENT '订单总金额',
    order_status INT COMMENT '订单状态',
    user_type    varchar(255) COMMENT '用户类型(new/old)'
) COMMENT '订单主表';

# --退款记录表
CREATE TABLE dev_offline_ecommerce_v2.refund
(
    refund_id     BIGINT COMMENT '退款ID',
    order_id      BIGINT COMMENT '订单ID',
    refund_amount DECIMAL(10, 2) COMMENT '退款金额',
    refund_time   DATE COMMENT '退款时间',
    refund_type   varchar(255) COMMENT '退款类型(full/partial)'
) COMMENT '退款记录表';


# ------------------02-----------------
-- 商品库存表（记录SKU库存信息）
CREATE TABLE dev_offline_ecommerce_v2.product_inventory (
    sku_id           BIGINT COMMENT 'SKU ID',
    product_id       BIGINT COMMENT '商品ID',
    stock            INT COMMENT '当前库存',
    update_time      date COMMENT '更新时间'
) COMMENT '商品库存表';

-- 流量来源分类表（细化流量来源类型）
CREATE TABLE dev_offline_ecommerce_v2.traffic_source (
    source_id        BIGINT COMMENT '来源ID',
    source_type      VARCHAR(255) COMMENT '来源类型（如手淘搜索、效果广告等）',
    source_detail    VARCHAR(255) COMMENT '来源详情'
) COMMENT '流量来源分类表';

-- 搜索关键词记录表
CREATE TABLE dev_offline_ecommerce_v2.search_keyword_log (
    keyword         VARCHAR(255) COMMENT '搜索词',
    user_id         BIGINT COMMENT '用户ID',
    product_id      BIGINT COMMENT '商品ID',
    search_time     date COMMENT '搜索时间',
    session_id      VARCHAR(255) COMMENT '会话ID'
) COMMENT '搜索关键词日志表';

-- 价格力商品信息表
CREATE TABLE dev_offline_ecommerce_v2.price_force_product (
    product_id          BIGINT COMMENT '商品ID',
    price_force_star    INT COMMENT '价格力星级（1-5星）',
    coupon_price        DECIMAL(10,2) COMMENT '普惠券后价',
    force_warning       VARCHAR(255) COMMENT '预警状态（如低价格力、低商品力）',
    update_time         date COMMENT '更新时间'
) COMMENT '价格力商品信息表';



select * from user_action_log;

SELECT *, DATE_FORMAT(order_time, '%Y%m%d') AS dt FROM dev_offline_ecommerce_v2.order;
