create database if not exists dev_offline_ecommerce_v2;
use dev_offline_ecommerce_v2;
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

# --订单明细表
CREATE TABLE dev_offline_ecommerce_v2.order_detail
(
    order_id   BIGINT COMMENT '订单ID',
    product_id BIGINT COMMENT '商品ID',
    quantity   INT COMMENT '购买数量',
    price      DECIMAL(10, 2) COMMENT '商品单价'
) COMMENT '订单明细表';

# --退款记录表
CREATE TABLE dev_offline_ecommerce_v2.refund
(
    refund_id     BIGINT COMMENT '退款ID',
    order_id      BIGINT COMMENT '订单ID',
    refund_amount DECIMAL(10, 2) COMMENT '退款金额',
    refund_time   DATE COMMENT '退款时间',
    refund_type   varchar(255) COMMENT '退款类型(full/partial)'
) COMMENT '退款记录表';

# --用户基础表
CREATE TABLE dev_offline_ecommerce_v2.user
(
    user_id       BIGINT COMMENT '用户ID',
    register_time DATE COMMENT '注册时间'
) COMMENT '用户基础表';

truncate table `order`;
truncate table `order_detail`;
truncate table `product`;
truncate table `refund`;
truncate table `user`;
truncate table `user_action_log`;

select * from user_action_log;