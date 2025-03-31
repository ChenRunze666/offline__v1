------01
--1
-- 全量快照 day更新 每天，每个店铺的每个商品的详情指标统计 source -> ods -> dws
drop table if exists dev_offline_ecommerce_v2.dws_product_info;
create table dev_offline_ecommerce_v2.dws_product_info(
    product_id bigint COMMENT '商品Id',
    product_name string COMMENT '商品名称',
    store_id bigint COMMENT '店铺Id',
    recent_days bigint COMMENT '时间周期,1:最近 1 天,7:最近 7 天,30:最近 30 天',
    product_visit_num_uv bigint COMMENT '商品访客数',
    product_visit_num_pv bigint COMMENT '商品浏览量',
    product_stopover_avg_ts bigint COMMENT '商品平均停留时长',
    product_info_page_jump_rate decimal(10,2) COMMENT '商品详情页跳出率',
    product_favorite_people_num bigint COMMENT '商品收藏人数',
    product_cart_packages_num bigint COMMENT '商品加购件数',
    product_cart_people_num bigint COMMENT '商品加购人数',
    product_favorite_rate decimal(10,2) COMMENT '访问收藏转化率',
    product_cart_rate decimal(10,2) COMMENT '访问收购转化率',
    product_order_people_num bigint COMMENT '下单买家数',
    product_order_packages_num bigint COMMENT '下单件数',
    product_order_price decimal(10,2) COMMENT '下单金额',
    product_order_rate decimal(10,2) COMMENT '下单转化率',
    product_payment_people_num bigint COMMENT '支付买家数',
    product_payment_packages_num bigint COMMENT '支付件数',
    product_payment_price decimal(10,2) COMMENT '支付金额',
    product_payment_rate decimal(10,2) COMMENT '支付转化率',
    product_payment_people_new_num bigint COMMENT '支付新买家数',
    product_payment_people_old_num bigint COMMENT '支付老买家数',
    product_payment_people_old_price decimal(10,2) COMMENT '老买家支付金额',
    product_payment_avg decimal(10,2) COMMENT '客单价',
    product_refund_price decimal(10,2) COMMENT '成功退款退货金额',
    product_payment_year_price decimal(10,2) COMMENT '年累计支付金额',
    product_payment_visit_avg decimal(10,2) COMMENT '访客平均价值',
    product_Competitiveness_score decimal(10,2) COMMENT '竞争力评分',
    product_micro_details_num_visit bigint COMMENT '商品微详情访客数'
)COMMENT '商品详情主题宽表'
    partitioned by(ds bigint COMMENT '日期分区')
stored as orc
tblproperties ('orc.compress'='snappy');

WITH date_filtered_data AS (
    SELECT
        ual.*,
        p.product_name,
        p.store_id,
        o.total_amount,
        o.user_type,
        r.refund_type,
        r.refund_amount
    FROM ods_user_action_log ual
    INNER JOIN ods_product p ON ual.product_id = p.product_id AND ual.ds='20250330' AND p.ds='20250330'
    INNER JOIN ods_order o ON ual.user_id = o.user_id AND o.ds='20250330'
    INNER JOIN ods_refund r ON r.order_id = o.order_id AND r.ds='20250330'
)
insert overwrite table dws_product_info partition (ds='20250330')
SELECT
    /*+ MAPJOIN(p) */
    product_id,
    product_name,
    store_id,
    recent_days,
    --访客
    count(distinct if(action_type='visit',user_id,null)) as product_visit_num_uv,
    count(if(action_type='visit',user_id,null)) as product_visit_num_pv,
    avg(page_stay_time) as product_stopover_avg_ts,
    count(distinct if(action_type='cart' or action_type='favor' or action_type='order',user_id,null))/
    count(distinct if(action_type='visit',user_id,null)) as product_info_page_jump_rate,
    --收藏、加购
    count(distinct if(action_type='favor',user_id,null)) as product_favorite_people_num,
    count(if(action_type='cart',user_id,null)) as product_cart_packages_num,
    count(distinct if(action_type='cart',user_id,null)) as product_cart_people_num,
    count(distinct if(action_type='favor',user_id,null))/
    count(distinct if(action_type='visit',user_id,null)) as product_favorite_rate,
    count(distinct if(action_type='favor',user_id,null))/
    count(distinct if(action_type='cart',user_id,null)) as product_cart_rate,
    --下单
    count(distinct if(action_type='order',user_id,null)) as product_order_people_num,
    count(if(action_type='order',user_id,null)) as product_order_packages_num,
    sum(if(action_type='order',total_amount,0)) as product_order_price,
    count(distinct if(action_type='order',user_id,null))/
    count(distinct if(action_type='visit',user_id,null)) as product_order_rate,
    --支付
    count(distinct if(action_type='payment',user_id,null)) as product_payment_people_num,
    count(if(action_type='payment',user_id,null)) as product_payment_packages_num,
    sum(if(action_type='payment',total_amount,0)) as product_payment_price,
    count(distinct if(action_type='payment',user_id,null))/
    count(distinct if(action_type='visit',user_id,null)) as product_payment_rate,
    --新老用户
    count(distinct if(action_type='payment' and user_type='new',user_id,null)) as product_payment_people_new_num,
    count(distinct if(action_type='payment' and user_type='old',user_id,null)) as product_payment_people_old_num,
    sum(if(action_type='payment' and user_type='old',total_amount,0)) as product_payment_people_old_price,
    sum(if(action_type='payment',total_amount,0))/
    count(distinct if(action_type='payment',user_id,null)) as product_payment_avg,
    sum(if(refund_type='full',refund_amount,0)) as product_refund_price,
    sum(if(action_type='payment' and year(action_time)=substr(ds,0,4),total_amount,0)) as product_payment_year_price,
    sum(if(action_type='payment',total_amount,0))/
    count(distinct if(action_type='visit',user_id,null)) as product_payment_visit_avg,
    count(if(action_type='visit' and user_type='new',user_id,null)) as product_Competitiveness_score,
    count(distinct if(action_type='visit' and referer_page/1000>=3,user_id,null)) as product_micro_details_num_visit
FROM date_filtered_data
LATERAL VIEW explode(Array(1, 7, 30)) tmp AS recent_days
GROUP BY product_id, product_name, store_id, recent_days;





---------------------------------------02------------------------------------------------------
--5

--商品销售汇总表
drop table if exists dev_offline_ecommerce_v2.dws_product_sales;
CREATE TABLE dev_offline_ecommerce_v2.dws_product_sales
(
    product_id            BIGINT COMMENT '商品ID',
    category_id           BIGINT COMMENT '类目ID',
    recent_days           bigint COMMENT '时间周期,1:最近 1 天,7:最近 7 天,30:最近 30 天',
    product_payment_price DECIMAL(18, 2) COMMENT '支付金额',
    product_payment_num   BIGINT COMMENT '支付件数',
    product_payment_rate  decimal(10, 2) COMMENT '支付转化率',
    product_cart_num      BIGINT COMMENT '商品加购件数',
    traffic_uv            bigint COMMENT '价格力星级'
) COMMENT '商品销售汇总表（按日聚合）'
    PARTITIONED BY (ds STRING COMMENT '时间分区（日/周/月）')
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

--流量来源分析表
drop table if exists dev_offline_ecommerce_v2.dws_traffic_source;
CREATE TABLE dev_offline_ecommerce_v2.dws_traffic_source
(
    source_id       BIGINT COMMENT '来源ID',
    source_type     VARCHAR(255) COMMENT '来源类型',
    visitor_count   INT COMMENT '访客数',
    payment_count   INT COMMENT '支付买家数',
    conversion_rate DECIMAL(5, 2) COMMENT '支付转化率',
    source_rank bigint comment '流量来源排名'
) COMMENT '流量来源分析表（按日聚合）'
    PARTITIONED BY (ds STRING COMMENT '时间分区')
stored as orc
tblproperties ('orc.compress'='snappy');

--SKU销售与库存表
drop table if exists dev_offline_ecommerce_v2.dws_sku_sales_inventory;
CREATE TABLE dev_offline_ecommerce_v2.dws_sku_sales_inventory
(
    sku_id           BIGINT COMMENT 'SKU ID',
    product_id       BIGINT COMMENT '商品ID',
    sales_quantity   INT COMMENT '销售件数',
    stock            INT COMMENT '当前库存',
    inventory_days   INT COMMENT '库存可售天数',
    sku_rank bigint comment 'SKU 销售排名'
) COMMENT 'SKU销售与库存表（按日聚合）'
    PARTITIONED BY (ds STRING COMMENT '时间分区')
stored as orc
tblproperties ('orc.compress'='snappy');

--搜索词热度表
drop table if exists dev_offline_ecommerce_v2.dws_search_keyword;
CREATE TABLE dev_offline_ecommerce_v2.dws_search_keyword
(
    keyword       VARCHAR(255) COMMENT '搜索词',
    visitor_count INT COMMENT '关联访客数',
    search_rank bigint comment '搜索词排名'
) COMMENT '搜索词热度表（按日聚合）'
    PARTITIONED BY (ds STRING COMMENT '时间分区')
stored as orc
tblproperties ('orc.compress'='snappy');

--价格力商品分析表
drop table if exists dev_offline_ecommerce_v2.dws_price_force_analysis;
CREATE TABLE dev_offline_ecommerce_v2.dws_price_force_analysis
(
    product_id       BIGINT COMMENT '商品ID',
    price_force_star INT COMMENT '价格力星级',
    coupon_price     DECIMAL(10, 2) COMMENT '普惠券后价',
    Flow_index    DECIMAL(10, 2) COMMENT '流量指数',
    payment_num_index DECIMAL(10, 2) COMMENT '支付件数指数',
    payment_rate  DECIMAL(10, 2) COMMENT '市场平均转化率'
) COMMENT '价格力商品分析表（按日聚合）'
    PARTITIONED BY (ds STRING COMMENT '时间分区')
stored as orc
tblproperties ('orc.compress'='snappy');


