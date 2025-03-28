-- 全量快照 day更新 每天，每个店铺的每个商品的详情指标统计 source -> ods -> dws
create external table dev_offline_ecommerce_v2.dws_product_info(
    product_id bigint COMMENT '商品Id',
    product_name string COMMENT '商品名称',
    store_id bigint COMMENT '店铺Id',
    recent_days bigint COMMENT '时间周期,1:最近 1 天,7:最近 7 天,30:最近 30 天',
    product_visit_num_uv bigint COMMENT '商品访客数',
    product_visit_num_pv bigint COMMENT '商品浏览量',
    product_stopover_avg_ts bigint COMMENT '商品平均停留时长',
    product_info_page_jump_rate string COMMENT '商品详情页跳出率',
    product_favorite_people_num bigint COMMENT '商品收藏人数',
    product_cart_packages_num bigint COMMENT '商品加购件数',
    product_cart_people_num bigint COMMENT '商品加购人数',
    product_favorite_rate string COMMENT '访问收藏转化率',
    product_cart_rate string COMMENT '访问收购转化率',
    product_order_people_num bigint COMMENT '下单买家数',
    product_order_packages_num bigint COMMENT '下单件数',
    product_order_price decimal(10,2) COMMENT '下单金额',
    product_order_rate string COMMENT '下单转化率',
    product_payment_people_num bigint COMMENT '支付买家数',
    product_payment_packages_num bigint COMMENT '支付件数',
    product_payment_price decimal(10,2) COMMENT '支付金额',
    product_payment_rate string COMMENT '支付转化率',
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
location '/warehouse/dev_offline_ecommerce_v2/dws/ads_page_path/'
tblproperties ('orc.compress'='snappy');





select
    /*+
        Mapjoin(
                ods_product,
                ods_order,
                ods_refund
        )
    */
    --维度
    ual.product_id,
    p.product_name,
    p.store_id,
    recent_days,
    --访客
    count(distinct if(ual.action_type='visit',ual.user_id,null)) as product_visit_num_uv,
    count(if(ual.action_type='visit',ual.user_id,null)) as product_visit_num_pv,
    avg(ual.page_stay_time) as product_stopover_avg_ts,
    count(distinct if(ual.action_type='cart' or ual.action_type='favor' or ual.action_type='order',ual.user_id,null))/
    count(distinct if(ual.action_type='visit',ual.user_id,null)) as product_info_page_jump_rate,
    --收藏、加购
    count(distinct if(ual.action_type='favor',ual.user_id,null)) as product_favorite_people_num,
    count(if(ual.action_type='cart',ual.user_id,null)) as product_cart_packages_num,
    count(distinct if(ual.action_type='cart',ual.user_id,null)) as product_cart_people_num,
    count(distinct if(ual.action_type='favor',ual.user_id,null))/
    count(distinct if(ual.action_type='visit',ual.user_id,null)) as product_favorite_rate,
    count(distinct if(ual.action_type='favor',ual.user_id,null))/
    count(distinct if(ual.action_type='cart',ual.user_id,null)) as product_cart_rate,
    --下单
    count(distinct if(ual.action_type='order',ual.user_id,null)) as product_order_people_num,
    count(if(ual.action_type='order',ual.user_id,null)) as product_order_packages_num,
    sum(if(ual.action_type='order',o.total_amount,0)) as product_order_price,
    count(distinct if(ual.action_type='order',ual.user_id,null))/
    count(distinct if(ual.action_type='visit',ual.user_id,null)) as product_order_rate,
    --支付
    count(distinct if(ual.action_type='payment',ual.user_id,null)) as product_payment_people_num,
    count(if(ual.action_type='payment',ual.user_id,null)) as product_payment_packages_num,
    sum(if(ual.action_type='payment',o.total_amount,0)) as product_payment_price,
    count(distinct if(ual.action_type='payment',ual.user_id,null))/
    count(distinct if(ual.action_type='visit',ual.user_id,null)) as product_payment_rate,
    --新老用户
    count(distinct if(ual.action_type='payment' and o.user_type='new',ual.user_id,null)) as product_payment_people_new_num,
    count(distinct if(ual.action_type='payment' and o.user_type='old',ual.user_id,null)) as product_payment_people_old_num,
    sum(if(ual.action_type='payment' and o.user_type='old',o.total_amount,0)) as product_payment_people_old_price,
    sum(if(ual.action_type='payment',o.total_amount,0))/
    count(distinct if(ual.action_type='payment',ual.user_id,null)) as product_payment_avg,
    sum(if(r.refund_type='full',r.refund_amount,0)) as product_refund_price,
    sum(if(ual.action_type='payment' and year(ual.action_time)=substr(ual.ds,0,4),o.total_amount,0)) as product_payment_price,
    sum(if(ual.action_type='payment',o.total_amount,0))/
    count(distinct if(ual.action_type='visit',ual.user_id,null)) as product_payment_visit_avg,
    count(if(ual.action_type='visit' and o.user_type='new',ual.user_id,null)) as product_Competitiveness_score, --新浏览的用户作为竞争力
    count(distinct if(ual.action_type='visit' and ual.referer_page/1000>=3,ual.user_id,null)) as product_micro_details_num_visit

from ods_user_action_log ual lateral view explode(Array(1, 7, 30)) tmp as recent_days
inner join ods_product p on ual.product_id = p.product_id and ual.ds='20250330' = p.ds='20250330'
inner join ods_order o on ual.user_id = o.user_id and o.ds='20250330' and ual.ds='20250330'
inner join ods_refund r on r.order_id = o.order_id and r.ds='20250330' and o.ds='20250330'
group by ual.product_id, p.product_name, p.store_id, recent_days;


