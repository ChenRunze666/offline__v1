-- 创建order_info表，存储订单信息
CREATE TABLE order_info (
                            order_id VARCHAR(255) NOT NULL COMMENT '订单ID',
                            user_id VARCHAR(255) NOT NULL COMMENT '用户ID',
                            product_id VARCHAR(255) NOT NULL COMMENT '商品ID',
                            order_time DATETIME COMMENT '下单时间',
                            payment_time DATETIME COMMENT '支付时间',
                            payment_amount DECIMAL(10, 2) COMMENT '支付金额',
                            payment_type VARCHAR(255) COMMENT '支付类型',
                            is_refund TINYINT(1) COMMENT '是否退款（0否，1是）',
                            PRIMARY KEY (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '订单信息原始表';

-- 创建product_info表，存储商品信息
CREATE TABLE product_info (
                              product_id VARCHAR(255) NOT NULL COMMENT '商品ID',
                              product_name VARCHAR(255) COMMENT '商品名称',
                              category_id VARCHAR(255) COMMENT '商品类目ID',
                              price DECIMAL(10, 2) COMMENT '商品价格',
                              stock INT COMMENT '商品库存',
                              PRIMARY KEY (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品信息原始表';

-- 创建user_behavior表，存储用户行为信息
CREATE TABLE user_behavior (
                               behavior_id VARCHAR(255) NOT NULL COMMENT '行为ID',
                               user_id VARCHAR(255) NOT NULL COMMENT '用户ID',
                               product_id VARCHAR(255) NOT NULL COMMENT '商品ID',
                               behavior_type VARCHAR(255) COMMENT '行为类型（访问、收藏、加购等）',
                               behavior_time DATETIME COMMENT '行为发生时间',
                               page_time INT COMMENT '在商品详情页停留时长（秒）',
                               PRIMARY KEY (behavior_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '用户行为信息原始表';