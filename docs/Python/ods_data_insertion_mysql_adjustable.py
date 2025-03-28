import pymysql
import random
from datetime import datetime, timedelta

# 数据库连接配置
db_config = {
    'host': 'cdh03',
    'user': 'root',
    'password': 'root',
    'database': 'dev_offline_ecommerce_v2',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}


# 生成随机日期
def random_date(start_date, end_date):
    # 将 start_date 和 end_date 统一转换为 datetime.date 类型
    if isinstance(start_date, datetime):
        start_date = start_date.date()
    if isinstance(end_date, datetime):
        end_date = end_date.date()
    time_diff = end_date - start_date
    random_days = random.randint(0, time_diff.days)
    return start_date + timedelta(days=random_days)


# 生成商品基础表数据
def generate_ods_product(num_records, start_date, end_date):
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            for i in range(num_records):
                product_id = i + 1
                product_name = f'Product {i + 1}'
                store_id = random.randint(1, 10)
                category_id = random.randint(1, 5)
                create_time = random_date(start_date, end_date)
                update_time = random_date(create_time, end_date)
                sql = "INSERT INTO product (product_id, product_name, store_id, category_id, create_time, update_time) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (product_id, product_name, store_id, category_id, create_time, update_time))
        connection.commit()
    finally:
        connection.close()


# 生成用户行为日志表数据
def generate_ods_user_action_log(num_records, product_count, user_count, start_date, end_date):
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            for i in range(num_records):
                user_id = random.randint(1, user_count)
                product_id = random.randint(1, product_count)
                action_types = ['visit', 'payment', 'favor', 'cart', 'order']
                action_type = random.choice(action_types)
                action_time = random_date(start_date, end_date)
                session_id = f'session_{random.randint(1, 100)}'
                page_stay_time = random.randint(1000, 60000)
                referer_pages = ['home', 'category', 'search']
                referer_page = random.choice(referer_pages)
                sql = "INSERT INTO user_action_log (user_id, product_id, action_type, action_time, session_id, page_stay_time, referer_page) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (
                user_id, product_id, action_type, action_time, session_id, page_stay_time, referer_page))
        connection.commit()
    finally:
        connection.close()


# 生成订单主表数据
def generate_ods_order(num_records, user_count, start_date, end_date):
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            for i in range(num_records):
                order_id = i + 1
                user_id = random.randint(1, user_count)
                order_time = random_date(start_date, end_date)
                payment_time = random_date(order_time, end_date)
                total_amount = round(random.uniform(10, 1000), 2)
                order_status = random.randint(1, 5)
                user_types = ['new', 'old']
                user_type = random.choice(user_types)
                sql = "INSERT INTO `order` (order_id, user_id, order_time, payment_time, total_amount, order_status, user_type) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql,
                               (order_id, user_id, order_time, payment_time, total_amount, order_status, user_type))
        connection.commit()
    finally:
        connection.close()


# 生成订单明细表数据
def generate_ods_order_detail(num_records, order_count, product_count, start_date, end_date):
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            for i in range(num_records):
                order_id = random.randint(1, order_count)
                product_id = random.randint(1, product_count)
                quantity = random.randint(1, 10)
                price = round(random.uniform(10, 100), 2)
                sql = "INSERT INTO order_detail (order_id, product_id, quantity, price) VALUES (%s, %s, %s, %s)"
                cursor.execute(sql, (order_id, product_id, quantity, price))
        connection.commit()
    finally:
        connection.close()


# 生成退款记录表数据
def generate_ods_refund(num_records, order_count, start_date, end_date):
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            for i in range(num_records):
                refund_id = i + 1
                order_id = random.randint(1, order_count)
                refund_amount = round(random.uniform(10, 1000), 2)
                refund_time = random_date(start_date, end_date)
                refund_types = ['full', 'partial']
                refund_type = random.choice(refund_types)
                sql = "INSERT INTO refund (refund_id, order_id, refund_amount, refund_time, refund_type) VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(sql, (refund_id, order_id, refund_amount, refund_time, refund_type))
        connection.commit()
    finally:
        connection.close()


# 生成用户基础表数据
def generate_ods_user(num_records, start_date, end_date):
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            for i in range(num_records):
                user_id = i + 1
                register_time = random_date(start_date, end_date)
                sql = "INSERT INTO user (user_id, register_time) VALUES (%s, %s)"
                cursor.execute(sql, (user_id, register_time))
        connection.commit()
    finally:
        connection.close()


if __name__ == "__main__":
    # 可调节的数据量变量
    product_count = 400
    user_count = 300
    order_count = 600
    product_record_num = 400
    user_action_log_record_num = 1000
    order_record_num = 600
    order_detail_record_num = 1200
    refund_record_num = 200
    user_record_num = 300

    # 指定日期范围
    start_date = datetime(2024, 2, 27)
    end_date = datetime(2024, 3, 27)

    generate_ods_product(product_record_num, start_date, end_date)
    generate_ods_user_action_log(user_action_log_record_num, product_count, user_count, start_date, end_date)
    generate_ods_order(order_record_num, user_count, start_date, end_date)
    generate_ods_order_detail(order_detail_record_num, order_count, product_count, start_date, end_date)
    generate_ods_refund(refund_record_num, order_count, start_date, end_date)
    generate_ods_user(user_record_num, start_date, end_date)
