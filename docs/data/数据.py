import mysql.connector
import random
import string
from datetime import datetime, timedelta

# 连接数据库
mydb = mysql.connector.connect(
    host="cdh03",
    user="root",
    password="root",
    database="dev_DianShang"
)

mycursor = mydb.cursor()

# 生成随机字符串
def random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

# 生成随机日期
def random_date(start_date, end_date):
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    return start_date + timedelta(days=random_number_of_days)

# 模拟生成order_info数据
for _ in range(100):  # 生成100条数据示例
    order_id = random_string(10)
    user_id = random_string(8)
    product_id = random_string(8)
    order_time = random_date(datetime(2025, 1, 1), datetime(2025, 12, 31))
    payment_time = random_date(order_time, order_time + timedelta(days=7))
    payment_amount = round(random.uniform(10, 1000), 2)
    payment_type = random.choice(['支付宝', '微信支付', '银行卡'])
    is_refund = random.randint(0, 1)

    sql = "INSERT INTO order_info (order_id, user_id, product_id, order_time, payment_time, payment_amount, payment_type, is_refund) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    val = (order_id, user_id, product_id, order_time, payment_time, payment_amount, payment_type, is_refund)
    mycursor.execute(sql, val)

# 模拟生成product_info数据
for _ in range(50):  # 生成50条数据示例
    product_id = random_string(8)
    product_name = random_string(15)
    category_id = random_string(6)
    price = round(random.uniform(10, 500), 2)
    stock = random.randint(0, 1000)

    sql = "INSERT INTO product_info (product_id, product_name, category_id, price, stock) VALUES (%s, %s, %s, %s, %s)"
    val = (product_id, product_name, category_id, price, stock)
    mycursor.execute(sql, val)

# 模拟生成user_behavior数据
for _ in range(200):  # 生成200条数据示例
    behavior_id = random_string(10)
    user_id = random_string(8)
    product_id = random_string(8)
    behavior_type = random.choice(['访问', '收藏', '加购'])
    behavior_time = random_date(datetime(2025, 1, 1), datetime(2025, 12, 31))
    page = random.randint(0, 300)

    sql = "INSERT INTO user_behavior (behavior_id, user_id, product_id, behavior_type, behavior_time, page_time) VALUES (%s, %s, %s, %s, %s, %s)"
    val = (behavior_id, user_id, product_id, behavior_type, behavior_time, page)
    mycursor.execute(sql, val)

mydb.commit()
mycursor.close()
mydb.close()