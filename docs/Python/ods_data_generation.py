import pymysql
import random
from datetime import datetime, timedelta


def generate_product_inventory_data(start_date, end_date, num_records):
    data = []
    for _ in range(num_records):
        sku_id = random.randint(1, 10000)
        product_id = random.randint(1, 1000)
        stock = random.randint(0, 1000)
        update_date = start_date + timedelta(
            days=random.randint(0, (end_date - start_date).days))
        data.append((sku_id, product_id, stock, update_date))
    return data


def generate_traffic_source_data(num_records):
    source_types = ["手淘搜索", "效果广告", "社交媒体", "直接访问", "站外广告", "内容广告", "购物车", "我的淘宝",
                    "手淘其他店铺", "手淘推荐", "品牌广告", "淘内待分类"]
    data = []
    for _ in range(num_records):
        source_id = random.randint(1, 100)
        source_type = random.choice(source_types)
        source_detail = f"详情_{source_type}_{random.randint(1, 10)}"
        data.append((source_id, source_type, source_detail))
    return data


def generate_search_keyword_log_data(start_date, end_date, num_records):
    keywords = ["手机", "电脑", "衣服", "鞋子"]
    data = []
    for _ in range(num_records):
        keyword = random.choice(keywords)
        user_id = random.randint(1, 1000)
        product_id = random.randint(1, 1000)
        search_date = start_date + timedelta(
            days=random.randint(0, (end_date - start_date).days))
        session_id = f"session_{random.randint(1, 1000)}"
        data.append((keyword, user_id, product_id, search_date, session_id))
    return data


def generate_price_force_product_data(start_date, end_date, num_records):
    data = []
    for _ in range(num_records):
        product_id = random.randint(1, 1000)
        price_force_star = random.randint(1, 5)
        coupon_price = round(random.uniform(10.0, 1000.0), 2)
        force_warning = random.choice(["低价格力", "低商品力", "正常"])
        update_date = start_date + timedelta(
            days=random.randint(0, (end_date - start_date).days))
        data.append((product_id, price_force_star, coupon_price, force_warning, update_date))
    return data


def insert_data_to_db(data, table_name, columns, conn):
    cursor = conn.cursor()
    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(columns)
    sql = f"INSERT INTO dev_offline_ecommerce_v2.{table_name} ({columns_str}) VALUES ({placeholders})"
    cursor.executemany(sql, data)
    conn.commit()


def main():
    # 连接数据库
    conn = pymysql.connect(
        host='cdh03',
        user='root',
        password='root',
        database='dev_offline_ecommerce_v2',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    # 定义时间范围和数据数量
    start_date = datetime(2025, 2, 27).date()
    end_date = datetime(2025, 3, 30).date()
    num_records = 500

    # 生成商品库存表数据
    product_inventory_data = generate_product_inventory_data(start_date, end_date, num_records)
    insert_data_to_db(product_inventory_data, "product_inventory",
                      ["sku_id", "product_id", "stock", "update_time"], conn)

    # 生成流量来源分类表数据
    traffic_source_data = generate_traffic_source_data(num_records)
    insert_data_to_db(traffic_source_data, "traffic_source",
                      ["source_id", "source_type", "source_detail"], conn)

    # 生成搜索关键词记录表数据
    search_keyword_log_data = generate_search_keyword_log_data(start_date, end_date, num_records)
    insert_data_to_db(search_keyword_log_data, "search_keyword_log",
                      ["keyword", "user_id", "product_id", "search_time", "session_id"], conn)

    # 生成价格力商品信息表数据
    price_force_product_data = generate_price_force_product_data(start_date, end_date, num_records)
    insert_data_to_db(price_force_product_data, "price_force_product",
                      ["product_id", "price_force_star", "coupon_price", "force_warning", "update_time"], conn)

    # 关闭数据库连接
    conn.close()


if __name__ == "__main__":
    main()
