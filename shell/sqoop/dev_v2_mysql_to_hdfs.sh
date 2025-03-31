do_date=$1

for i in order order_detail product refund user user_action_log
do
	sqoop import --connect jdbc:mysql://cdh03:3306/dev_offline_ecommerce_v2 \
    	--username root  \
	--password root \
	--delete-target-dir \
	-m 1 \
	--table $i \
	--target-dir /origin_data/dev_offline_ecommerce_v2/$i/$do_date \
	-z \
	--as-parquetfile  \
	--compression-codec lzop \
	--null-string '\\N' \
	--null-non-string '\\N' \
	--fields-terminated-by '\t'
done
