do_date=$1
for i in	order_info  user_info  order_detail  sku_info
do
	sqoop import --connect jdbc:mysql://cdh03:3306/gmall \
    --username root  \
	--password root \
	--delete-target-dir \
	-m 1 \
	--table $i \
	--target-dir /origin_data/gmall/$i/$do_date \
	-z \
	--compression-codec snappy \
	--null-string '\\N' \
	--null-non-string '\\N' \
	--fields-terminated-by '\t' 
done
