#####many userid='None' or userid=''   around 1% 
#####1 checkout with multiple checkoutids   around 2%

import datetime
import csv
import sys
from collections import defaultdict
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext,SparkSession
import subprocess
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import json
import urllib2
import zipfile
import pandas
import re
import cStringIO
import codecs
import calendar
import collections
import pandas

reload(sys)
sys.setdefaultencoding('utf-8')



end_date = datetime.datetime.combine(datetime.datetime.now(), datetime.time(0))
end_time = int(time.mktime(end_date.timetuple()))
print end_date, end_time


print 'to connect spark'



spark=SparkSession \
    .builder \
    .enableHiveSupport() \
    .getOrCreate()
spark.sql('use shopee')

print 'connected'

if __name__ == '__main__':
	program_start=datetime.datetime.now()
	print 'in main loop'
	print('Query starting at %s...' % (datetime.datetime.now().strftime('%H:%M:%S')))
	#------------------ main code below ----------------------


	q1 = '''
	select 
		oi.userid, oi.orderid, checkoutid, oi.itemid, oi.item_price, oi.amount, from_unixtime(create_time) as time
	from
		shopee_db__order_item_tab oi
	left join
		shopee_order_details_db__order_details_tab or
	on
		oi.orderid = or.orderid
	where 
		from_unixtime(create_time,"yyyy-MM-dd") between "2017-08-01" and "2017-08-31"

	'''
	df1 = spark.sql(q1)
	df1.registerTempTable('table1')
	q2 = '''
	select 
		t1.userid, t1.orderid, checkoutid, t1.itemid, t1.item_price, amount, time, cat_group['main_cat'] as main_cat, cat_group['sub_cat'] as sub_cat, grass_region
	from
		table1 t1
	inner join
		item_profile ip
	on 
		t1.itemid = ip.itemid
	'''

	df2 = spark.sql(q2)
	df2.registerTempTable('table2')

	q21 = '''
	select
		distinct(*)
	from
		table2

	'''
	df21 = spark.sql(q21)
	df21.registerTempTable('table2')


	q3 = '''
	select  
		userid, orderid, checkoutid, itemid, item_price, amount, t2.main_cat, main_name, t2.sub_cat,sub_name,time, grass_region
	from
		table2 t2
	left join
		dim_category dc
	on
		t2.main_cat = dc.main_cat and t2.sub_cat = dc.sub_cat
	'''

	df3 = spark.sql(q3)
	df3.registerTempTable('table3')

	q31 = ''' 
	select distinct(*) from table3
	'''
	df31 = spark.sql(q31)
	df31.registerTempTable('table3')
	q32 = '''
	select 
		count(*) as count, userid
	from
		(select 
			distinct checkoutid, userid, time
		from table3)
	group by userid
	'''

	userlist = spark.sql(q32)
	repurchaseuser = userlist.filter('count > 1').registerTempTable('repurchaseuser')
	repurchaseuser = spark.sql("select distinct(userid) as userid, count from repurchaseuser").registerTempTable('repurchaseuser')

	q4 = '''
	select 
		t3.userid, orderid, checkoutid, ((item_price*amount)/100000) as gmv, main_name, sub_name, time, grass_region
	from
		table3 t3
	inner join
		repurchaseuser r
	on
		t3.userid = r.userid
	order by 
		userid, time
	'''

	df4 = spark.sql(q4)
	df4.registerTempTable("table4")


	### start!!!
	### part1 : get 1st-purchase user info
	q41 = '''
	select 
		userid, checkoutid, time, row_number() over(
		partition by userid order by time) as rank
	from
		(select 
			distinct checkoutid, userid, time
		from table4) tmp
	'''
	df41 = spark.sql(q41)
	df41.registerTempTable("ranktb")

	q42 = '''
	select 
		t4.userid, orderid, t4.checkoutid, gmv, main_name, sub_name, t4.time, grass_region, rank
	from
		table4 t4
	left join
		ranktb
	on
		t4.checkoutid = ranktb.checkoutid
	'''
	df42 = spark.sql(q42)
	df42.registerTempTable('table42')

	q5 = '''
	select
		userid, orderid, checkoutid, gmv, main_name, sub_name, time, grass_region, rank
	from
		table42 t4
	where
		rank = 1
	'''
	spark.sql(q5).registerTempTable("table5")


	### calculate sum of gmv across the region
	q51 = '''
	select 
		sum(gmv), grass_region
	from 
		table5
	group by
		grass_region

	'''

	df51 = spark.sql(q51)
	#df51.toPandas().to_csv("total_gmv_first_purchase.csv")

	q52 = '''
	select 
		count(orderid), grass_region
	from 
		table5
	group by
		grass_region

	'''

	df52 = spark.sql(q52)
	#df52.toPandas().to_csv("total_orders_first_purchase.csv")

	q53= '''
	select 
		count(distinct checkoutid), grass_region
	from 
		table5
	group by
		grass_region
	'''
	df53 = spark.sql(q53)
	#df53.toPandas().to_csv("total_num_customers_first_purchase.csv")


	q54 = '''
	select 
		main_name, sub_name, count(orderid) as numOfOrders, grass_region
	from 
		table5
	group by
		main_name, sub_name, grass_region
	order by
		grass_region, main_name, sub_name
	'''
	#spark.sql(q54).toPandas().to_csv("num_orders_main&sub_firstPurchase.csv")


	q55 = '''
	select 
		main_name, sub_name, count(distinct checkoutid) as numOfPurchse, grass_region
	from 
		table5
	group by
		main_name, sub_name, grass_region
	order by
		grass_region, main_name, sub_name

	'''
	#spark.sql(q55).toPandas().to_csv("num_purchase_main&sub_firstPurchase.csv")

	q56 = '''
	select 
		main_name, sub_name, sum(gmv) as profit, grass_region
	from 
		table5
	group by
		main_name, sub_name, grass_region
	order by
		grass_region, main_name, sub_name

	'''
	#spark.sql(q56).toPandas().to_csv("total_gmv_main&sub_firstPurchase.csv")


	q61 = '''
	select
		count(distinct userid), substring(time,0,10) as date, grass_region
	from
		table3
	group by
		substring(time, 0,10), grass_region
	order by
		grass_region, substring(time, 0,10)

	'''

	df61 = spark.sql(q61)
	#df61.toPandas().to_csv("unique_buyers_everyday.csv")

	q62 = '''
	select *,
		case
		when time between '2017-08-01' and '2017-08-06' then 1
		when time between '2017-08-07' and '2017-08-13' then 2
		when time between '2017-08-14' and '2017-08-20' then 3
		when time between '2017-08-21' and '2017-08-27' then 4
		when time between '2017-08-28' and '2017-08-31' then 5
		end week

	from table3

	'''
	df62 = spark.sql(q62)
	df62.registerTempTable('table6')

	q63 = '''
	select
		count(distinct userid) as count, week, grass_region
	from
		table6
	group by
		week, grass_region
	order by
		week
	'''

	df63 = spark.sql(q63)
	df63.toPandas().to_csv("unique_buyers_everyweek.csv")

	q64 = '''
	select
		count(distinct userid), grass_region
	from
		table6
	group by
		grass_region

	'''
	#spark.sql(q64).toPandas().to_csv("unique_buyers_overall.csv")


	#------------------- main code finished -------------------
	print('Query ended at %s. See you again!' % (datetime.datetime.now().strftime('%H:%M:%S')))  