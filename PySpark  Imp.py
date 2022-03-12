from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf, SQLContext
import getpass
from datetime import datetime,timedelta
#pwd =getpass.getpass()
import os
from pyspark.sql import functions as F



##################################### Spark Setup ##########################
appName = "hadoop"
master = "local[*]"
conf = SparkConf() \
.setAppName(appName) \
.setMaster(master) \
.set("spark.driver.extraClassPath","/opt/mapr/spark/spark-2.3.2/jars")



##########################################################################

######################################### DB Setup ###########################



database = 'postilion_settlement'
#url = f"jdbc:sqlserver://172.19.20.48;databaseName={database}"
url = 'jdbc:sqlserver://172.19.20.48;databaseName={}'.format(database)
user = 'ssrs_user'
password = 'Password12'



#############################################################################

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession



df = spark.read.parquet\
('maprfs:///iswdata/storage/products/rj/superswitch/report_results_rj/2022/01/*/*.parquet')


df.createOrReplaceTempView('rj_report')



b = spark.sql("""Select CONCAT_WS('_',ptc_pan,Ptc_terminal_id,pt_retrieval_reference_nr,Pt_system_trace_audit_nr,PTC_totals_group) as Unique_ID,Unique_Key,
CAST(sum(final_fee) AS DECIMAL(20,2)) as Amount, Trxn_Category
from rj_report
where trxn_category NOT LIKE  'UNK%'
AND NOT (trxn_category like 'POS%' and trxn_category not like '%transfer%'and ((debit_account_type like '%amount%' or credit_account_type like '%amount%') and late_reversal = 0))
AND NOT (trxn_category in ('FEES COLLECTED FOR ALL PTSPs','Fees collected for all Terminal_owners'))
AND NOT (trxn_category LIKE 'BILLPAYMENT%' and (debit_account_type like '%amount%' or credit_account_type like '%amount%'))
AND NOT (trxn_category LIKE 'PREPAID MERCHANDISE%')
AND  (currency = '566' or (currency = '840' and 
(trxn_category in ('QUICKTELLER TRANSFERS(SVA)','WESTERN UNION MONEY TRANSFERS','BILLPAYMENT MASTERCARD BILLING','ATM WITHDRAWAL (MASTERCARD ISO)','MASTERCARD LOCAL PROCESSING BILLING(ATM WITHDRAWAL)') 
or trxn_category like '%MASTERCARD LOCAL PROCESSING BILLING%' OR trxn_category like '%VISA ACQUIRING BILLING%'
OR trxn_category like  'switched%' or trxn_category like  '%voice%' or trxn_category like '%purchase%billing%'))) AND trxn_category <> 'DEPOSIT'
and NOT (trxn_category like 'POS%') and PT_message_type ='0420'
group by unique_key,PTC_terminal_id,ptc_pan,pt_retrieval_reference_nr,Pt_system_trace_audit_nr,trxn_category,Rate,PTC_totals_group
having CAST(sum(final_fee) AS DECIMAL(20,2)) <> 0 AND settlement_date >= '2022-01-01' AND settlement_date < '2022-01-22' """)




try:
#start_time = datetime.now()



b.write.format("jdbc") \
.mode("overwrite") \
.option("url", url) \
.option("dbtable", "dbo.rj_report_GST59851_20220127") \
.option("user", user) \
.option("password", password) \
.save()



#end_time = datetime.now()
#print(f'Time elaspsed (hh:mm:ss:ms): {end_time - start_time}')



except Exception as e:
print(e)