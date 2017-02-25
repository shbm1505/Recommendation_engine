package pl.japila.spark
import org.apache.spark.sql._
import com.datastax.spark.connector._
import com.datastax.driver.core._
import com.datastax.spark.connector.cql._
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector.cql.CassandraConnectorConf.CassandraSSLConf
import java.io.FileInputStream
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{SSLContext, TrustManagerFactory}
import com.datastax.driver.core.QueryOptions._
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import com.datastax.driver.core._
import com.datastax.spark.connector._
import com.datastax.spark.connector.util.{ConfigParameter, ReflectionUtil}
import com.datastax.spark.connector.rdd._
import org.apache.commons.configuration.ConfigurationException
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructType, StructField, StringType,IntegerType,DateType};
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.input_file_name 
import scala.util.Try

import scala.io._
import scala.util._      


 object CassandraImport {
  def main(args: Array[String]) {

val hadoopConf = new Configuration()
val fs = FileSystem.get(hadoopConf)

val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://master:9000"), hadoopConf)
val add_type = udf((card_type: String) => {
  "credit" 
})
val changing_name = udf((card_type: String) => {
  card_type 
})
val add_type_debit = udf((card_type: String) => {
  "debit" 
})
val add_type_savings = udf((card_type: String) => {
  "savings" 
})
val merchant_udf = udf((merchant_name: String,cleaned_merchant_name: String) => {  if (cleaned_merchant_name==null) merchant_name else cleaned_merchant_name })
val city_udf = udf((city: String,cleaned_city_name: String) => {  if (cleaned_city_name==null) city else cleaned_city_name })
val verify_city = udf((city: String,cleaned_city_name: String) => {  if (city==cleaned_city_name) "0" else "1" })
val verify_merchant = udf((merchant_name: String,cleaned_merchant_name: String) => {  if (cleaned_merchant_name==merchant_name) "0" else "1" })
var flag=0;
val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
val sc = new SparkContext(conf)
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

val persona_schema = StructType(Array(StructField("ucic", StringType, true),StructField("last_txn_date_cred", StringType, true),StructField("last_txn_date_debit", StringType, true),StructField("persona", StringType, true)))
val persona_name = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").schema(persona_schema).load("hdfs://master:9000/ucic_persona_data.csv")
val city_schema = StructType(Array(StructField("cleaned_city_name", StringType, true),StructField("city", StringType, true)))
val city_name = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").schema(city_schema).load("hdfs://master:9000/City_mapping_list.csv")
val merchant_schema = StructType(Array(StructField("merchant_name", StringType, true),StructField("cleaned_merchant_name", StringType, true)))
val merchant_category_schema = StructType(Array(StructField("merchant_type_desc", StringType, true),StructField("Mapped_Credit_merchant_cat", StringType, true)))
val merchant_category = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").schema(merchant_category_schema).load("hdfs://master:9000/Debit_merchan_cat_mapping.csv")
val merchant_name = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").schema(merchant_schema).load("hdfs://master:9000/merchant_mapping.csv")
val customer_credit_schema = StructType(Array(StructField("customer_code", StringType, true),StructField("account_no", StringType, true),StructField("card_type", StringType, true),StructField("gender", StringType, true),StructField("age", StringType, true),
                           StructField("add_on_card", StringType, true),StructField("city", StringType, true),StructField("ucic", StringType, true)))
val customer_debit_schema = StructType(Array(StructField("cust_id", StringType, true),StructField("account_no", StringType, true),StructField("card_type", StringType, true),StructField("gender", StringType, true),StructField("age", StringType, true),
                           StructField("add_on_card", StringType, true),StructField("city", StringType, true),StructField("mob_download", StringType, true),StructField("mob_active", StringType, true),StructField("employment_status", StringType, true),StructField("marital_status", StringType, true),StructField("ucic", StringType, true)))

val customer_credit_data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").schema(customer_credit_schema).load("hdfs://master:9000/goals101/credit_card_data/customer/files/*")
val customer_debit_data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").schema(customer_debit_schema).load("hdfs://master:9000/goals101/debit_card_data/customer/files/*")
val customer_names = customer_credit_data.select(input_file_name())
val customer_names_debit = customer_debit_data.select(input_file_name())
var customer_names2=customer_names.dropDuplicates()
var customer_names2_debit=customer_names_debit.dropDuplicates()
val statement_credit_schema2 = StructType(Array(StructField("customer_code", StringType, true),StructField("accountno", StringType, true),StructField("month", StringType, true),StructField("tad", StringType, true),StructField("late_fee", StringType, true),
                           StructField("ovl_fee", StringType, true),StructField("othr_fee", StringType, true),StructField("mem_fee", StringType, true),StructField("fee_type", StringType, true),StructField("service_tax", StringType, true),StructField("interest", StringType, true),StructField("bc1", StringType, true),StructField("bc2", StringType, true),StructField("crlimit", StringType, true)
                           ,StructField("cash_limit", StringType, true),StructField("stm_date", StringType, true),StructField("pmt_due_date", StringType, true),StructField("unbilled_principal", StringType, true)))
val statement_credit_data2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").schema(statement_credit_schema2).load("hdfs://master:9000/goals101/credit_card_data/statement/files/*")
var customer_count=customer_names2.count()
var customer_count_debit=customer_names2_debit.count()

val a = 0;

  for( a <- 0 to (customer_count.toInt-1) ){

var customer_file=customer_names2.collect()(a).getString(0)       
var start_index=customer_file.indexOf("files/")+6
var end_index=customer_file.length() - 4
var file_name=customer_file.substring(start_index,end_index+4)
var file_names=customer_file.substring(start_index,end_index)
val customer_single_file = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").schema(customer_credit_schema).load("hdfs://master:9000/goals101/credit_card_data/customer/files/"+file_name)         
val customer_credit_final=customer_single_file.select("ucic","age","city","gender")
val customer_credit_final2 = city_name.as('a).join(customer_credit_final.as('b), $"a.city" === $"b.city","rightouter")
val customer_credit_final3=customer_credit_final2.select("ucic","age","b.city","gender","cleaned_city_name")
val customer_credit_final4=customer_credit_final3.withColumn("cleaned_city_name",city_udf(customer_credit_final3("city"),customer_credit_final3("cleaned_city_name")))
var customer_credit_final5=customer_credit_final4.withColumn("verify",verify_city(customer_credit_final4("city"),customer_credit_final4("cleaned_city_name")))
var customer_credit_final6 = persona_name.as('a).join(customer_credit_final5.as('b), $"a.ucic" === $"b.ucic","rightouter")
customer_credit_final6=customer_credit_final6.select("a.ucic","age","city","cleaned_city_name","gender","last_txn_date_cred","last_txn_date_debit","persona","verify")
customer_credit_final6.registerTempTable("customer_credit_final6")
customer_credit_final6=sqlContext.sql("select * from customer_credit_final6 where ucic != ''")
var customer_credit_final7=sqlContext.sql("select * from customer_credit_final6 where ucic =''")
val account_credit_final=customer_single_file.select("ucic","account_no","card_type","add_on_card")
val account_credit_final2=account_credit_final.withColumn("type",add_type(account_credit_final("card_type")))
var account_credit_final3=account_credit_final2.select("ucic","account_no","card_type","add_on_card","type")
var account_credit_final4 = account_credit_final3.as('a).join(statement_credit_data2.as('b), $"a.account_no" === $"b.accountno","leftouter")
account_credit_final4=account_credit_final4.select("ucic","account_no","bc1","bc2","card_type","add_on_card","type")
account_credit_final4.registerTempTable("account_credit_final3")
account_credit_final4=sqlContext.sql("select * from account_credit_final3 where ucic != '' or account_no !=''")
var account_credit_final5=sqlContext.sql("select * from account_credit_final3 where ucic = '' or account_no =''")

try{

customer_credit_final6.write .format("org.apache.spark.sql.cassandra")  //writing data to casandra customer table
 .options(Map( "table" -> "customer", "keyspace" -> "goals101"))
 .mode(SaveMode.Append).save()

 try{

account_credit_final3.write.format("org.apache.spark.sql.cassandra")     //writing data to cassandra account table
 .options(Map( "table" -> "account", "keyspace" -> "goals101"))
 .mode(SaveMode.Append).save()

 } catch {
         case x:Exception=>{

}}

 } catch {

         case x:Exception=>{
}}
 }
for( a <- 0 to (customer_count_debit.toInt-1) ){

var customer_file=customer_names2_debit.collect()(a).getString(0)
var start_index=customer_file.indexOf("files/")+6
var end_index=customer_file.length() - 4
var file_name=customer_file.substring(start_index,end_index+4)
var file_names=customer_file.substring(start_index,end_index)
val customer_single_file = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").schema(customer_debit_schema).load("hdfs://master:9000/goals101/debit_card_data/customer/files/"+file_name)
val customer_credit_final=customer_single_file.select("ucic","age","city","gender","employment_status","marital_status")
val customer_credit_final2 = city_name.as('a).join(customer_credit_final.as('b), $"a.city" === $"b.city","rightouter")
val customer_credit_final3=customer_credit_final2.select("ucic","age","b.city","gender","cleaned_city_name","employment_status","marital_status")
val customer_credit_final4=customer_credit_final3.withColumn("cleaned_city_name",city_udf(customer_credit_final3("city"),customer_credit_final3("cleaned_city_name")))
var customer_credit_final5=customer_credit_final4.withColumn("verify",verify_city(customer_credit_final4("city"),customer_credit_final4("cleaned_city_name")))
customer_credit_final5=customer_credit_final5.as('a).join(persona_name.as('b),$"a.ucic" === $"b.ucic","leftouter")
customer_credit_final5=customer_credit_final5.select("a.ucic","age","city","cleaned_city_name","gender","last_txn_date_cred","last_txn_date_debit","persona","verify","employment_status")
customer_credit_final5.registerTempTable("customer_credit_final5")
var customer_credit_final6=sqlContext.sql("select * from customer_credit_final5 where ucic != ''")
var customer_credit_final7=sqlContext.sql("select * from customer_credit_final5 where ucic =''")
val account_credit_final=customer_single_file.select("ucic","account_no","card_type","add_on_card")
val account_credit_final2=account_credit_final.withColumn("type",add_type_debit(account_credit_final("card_type")))
val account_credit_final3=account_credit_final2.select("ucic","account_no","card_type","add_on_card","type")
account_credit_final3.registerTempTable("account_credit_final3")
var account_credit_final4=sqlContext.sql("select * from account_credit_final3 where ucic != '' or account_no !=''")
var account_credit_final5=sqlContext.sql("select * from account_credit_final3 where ucic = '' or account_no =''")

try{

customer_credit_final6.write .format("org.apache.spark.sql.cassandra")  //writing data to customer table
 .options(Map( "table" -> "customer", "keyspace" -> "goals101"))
 .mode(SaveMode.Append).save()

 try{

account_credit_final4.write.format("org.apache.spark.sql.cassandra")    //writing data to account table
 .options(Map( "table" -> "account", "keyspace" -> "goals101"))
 .mode(SaveMode.Append).save()

 } catch {
         case x:Exception=>{

}}

 } catch {

         case x:Exception=>{
}}

}

val transaction_credit_schema = StructType(Array(StructField("account_no", StringType, true),StructField("merchant_name", StringType, true),StructField("date_of_txn", StringType, true),StructField("amount", StringType, true),StructField("description", StringType, true),
                           StructField("mcc_code", StringType, true),StructField("nrr", StringType, true),StructField("cleaned_merchant_category", StringType, true),StructField("txn_flag", StringType, true),StructField("tid", StringType, true),StructField("mid", StringType, true),StructField("card_flag", StringType, true)))

val transaction_credit_data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").schema(transaction_credit_schema).load("hdfs://master:9000/goals101/credit_card_data/transaction/files/*")
val transaction_names = transaction_credit_data.select(input_file_name())
var transaction_names2=transaction_names.dropDuplicates()
var transaction_count=transaction_names2.count()

var b = 0;
for( b <- 0 to (transaction_count.toInt-1) ){

var transaction_file=transaction_names2.collect()(b).getString(0)
var start_index=transaction_file.indexOf("files/")+6
var end_index=transaction_file.length()
var file_name=transaction_file.substring(start_index,end_index)
var file_names=transaction_file.substring(start_index,end_index-4)
val transaction_single_file = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").schema(transaction_credit_schema).load("hdfs://master:9000/goals101/credit_card_data/transaction/files/"+file_name)
val transaction_credit_final=transaction_single_file.select("account_no","merchant_name","date_of_txn","amount","cleaned_merchant_category","txn_flag")
val transaction_credit_final2 = merchant_name.as('a).join(transaction_credit_final.as('b), $"a.merchant_name" === $"b.merchant_name","rightouter")
val transaction_credit_final3=transaction_credit_final2.select("account_no","b.merchant_name","date_of_txn","amount","cleaned_merchant_category","cleaned_merchant_name","txn_flag")
var transaction_credit_final4=transaction_credit_final3.withColumn("cleaned_merchant_name",merchant_udf(transaction_credit_final3("merchant_name"),transaction_credit_final3("cleaned_merchant_name")))

var transaction_credit_final5=transaction_credit_final4.withColumn("verify",verify_merchant(transaction_credit_final4("merchant_name"),transaction_credit_final4("cleaned_merchant_name")))
transaction_credit_final5=transaction_credit_final5.withColumn("type",add_type(transaction_credit_final5("account_no")))
transaction_credit_final5=transaction_credit_final5.select("account_no","amount","type","cleaned_merchant_name","date_of_txn","cleaned_merchant_category","merchant_name","verify","txn_flag")
transaction_credit_final5.registerTempTable("transaction_credit_final5")
var transaction_credit_final6=sqlContext.sql("select * from transaction_credit_final5 where account_no !=''")
var transaction_credit_final7=sqlContext.sql("select * from transaction_credit_final5 where account_no =''")


try{

transaction_credit_final6.write .format("org.apache.spark.sql.cassandra")  //writing data to transaction table
 .options(Map( "table" -> "transaction", "keyspace" -> "goals101"))
 .mode(SaveMode.Append).save()

 } catch {

         case x:Exception=>{

}}
}

val transaction_debit_schema = StructType(Array(StructField("cust_id", StringType, true),StructField("merchant_name", StringType, true),StructField("date_of_txn", StringType, true),StructField("amount", StringType, true),StructField("tran_type_desc", StringType, true),
                           StructField("merchant_type_code", StringType, true),StructField("merchant_type_desc", StringType, true),StructField("txn_type", StringType, true),StructField("txn_on", StringType, true),StructField("account_no", StringType, true),StructField("merchant_type", StringType, true)))
val transaction_debit_data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").schema(transaction_debit_schema).load("hdfs://master:9000/goals101/debit_card_data/transaction/files/*")
val transaction_names_debit = transaction_debit_data.select(input_file_name())
var transaction_names2_debit=transaction_names_debit.dropDuplicates()
var transaction_count_debit=transaction_names2_debit.count()

b = 0;
for( b <- 0 to (transaction_count_debit.toInt-1) ){

var transaction_file=transaction_names2_debit.collect()(b).getString(0)
var start_index=transaction_file.indexOf("files/")+6
var end_index=transaction_file.length()
var file_name=transaction_file.substring(start_index,end_index)
var file_names=transaction_file.substring(start_index,end_index-4)
val transaction_single_file = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").schema(transaction_debit_schema).load("hdfs://master:9000/goals101/debit_card_data/transaction/files/"+file_name)
val transaction_credit_final=transaction_single_file.select("account_no","merchant_name","date_of_txn","amount","merchant_type_desc")
var transaction_credit_final2 = merchant_name.as('a).join(transaction_credit_final.as('c), $"a.merchant_name" === $"c.merchant_name","rightouter")
transaction_credit_final2=transaction_credit_final2.select("account_no","c.merchant_name","date_of_txn","amount","merchant_type_desc","cleaned_merchant_name")
transaction_credit_final2.show()
transaction_credit_final2 = merchant_category.as('a).join(transaction_credit_final2.as('b), $"a.merchant_type_desc" === $"b.merchant_type_desc","rightouter")
val transaction_credit_final3=transaction_credit_final2.select("account_no","merchant_name","date_of_txn","amount","b.merchant_type_desc","Mapped_Credit_merchant_cat","cleaned_merchant_name")
var transaction_credit_final4=transaction_credit_final3.withColumn("cleaned_merchant_name",merchant_udf(transaction_credit_final3("merchant_name"),transaction_credit_final3("cleaned_merchant_name")))
transaction_credit_final4=transaction_credit_final4.withColumn("cleaned_merchant_category",merchant_udf(transaction_credit_final4("merchant_type_desc"),transaction_credit_final3("Mapped_Credit_merchant_cat")))
var transaction_credit_final5=transaction_credit_final4.withColumn("verify",verify_merchant(transaction_credit_final4("merchant_name"),transaction_credit_final4("cleaned_merchant_name")))
transaction_credit_final5=transaction_credit_final5.withColumn("type",add_type_debit(transaction_credit_final5("account_no"))).withColumn("merchant_category",changing_name(transaction_credit_final5("merchant_type_desc")))
transaction_credit_final5=transaction_credit_final5.select("account_no","amount","type","cleaned_merchant_name","date_of_txn","merchant_category","merchant_name","cleaned_merchant_category","verify")
transaction_credit_final5.registerTempTable("transaction_credit_final5")
var transaction_credit_final6=sqlContext.sql("select * from transaction_credit_final5 where account_no !=''")
var transaction_credit_final7=sqlContext.sql("select * from transaction_credit_final5 where account_no =''")


try{

transaction_credit_final6.write .format("org.apache.spark.sql.cassandra")  //writing transactions to transaction table
 .options(Map( "table" -> "transaction", "keyspace" -> "goals101"))
 .mode(SaveMode.Append).save()

 } catch {

         case x:Exception=>{

}}
}

val statement_credit_schema = StructType(Array(StructField("customer_code", StringType, true),StructField("accountno", StringType, true),StructField("month", StringType, true),StructField("tad", StringType, true),StructField("late_fee", StringType, true),
                           StructField("ovl_fee", StringType, true),StructField("othr_fee", StringType, true),StructField("mem_fee", StringType, true),StructField("fee_type", StringType, true),StructField("service_tax", StringType, true),StructField("interest", StringType, true),StructField("bc1", StringType, true),StructField("bc2", StringType, true),StructField("crlimit", StringType, true)
                           ,StructField("cash_limit", StringType, true),StructField("stm_date", StringType, true),StructField("pmt_due_date", StringType, true),StructField("unbilled_principal", StringType, true)))

val statement_credit_data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").schema(statement_credit_schema).load("hdfs://master:9000/goals101/credit_card_data/statement/files/*")
val statement_names = statement_credit_data.select(input_file_name())
var statement_names2=statement_names.dropDuplicates()
var statement_count=statement_names2.count()
var c=0;

for( c <- 0 to (statement_count.toInt-1) ){

var statement_file=statement_names2.collect()(c).getString(0)
var start_index=statement_file.indexOf("files/")+6
var end_index=statement_file.length()
var file_name=statement_file.substring(start_index,end_index)
var file_names=statement_file.substring(start_index,end_index-4)
val statement_single_file = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter","|").schema(statement_credit_schema).load("hdfs://master:9000/goals101/credit_card_data/statement/files/"+file_name) 
val statement_credit_final=statement_single_file.select("customer_code","accountno","month","tad","late_fee","ovl_fee","othr_fee","mem_fee","fee_type","service_tax","interest","bc1","bc2","crlimit","cash_limit","stm_date","pmt_due_date","unbilled_principal")
statement_credit_final.registerTempTable("statement_credit_final")
var statement_credit_final2=sqlContext.sql("select * from statement_credit_final where accountno !=''")
var statement_credit_final3=sqlContext.sql("select * from statement_credit_final where accountno =''")

try{
statement_credit_final2.write .format("org.apache.spark.sql.cassandra")   //writing credit statement data to creditstatement table
 .options(Map( "table" -> "creditstatement", "keyspace" -> "goals101"))
 .mode(SaveMode.Append).save()    
 } catch {

         case x:Exception=>{

}}
}

val statement_debit_data = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferschema","true").option("delimiter","|").load("hdfs://master:9000/goals101/debit_card_data/statement/files/*")
val statement_names_debit = statement_debit_data.select(input_file_name())
var statement_names2_debit=statement_names_debit.dropDuplicates()
var statement_count_debit=statement_names2_debit.count()

c=0;
for( c <- 0 to (statement_count_debit.toInt-1) ){

var statement_file=statement_names2_debit.collect()(c).getString(0)
var start_index=statement_file.indexOf("files/")+6
var end_index=statement_file.length()
var file_name=statement_file.substring(start_index,end_index)
var file_names=statement_file.substring(start_index,end_index-4)
val statement_credit_final = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferschema","true").option("delimiter","|").load("hdfs://master:9000/goals101/debit_card_data/statement/files/"+file_name)        
statement_credit_final.registerTempTable("statement_credit_final")
var statement_credit_final2=sqlContext.sql("select * from statement_credit_final where account_no !=''")
var statement_credit_final3=sqlContext.sql("select * from statement_credit_final where account_no =''")
try{
statement_credit_final2.write .format("org.apache.spark.sql.cassandra")     //writing debit statement data to debit statement table
 .options(Map( "table" -> "debitstatement", "keyspace" -> "goals101"))
 .mode(SaveMode.Append).save()    
 } catch {

         case x:Exception=>{

}}
}

val savings_transaction_schema = StructType(Array(StructField("customer_code", StringType, true),StructField("account_no", StringType, true),StructField("date_of_txn", StringType, true),StructField("tran_particular", StringType, true),StructField("amount", StringType, true)))
val savings_transaction_data = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("delimiter","|").schema(savings_transaction_schema).load("hdfs://master:9000/goals101/savings_account/transaction/files/*")
val savings_names = savings_transaction_data.select(input_file_name())
var savings_names2=savings_names.dropDuplicates()
var savings_count=savings_names2.count()
 c=0;

for( c <- 0 to (savings_count.toInt-1) ){

 var statement_file=savings_names2.collect()(c).getString(0)
 var start_index=statement_file.indexOf("files/")+6
 var end_index=statement_file.length()
 var file_name=statement_file.substring(start_index,end_index)
 var file_names=statement_file.substring(start_index,end_index-4)
 var statement_credit_final = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("delimiter","|").schema(savings_transaction_schema).load("hdfs://master:9000/goals101/savings_account/transaction/files/"+file_name)
 statement_credit_final=statement_credit_final.select("account_no","date_of_txn","amount","tran_particular")
 statement_credit_final=statement_credit_final.withColumn("type",add_type_savings(statement_credit_final("amount")))
 statement_credit_final.registerTempTable("statement_credit_final")
 var statement_credit_final2=sqlContext.sql("select * from statement_credit_final where account_no !=''")
 var statement_credit_final3=sqlContext.sql("select * from statement_credit_final where account_no =''")

try{
statement_credit_final2.write .format("org.apache.spark.sql.cassandra")      
 .options(Map( "table" -> "transaction", "keyspace" -> "goals101"))
 .mode(SaveMode.Append).save()
 } catch {
         case x:Exception=>{

}}
}
}}







