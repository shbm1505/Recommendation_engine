package pl.japila.spark
import org.apache.spark.sql._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector.cql.CassandraConnectorConf.CassandraSSLConf
import java.io.FileInputStream
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{SSLContext, TrustManagerFactory}
import com.datastax.driver.core.QueryOptions._
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.util.{ConfigParameter, ReflectionUtil}
import com.datastax.spark.connector.rdd._
import org.apache.commons.configuration.ConfigurationException
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructType, StructField, StringType,IntegerType,DateType};
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, StringType,DoubleType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.GroupedData
import org.apache.spark.sql.functions.input_file_name 
import org.apache.spark.sql.types.DataTypes
import scala.util.Try
import scala.io._
import scala.util._ 
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp} 
import scala.collection.mutable.Queue
import scala.util.control.Breaks._
import org.apache.spark.sql.functions._
import java.util.Calendar



 object MysqlImport {
  def main(args: Array[String]) {

val hadoopConf = new Configuration()
val fs = FileSystem.get(hadoopConf)

var statement_flag=0;
var transaction_flag=0;

//var current_time="'"+java.time.LocalDate.now+"'"
val conf = new SparkConf(true)
      
val sc = new SparkContext(conf)

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
import sqlContext.implicits.StringToColumn
Class.forName("com.mysql.jdbc.Driver")

val merchant_schema = StructType(Array(StructField("merchant_name", StringType, true),StructField("cleaned_merchant_name", StringType, true)))

val merchant_name = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").schema(merchant_schema).load("hdfs://master:9000/merchant_mapping.csv")
val merchant_udf = udf((merchant_name: String,cleaned_merchant_name: String) => {  if (cleaned_merchant_name==null) merchant_name else cleaned_merchant_name })


val persona_schema = StructType(Array(StructField("customer_code", StringType, true),StructField("last_txn_date_cred", StringType, true),StructField("last_txn_date_deb", StringType, true),StructField("persona_name", StringType, true)))

val persona_name = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").schema(persona_schema).load("hdfs://master:9000/ucic_persona_data.csv")

  
val city_schema = StructType(Array(StructField("cleaned_city_name", StringType, true),StructField("city", StringType, true)))

val city_name = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").schema(city_schema).load("hdfs://master:9000/City_mapping_list.csv")

val city_udf = udf((city: String,cleaned_city_name: String) => {  if (cleaned_city_name==null) city else cleaned_city_name })

val merchant_category_schema = StructType(Array(StructField("merchant_type_desc", StringType, true),StructField("Mapped_Credit_merchant_cat", StringType, true)))

val merchant_category = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").schema(merchant_category_schema).load("hdfs://master:9000/Debit_merchan_cat_mapping.csv")

val verify_merchant = udf((merchant_name: String,cleaned_merchant_name: String) => {  if (cleaned_merchant_name==merchant_name) "0" else "1" })
val verify_city = udf((city: String,cleaned_city_name: String) => {  if (city==cleaned_city_name) "0" else "1" })



val changing_name = udf((_1: String)=> {
        _1
  })


val add_type = udf((card_type: String) => {
  "credit" 
})

val prop = new java.util.Properties
prop.setProperty("user","goals101")
prop.setProperty("password","goals123")

val url="jdbc:mysql://engage01.cfyrn3uyeiwt.us-west-2.rds.amazonaws.com:3306/engage_db"

//val customer_debit_schema = StructType(Array(StructField("cust_id", StringType, true),StructField("account_no", StringType, true),StructField("tier", StringType, true),StructField("gender", StringType, true),StructField("age", IntegerType, true),
 //                          StructField("add_on_card", StringType, true),StructField("city", StringType, true),StructField("mob_download", StringType, true),StructField("mob_active", StringType, true),StructField("employment_type", StringType, true),StructField("marital_status", StringType, true),StructField("customer_code", StringType, true)))

//var customer_debit_data2 = sqlContext.read.format("com.databricks.spark.csv").option("driver", "com.mysql.jdbc.Driver").option("header", "true").option("delimiter","|").schema(customer_debit_schema).load("hdfs://master:9000/goals101/debit_card_data/customer/files/*")
//var customer_debit_data=customer_debit_data2.select("customer_code","age","gender","city","employment_type","marital_status")
//customer_debit_data = city_name.as('a).join(customer_debit_data.as('b), $"a.city" === $"b.city","rightouter")
//customer_debit_data=customer_debit_data.select("customer_code","age","b.city","gender","cleaned_city_name","employment_type","marital_status")
//customer_debit_data.registerTempTable("customer_debit_data")
//customer_debit_data=sqlContext.sql("select distinct customer_code,age,city,gender,cleaned_city_name,employment_type,marital_status from customer_debit_data")
//customer_debit_data = persona_name.as('a).join(customer_debit_data.as('b), $"a.customer_code" === $"b.customer_code","rightouter")
//customer_debit_data=customer_debit_data.select("b.customer_code","age","city","gender","cleaned_city_name","persona_name","employment_type","marital_status")
//customer_debit_data.registerTempTable("customer_debit_data")
//customer_debit_data=sqlContext.sql("select distinct customer_code,age,city,gender,cleaned_city_name,persona_name,employment_type,marital_status from customer_debit_data")
//customer_debit_data=customer_debit_data.withColumn("city_name",city_udf(customer_debit_data("city"),customer_debit_data("cleaned_city_name")))
//customer_debit_data=customer_debit_data.withColumn("verify",verify_city(customer_debit_data("city"),customer_debit_data("cleaned_city_name")))

//customer_debit_data=customer_debit_data.withColumn("customer_id",lit(0).cast(IntegerType))
//customer_debit_data=customer_debit_data.withColumn("persona_id",lit(0).cast(IntegerType))
//customer_debit_data=customer_debit_data.withColumn("dob",lit(null: String).cast(StringType))
//customer_debit_data=customer_debit_data.withColumn("city_id",lit(0).cast(IntegerType)).withColumn("latitude",lit(null: String).cast(StringType))
//customer_debit_data=customer_debit_data.withColumn("longitude",lit(null: String).cast(StringType))
//customer_debit_data=customer_debit_data.withColumn("dob",lit(null: String).cast(StringType)).withColumn("dob",lit(null: String).cast(StringType))
//customer_debit_data=customer_debit_data.withColumn("count",lit(0).cast(IntegerType)).withColumn("customer_activity",lit("None").cast(StringType))
//customer_debit_data=customer_debit_data.withColumn("updated_on",lit(current_timestamp()).cast(DateType)).withColumn("created_on",lit(current_timestamp()).cast(DateType)).withColumn("is_processed",lit(0).cast(IntegerType))
//customer_debit_data=customer_debit_data.select("customer_id","customer_code","persona_name","persona_id","age","dob","gender","marital_status","employment_type","city_name","city_id","latitude","longitude","count","customer_activity","verify","is_processed","created_on","updated_on")
//customer_debit_data.write.mode("append").jdbc(url,"customers",prop)
//customer_debit_data.registerTempTable("customer_debit_data")
//customer_debit_data=sqlContext.sql("select count(*) from customer_debit_data")
//customer_debit_data.show()

val transaction_credit_schema = StructType(Array(StructField("account_number", StringType, true),StructField("merchant", StringType, true),StructField("date", DateType, true),StructField("amount", DoubleType, true),StructField("description", StringType, true),
                           StructField("mcc_code", StringType, true),StructField("nrr", StringType, true),StructField("merchant_category", StringType, true),StructField("txn_flag", StringType, true),StructField("tid", StringType, true),StructField("mid", StringType, true),StructField("card_flag", StringType, true)))

var transaction_credit_data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").schema(transaction_credit_schema).load("hdfs://master:9000/goals101/credit_card_data/transaction/files/*")
transaction_credit_data=transaction_credit_data.select("account_number","merchant","date","amount","merchant_category","tid")
transaction_credit_data= merchant_name.as('a).join(transaction_credit_data.as('b), $"a.merchant_name" === $"b.merchant","rightouter")
transaction_credit_data=transaction_credit_data.select("account_number","date","amount","b.merchant","merchant_category","cleaned_merchant_name","tid")
transaction_credit_data.registerTempTable("transaction_credit_data")
transaction_credit_data=transaction_credit_data.sqlContext.sql("select distinct account_number,date,amount,merchant,merchant_category,cleaned_merchant_name,tid from transaction_credit_data")
transaction_credit_data=transaction_credit_data.withColumn("cleaned_merchant_name",merchant_udf(transaction_credit_data("merchant"),transaction_credit_data("cleaned_merchant_name")))

var transaction_credit_data3=transaction_credit_data.withColumn("verify",verify_merchant(transaction_credit_data("merchant"),transaction_credit_data("cleaned_merchant_name")))
transaction_credit_data3=transaction_credit_data3.select("account_number","date","amount","merchant_category","cleaned_merchant_name","tid","verify")
transaction_credit_data3=transaction_credit_data3.withColumn("merchant",changing_name(transaction_credit_data("cleaned_merchant_name")))

transaction_credit_data3=transaction_credit_data3.withColumn("id",lit(0).cast(IntegerType)).withColumn("account_id",lit("").cast(StringType)).withColumn("tier",lit("").cast(StringType)).withColumn("merchant_id",lit(0).cast(IntegerType)).withColumn("week_num",lit(0).cast(IntegerType)).withColumn("month",lit(0).cast(IntegerType)).withColumn("year",lit(0).cast(IntegerType)).withColumn("category_id",lit("").cast(StringType)).withColumn("customer_code",lit("").cast(StringType)).withColumn("customer_id",lit(0).cast(IntegerType)).withColumn("type",lit("credit").cast(StringType)).withColumn("service",lit("").cast(StringType)).withColumn("gender",lit("").cast(StringType)).withColumn("age",lit(0).cast(IntegerType)).withColumn("maritial_status",lit("").cast(StringType)).withColumn("city_id",lit(0).cast(IntegerType)).withColumn("city",lit("").cast(StringType)).withColumn("persona_id",lit(0).cast(IntegerType)).withColumn("persona",lit("").cast(StringType)).withColumn("latitude",lit(0).cast(DoubleType)).withColumn("longitude",lit(0).cast(DoubleType)).withColumn("redemption_flag",lit(0).cast(IntegerType)).withColumn("campaign_id",lit(0).cast(IntegerType)).withColumn("is_processed",lit(0).cast(IntegerType))
transaction_credit_data3=transaction_credit_data3.select("id","account_number","account_id","tier","merchant","merchant_id","date","week_num","month","year","amount","merchant_category","category_id","customer_code","customer_id","tid","type","service","gender","age","maritial_status","city_id","city","persona_id","persona","latitude","longitude","verify","is_processed","redemption_flag","campaign_id")     

transaction_credit_data3.write.mode("append").jdbc(url,"transactions",prop)
transaction_credit_data3.registerTempTable("transaction_credit_data3")
transaction_credit_data3=sqlContext.sql("select count(*) from transaction_credit_data3")
transaction_credit_data3.show()
}}


