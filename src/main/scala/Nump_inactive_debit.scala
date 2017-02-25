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
import org.apache.spark.sql.types.{StructType, StructField, StringType,IntegerType};
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
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql
import java.lang.Object
import org.apache.spark.sql.functions._
import scala.io._
import scala.util._ 
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp} 
import scala.collection.mutable.Queue
import scala.util.control.Breaks._

 object Nump_inactive_debit {
  def main(args: Array[String]) {

val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")

val sc = new SparkContext(conf)

Class.forName("com.mysql.jdbc.Driver")

val prop = new java.util.Properties
prop.setProperty("user","goals101")
prop.setProperty("password","goals123")

var bank_name="RBL Bank"
val url="jdbc:mysql://engage01.cfyrn3uyeiwt.us-west-2.rds.amazonaws.com:3306/engage_db?zeroDateTimeBehavior=convertToNull"

val ran = scala.util.Random

//var current_time=java.time.LocalDate.now
//var ct=current_time.toString()
/*var year=ct.substring(0,4)
var month=ct.substring(5,7)
var day=ct.substring(8,10)
*/
import java.util.Calendar
import java.text.SimpleDateFormat
val format = new SimpleDateFormat("d-M-y")
var ct2=format.format(Calendar.getInstance().getTime())
var year2=ct2.substring(4,8)
var month2=ct2.substring(2,3)
var day2=ct2.substring(0,1)
var ct=year2+"-"+month2+"-"+day2


val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

val time_simulator = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "time_simulator", "keyspace" -> "goals101"))
  .load()
var month=time_simulator.collect()(0).getString(2)
var year=time_simulator.collect()(0).getString(0)
var day=time_simulator.collect()(0).getString(1)
ct=year+"-"+month+"-"+day
val calculating_date = udf((week_no: String,day_of_the_week: String,time: String) => { 
var t=time.substring(10,19)
year+"-"+month+"-"+day+" "+t
 })

val first_date = udf((offer_name: String) => {
  month+"/"+day+"/"+year 
})

val changing_name = udf((_1: String)=> {
        _1
  })

var offer_data = sqlContext.read.jdbc(url,"offer_data",prop)


offer_data=offer_data.withColumn("delivery_date",calculating_date(offer_data("week_no"),offer_data("day_of_the_week"),offer_data("time")))
offer_data.registerTempTable("offer")

var customer = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "customer", "keyspace" -> "goals101"))
  .load() 

customer.registerTempTable("customer")
customer=sqlContext.sql("select * from customer where ucic in ('RBL000002483','RBL000002555','RBL000002556','RBL000002557','RBL000002562','RBL000002563','RBL000002564','RBL000002566','RBL000002573','RBL000002578','RBL000002579','RBL000002580','RBL000002581','RBL000002583','RBL000002584','RBL000002586','RBL000002588','RBL000002589','RBL000002590','RBL000002593','RBL000002595','RBL000002596','RBL000002597','RBL000002598','RBL000002601','RBL000002605','RBL000002607','RBL000002608','RBL000002609','RBL000002615','RBL000002616','RBL000002618','RBL000002619','RBL000002622','RBL000002623','RBL000002625','RBL000002628','RBL000002633','RBL000002634','RBL000002635','RBL000002636','RBL000002637','RBL000002638','RBL000002639','RBL000002640','RBL000002641','RBL000002642','RBL000002644','RBL000002645','RBL000002647','RBL000002648','RBL000002649','RBL000002652','RBL000002653','RBL000002654','RBL000002655','RBL000002656','RBL000002658','RBL000002659','RBL000002661','RBL000002665','RBL000000148','RBL000000265','RBL000000309','RBL000000620','RBL000001474','RBL000001626','RBL000001673','RBL000001693','RBL000001700','RBL000001724','RBL000001736','RBL000001744','RBL000001934','RBL000001941','RBL000001954','RBL000001963','RBL000001978','RBL000001981','RBL000002017','RBL000002029','RBL000002067','RBL000002073','RBL000002100','RBL000002101','RBL000002129','RBL000002137','RBL000002239','RBL000002240','RBL000002247','RBL000002260','RBL000002331','RBL000002363','RBL000002371','RBL000002426','RBL000002440')")
var customer_inactive=customer.filter($"last_txn_date_debit"!=="NA")
var customer_nump=customer.filter($"last_txn_date_debit"==="NA")
customer_inactive.registerTempTable("customer_inactive")
customer_nump.registerTempTable("customer_nump")
customer_inactive=sqlContext.sql("select ucic from customer_inactive where months_between(current_date(),last_txn_date_debit)>3") //filtering all inactive debit users 
var customer_nump_count=customer_nump.count()
var customer_inactive_count=customer_inactive.count()
val a = 0;

for(a <- 0 to (customer_nump_count.toInt-1)) {

var ucic=customer_nump.collect()(a).getString(0)


var debit_nump_inactive = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "debit_nump_inactive", "keyspace" -> "goals101"))
  .load()

var debit_trending_offer=sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='Bank offer wall'")
var debit_nump_spend=sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='Spend offer wall'")
var credit_nump_tax_saving=sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='NUNP Tax saving'")

//cash_loan_offer.show()
/*ar offer_id=cash_loan_offer.collect()(0).getInt(0)
 var campaign_id=cash_loan_offer.collect()(0).getInt(1)
 
 var delivery_date=cash_loan_offer.collect()(0).getString(3)*/

debit_nump_inactive.registerTempTable("debit_nump_inactive")

if (sqlContext.sql("select * from debit_nump_inactive where ucic='"+ucic+"'").count()==0)
 {
var offer_id=debit_trending_offer.collect()(0).getInt(0)
var campaign_id=debit_trending_offer.collect()(0).getInt(1)
var delivery_date=debit_trending_offer.collect()(0).getString(3)
var SMS_text_1=debit_trending_offer.collect()(0).getString(4)
SMS_text_1=SMS_text_1.replaceAll("<bn>",bank_name)
    
    
    var collection4 = sc.parallelize(Seq((0,ucic,campaign_id,delivery_date,offer_id,"[]",SMS_text_1,0,ct))).toDF()
    collection4=collection4.withColumn("msg_system_id",changing_name(collection4("_1")))
    collection4=collection4.withColumn("customer_code",changing_name(collection4("_2")))
    collection4=collection4.withColumn("campaign_id",changing_name(collection4("_3")))
    collection4=collection4.withColumn("delivery_date",changing_name(collection4("_4")))
    collection4=collection4.withColumn("offer_id",changing_name(collection4("_5")))
    collection4=collection4.withColumn("secondary_offers",changing_name(collection4("_6")))
    collection4=collection4.withColumn("message_text",changing_name(collection4("_7")))
    collection4=collection4.withColumn("status",changing_name(collection4("_8")))
    collection4=collection4.withColumn("created_on",changing_name(collection4("_9")))
    collection4=collection4.select("msg_system_id","customer_code","campaign_id","delivery_date","offer_id","secondary_offers","message_text","status","created_on")
    collection4.show()
    collection4.write.mode("append").jdbc(url,"message_to_be_sent_system",prop)

var collection3 = sc.parallelize(Seq((ucic,"1"))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("nump_cond",changing_name(collection3("_2")))
    collection3=collection3.select("ucic","nump_cond")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "debit_nump_inactive", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
  }
else{    
val debit_nump_inactive2 = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "debit_nump_inactive", "keyspace" -> "goals101"))
  .load()
  val debit_nump_inactive_count=debit_nump_inactive2.count()
  for( r <- 0 to (debit_nump_inactive_count.toInt-1) ){
  if(ucic==debit_nump_inactive2.collect()(r).getString(0))
{  

  var xx=debit_nump_inactive2.collect()(r).getString(2)
 if(xx=="1")
  {
    var offer_id=debit_nump_spend.collect()(0).getInt(0)
var campaign_id=debit_nump_spend.collect()(0).getInt(1)
var delivery_date=debit_nump_spend.collect()(0).getString(3)

    var SMS_text_2=debit_nump_spend.collect()(0).getString(5)
    SMS_text_2=SMS_text_2.replaceAll("<bn>",bank_name)
    var collection4 = sc.parallelize(Seq((0,ucic,campaign_id,delivery_date,offer_id,"[]",SMS_text_2,0,ct))).toDF()
    collection4=collection4.withColumn("msg_system_id",changing_name(collection4("_1")))
    collection4=collection4.withColumn("customer_code",changing_name(collection4("_2")))
    collection4=collection4.withColumn("campaign_id",changing_name(collection4("_3")))
    collection4=collection4.withColumn("delivery_date",changing_name(collection4("_4")))
    collection4=collection4.withColumn("offer_id",changing_name(collection4("_5")))
    collection4=collection4.withColumn("secondary_offers",changing_name(collection4("_6")))
    collection4=collection4.withColumn("message_text",changing_name(collection4("_7")))
    collection4=collection4.withColumn("status",changing_name(collection4("_8")))
    collection4=collection4.withColumn("created_on",changing_name(collection4("_9")))
    collection4=collection4.select("msg_system_id","customer_code","campaign_id","delivery_date","offer_id","secondary_offers","message_text","status","created_on")
    collection4.show()
    collection4.write.mode("append").jdbc(url,"message_to_be_sent_system",prop)

var collection3 = sc.parallelize(Seq((ucic,"2"))).toDF()        //maintaining debit nump inactive log
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("nump_cond",changing_name(collection3("_2")))
    collection3=collection3.select("ucic","nump_cond")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "debit_nump_inactive", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()

  }
  else if(xx=="2")
  {
    var offer_id=credit_nump_tax_saving.collect()(0).getInt(0)
var campaign_id=credit_nump_tax_saving.collect()(0).getInt(1)
var delivery_date=credit_nump_tax_saving.collect()(0).getString(3)

    var SMS_text_1=credit_nump_tax_saving.collect()(0).getString(5)
    SMS_text_1=SMS_text_1.replaceAll("<bn>",bank_name)
    var collection4 = sc.parallelize(Seq((0,ucic,campaign_id,delivery_date,offer_id,"[]",SMS_text_1,0,ct))).toDF()
    collection4=collection4.withColumn("msg_system_id",changing_name(collection4("_1")))
    collection4=collection4.withColumn("customer_code",changing_name(collection4("_2")))
    collection4=collection4.withColumn("campaign_id",changing_name(collection4("_3")))
    collection4=collection4.withColumn("delivery_date",changing_name(collection4("_4")))
    collection4=collection4.withColumn("offer_id",changing_name(collection4("_5")))
    collection4=collection4.withColumn("secondary_offers",changing_name(collection4("_6")))
    collection4=collection4.withColumn("message_text",changing_name(collection4("_7")))
    collection4=collection4.withColumn("status",changing_name(collection4("_8")))
    collection4=collection4.withColumn("created_on",changing_name(collection4("_9")))
    collection4=collection4.select("msg_system_id","customer_code","campaign_id","delivery_date","offer_id","secondary_offers","message_text","status","created_on")
    collection4.show()
    collection4.write.mode("append").jdbc(url,"message_to_be_sent_system",prop)

var collection3 = sc.parallelize(Seq((ucic,"3"))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("nump_cond",changing_name(collection3("_2")))
    collection3=collection3.select("ucic","nump_cond")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "debit_nump_inactive", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()

  }
  else if(xx=="3")
  {
    var offer_id=debit_trending_offer.collect()(0).getInt(0)
var campaign_id=debit_trending_offer.collect()(0).getInt(1)
var delivery_date=debit_trending_offer.collect()(0).getString(3)

    var SMS_text_2=debit_trending_offer.collect()(0).getString(5)
    SMS_text_2=SMS_text_2.replaceAll("<bn>",bank_name)
    var collection4 = sc.parallelize(Seq((0,ucic,campaign_id,delivery_date,offer_id,"[]",SMS_text_2,0,ct))).toDF()
    collection4=collection4.withColumn("msg_system_id",changing_name(collection4("_1")))
    collection4=collection4.withColumn("customer_code",changing_name(collection4("_2")))
    collection4=collection4.withColumn("campaign_id",changing_name(collection4("_3")))
    collection4=collection4.withColumn("delivery_date",changing_name(collection4("_4")))
    collection4=collection4.withColumn("offer_id",changing_name(collection4("_5")))
    collection4=collection4.withColumn("secondary_offers",changing_name(collection4("_6")))
    collection4=collection4.withColumn("message_text",changing_name(collection4("_7")))
    collection4=collection4.withColumn("status",changing_name(collection4("_8")))
    collection4=collection4.withColumn("created_on",changing_name(collection4("_9")))
    collection4=collection4.select("msg_system_id","customer_code","campaign_id","delivery_date","offer_id","secondary_offers","message_text","status","created_on")
    collection4.show()
    collection4.write.mode("append").jdbc(url,"message_to_be_sent_system",prop)

var collection3 = sc.parallelize(Seq((ucic,"1"))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("nump_cond",changing_name(collection3("_2")))
    collection3=collection3.select("ucic","nump_cond")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "debit_nump_inactive", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()

  }


  else{
     var offer_id=debit_trending_offer.collect()(0).getInt(0)
var campaign_id=debit_trending_offer.collect()(0).getInt(1)
var delivery_date=debit_trending_offer.collect()(0).getString(3)

var SMS_text_1=debit_trending_offer.collect()(0).getString(4)
SMS_text_1=SMS_text_1.replaceAll("<bn>",bank_name)     //replacing <bn> with the name of bank in SMS_text
   var collection4 = sc.parallelize(Seq((0,ucic,campaign_id,delivery_date,offer_id,"[]",SMS_text_1,0,ct))).toDF()
    collection4=collection4.withColumn("msg_system_id",changing_name(collection4("_1")))
    collection4=collection4.withColumn("customer_code",changing_name(collection4("_2")))
    collection4=collection4.withColumn("campaign_id",changing_name(collection4("_3")))
    collection4=collection4.withColumn("delivery_date",changing_name(collection4("_4")))
    collection4=collection4.withColumn("offer_id",changing_name(collection4("_5")))
    collection4=collection4.withColumn("secondary_offers",changing_name(collection4("_6")))
    collection4=collection4.withColumn("message_text",changing_name(collection4("_7")))
    collection4=collection4.withColumn("status",changing_name(collection4("_8")))
    collection4=collection4.withColumn("created_on",changing_name(collection4("_9")))
    collection4=collection4.select("msg_system_id","customer_code","campaign_id","delivery_date","offer_id","secondary_offers","message_text","status","created_on")
    collection4.show()
    collection4.write.mode("append").jdbc(url,"message_to_be_sent_system",prop)

var collection3 = sc.parallelize(Seq((ucic,"1"))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("nump_cond",changing_name(collection3("_2")))
    collection3=collection3.select("ucic","nump_cond")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "debit_nump_inactive", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()

  }}
}}}

val b = 0;

for(b <- 0 to (customer_inactive_count.toInt-1)) {

var ucic=customer_inactive.collect()(b).getString(0)

var debit_nump_inactive = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "debit_nump_inactive", "keyspace" -> "goals101"))
  .load()

var debit_trending_offer=sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='Trending offer'")  //filtering trending offer from offer_data
debit_trending_offer.show()
debit_nump_inactive.registerTempTable("debit_nump_inactive")

if (sqlContext.sql("select * from debit_nump_inactive where ucic='"+ucic+"'").count()==0)
 {
var offer_id=debit_trending_offer.collect()(0).getInt(0)
var campaign_id=debit_trending_offer.collect()(0).getInt(1)
var delivery_date=debit_trending_offer.collect()(0).getString(3)
var SMS_text_1=debit_trending_offer.collect()(0).getString(4)
SMS_text_1=SMS_text_1.replaceAll("<bn>",bank_name)
        
    var collection4 = sc.parallelize(Seq((0,ucic,campaign_id,delivery_date,offer_id,"[]",SMS_text_1,0,ct))).toDF()
    collection4=collection4.withColumn("msg_system_id",changing_name(collection4("_1")))
    collection4=collection4.withColumn("customer_code",changing_name(collection4("_2")))
    collection4=collection4.withColumn("campaign_id",changing_name(collection4("_3")))
    collection4=collection4.withColumn("delivery_date",changing_name(collection4("_4")))
    collection4=collection4.withColumn("offer_id",changing_name(collection4("_5")))
    collection4=collection4.withColumn("secondary_offers",changing_name(collection4("_6")))
    collection4=collection4.withColumn("message_text",changing_name(collection4("_7")))
    collection4=collection4.withColumn("status",changing_name(collection4("_8")))
    collection4=collection4.withColumn("created_on",changing_name(collection4("_9")))
    collection4=collection4.select("msg_system_id","customer_code","campaign_id","delivery_date","offer_id","secondary_offers","message_text","status","created_on")
    collection4.show()
    collection4.write.mode("append").jdbc(url,"message_to_be_sent_system",prop)

var collection3 = sc.parallelize(Seq((ucic,"1"))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("inactive_cond",changing_name(collection3("_2")))
    collection3=collection3.select("ucic","inactive_cond")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "debit_nump_inactive", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
  }
else{    
val debit_nump_inactive2 = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "debit_nump_inactive", "keyspace" -> "goals101"))
  .load()
  val debit_nump_inactive_count=debit_nump_inactive2.count()
  for( r <- 0 to (debit_nump_inactive_count.toInt-1) ){
  if(ucic==debit_nump_inactive2.collect()(r).getString(0))
{  

  var xx=debit_nump_inactive2.collect()(r).getString(1)
 if(xx=="1")
  {
var offer_id=debit_trending_offer.collect()(0).getInt(0)
var campaign_id=debit_trending_offer.collect()(0).getInt(1)
var delivery_date=debit_trending_offer.collect()(0).getString(3)

    var SMS_text_2=debit_trending_offer.collect()(0).getString(5)
    SMS_text_2=SMS_text_2.replaceAll("<bn>",bank_name)
    var collection4 = sc.parallelize(Seq((0,ucic,campaign_id,delivery_date,offer_id,"[]",SMS_text_2,0,ct))).toDF()
    collection4=collection4.withColumn("msg_system_id",changing_name(collection4("_1")))
    collection4=collection4.withColumn("customer_code",changing_name(collection4("_2")))
    collection4=collection4.withColumn("campaign_id",changing_name(collection4("_3")))
    collection4=collection4.withColumn("delivery_date",changing_name(collection4("_4")))
    collection4=collection4.withColumn("offer_id",changing_name(collection4("_5")))
    collection4=collection4.withColumn("secondary_offers",changing_name(collection4("_6")))
    collection4=collection4.withColumn("message_text",changing_name(collection4("_7")))
    collection4=collection4.withColumn("status",changing_name(collection4("_8")))
    collection4=collection4.withColumn("created_on",changing_name(collection4("_9")))
    collection4=collection4.select("msg_system_id","customer_code","campaign_id","delivery_date","offer_id","secondary_offers","message_text","status","created_on")
    collection4.show()
    collection4.write.mode("append").jdbc(url,"message_to_be_sent_system",prop)

var collection3 = sc.parallelize(Seq((ucic,"2"))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("inactive_cond",changing_name(collection3("_2")))
    collection3=collection3.select("ucic","inactive_cond")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "debit_nump_inactive", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()

  }
 /* else if(xx=="2")
  {
var offer_id=debit_trending_offer.collect()(0).getInt(0)
var campaign_id=debit_trending_offer.collect()(0).getInt(1)
var delivery_date=debit_trending_offer.collect()(0).getString(3)

    var SMS_text_2=debit_trending_offer.collect()(0).getString(5)
    SMS_text_2=SMS_text_2.replaceAll("<bn>",bank_name)
    var collection4 = sc.parallelize(Seq((0,ucic,campaign_id,delivery_date,offer_id,"[]",SMS_text_2,0,ct))).toDF()
    collection4=collection4.withColumn("msg_system_id",changing_name(collection4("_1")))
    collection4=collection4.withColumn("customer_code",changing_name(collection4("_2")))
    collection4=collection4.withColumn("campaign_id",changing_name(collection4("_3")))
    collection4=collection4.withColumn("delivery_date",changing_name(collection4("_4")))
    collection4=collection4.withColumn("offer_id",changing_name(collection4("_5")))
    collection4=collection4.withColumn("secondary_offers",changing_name(collection4("_6")))
    collection4=collection4.withColumn("message_text",changing_name(collection4("_7")))
    collection4=collection4.withColumn("status",changing_name(collection4("_8")))
    collection4=collection4.withColumn("created_on",changing_name(collection4("_9")))
    collection4=collection4.select("msg_system_id","customer_code","campaign_id","delivery_date","offer_id","secondary_offers","message_text","status","created_on")
    collection4.show()
    collection4.write.mode("append").jdbc(url,"message_to_be_sent_system",prop)

var collection3 = sc.parallelize(Seq((ucic,"1"))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("inactive_cond",changing_name(collection3("_2")))
    collection3=collection3.select("ucic","inactive_cond")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "debit_nump_inactive", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()

  }
*/

  else{
     var offer_id=debit_trending_offer.collect()(0).getInt(0)
var campaign_id=debit_trending_offer.collect()(0).getInt(1)
var delivery_date=debit_trending_offer.collect()(0).getString(3)

var SMS_text_1=debit_trending_offer.collect()(0).getString(4)
SMS_text_1=SMS_text_1.replaceAll("<bn>",bank_name)
   var collection4 = sc.parallelize(Seq((0,ucic,campaign_id,delivery_date,offer_id,"[]",SMS_text_1,0,ct))).toDF()
    collection4=collection4.withColumn("msg_system_id",changing_name(collection4("_1")))
    collection4=collection4.withColumn("customer_code",changing_name(collection4("_2")))
    collection4=collection4.withColumn("campaign_id",changing_name(collection4("_3")))
    collection4=collection4.withColumn("delivery_date",changing_name(collection4("_4")))
    collection4=collection4.withColumn("offer_id",changing_name(collection4("_5")))
    collection4=collection4.withColumn("secondary_offers",changing_name(collection4("_6")))
    collection4=collection4.withColumn("message_text",changing_name(collection4("_7")))
    collection4=collection4.withColumn("status",changing_name(collection4("_8")))
    collection4=collection4.withColumn("created_on",changing_name(collection4("_9")))
    collection4=collection4.select("msg_system_id","customer_code","campaign_id","delivery_date","offer_id","secondary_offers","message_text","status","created_on")
    collection4.show()
    collection4.write.mode("append").jdbc(url,"message_to_be_sent_system",prop)

var collection3 = sc.parallelize(Seq((ucic,"1"))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("inactive_cond",changing_name(collection3("_2")))
    collection3=collection3.select("ucic","inactive_cond")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "debit_nump_inactive", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()

  }}
}}}

}}
