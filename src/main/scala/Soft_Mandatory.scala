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

 object Soft_Mandatory {
  def main(args: Array[String]) {

val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")

val sc = new SparkContext(conf)

Class.forName("com.mysql.jdbc.Driver")

val prop = new java.util.Properties
prop.setProperty("user","goals101")
prop.setProperty("password","goals123")

val url="jdbc:mysql://engage01.cfyrn3uyeiwt.us-west-2.rds.amazonaws.com:3306/engage_db?zeroDateTimeBehavior=convertToNull"

val ran = scala.util.Random
var bank_name="RBL Bank"

/*var current_time=java.time.LocalDate.now
var ct=current_time.toString()
var year=ct.substring(0,4)
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
//var ct=year2+"-"+month2+"-"+day2
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

val time_simulator = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "time_simulator", "keyspace" -> "goals101"))
  .load()

//val cash_loan_schema = StructType(Array(StructField("ucic", StringType, true),StructField("amount", StringType, true),StructField("interest", StringType, true)
 //                          ))

//val cash_loan_data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").schema(cash_loan_schema).load("hdfs://master:9000/cash_loan.csv")

//val limit_schema = StructType(Array(StructField("ucic", StringType, true),StructField("limit_amount", StringType, true)
//                           ))

//val limit_data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").schema(limit_schema).load("hdfs://master:9000/limit_enhancement.csv")

//val card_insurance_schema = StructType(Array(StructField("ucic", StringType, true),StructField("flag", StringType, true)
//                           ))

//val card_insurance_data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").schema(card_insurance_schema).load("hdfs://master:9000/card_insurance.csv")

//cash_loan_data.write.format("org.apache.spark.sql.cassandra")
// .options(Map( "table" -> "cash_loan", "keyspace" -> "goals101"))
// .mode(SaveMode.Append).save()
// limit_data.write.format("org.apache.spark.sql.cassandra")
// .options(Map( "table" -> "limit_enhancement", "keyspace" -> "goals101"))
// .mode(SaveMode.Append).save()
// card_insurance_data.write.format("org.apache.spark.sql.cassandra")
// .options(Map( "table" -> "card_insurance", "keyspace" -> "goals101"))
// .mode(SaveMode.Append).save()

var month=time_simulator.collect()(0).getString(2)
var year=time_simulator.collect()(0).getString(0)
var day=time_simulator.collect()(0).getString(1)
var ct=year+"-"+month+"-"+day
var cash_loan_month = "[02,04,06,08,10,12]"
var limit_enhancement_month="[01,04,07,11]"
var card_insurance_month="[06,12]"

val user_quota = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
  .load()

user_quota.registerTempTable("user_quota")
val first_date = udf((offer_name: String) => {
  month+"/"+day+"/"+year 
})

val changing_name = udf((_1: String)=> {
        _1
  })


val calculating_date = udf((week_no: String,day_of_the_week: String,first_day: String,time: String) => {   //evaluating delivery date 
var z = new Array[String](10)
var poka = new Array[String](10)

var arr = new Array[String](10)
var c=0;
var g=0;
    arr = Array("Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday")
      try{
      for( a <- 0 to 6){

        if(arr(a)!=first_day)
        {
            z(a)=arr(a)
            c=c+1;
        }
else
{
      throw new Exception("arg 1 was wrong...");

      }     
      }}

catch {
         case x:Exception=>{

}}

var e=0;
var s=0;
      for(l<-c to 6)
      {
         poka(g)=arr(l)
    g=g+1;
      }
      for(i<-0 to c-1)
      {
         poka(g)=z(i)
         g=g+1
      }

      for(j<-0 to 6)
      {
         if(poka(j)==day_of_the_week)
         s=j+1;
      }

      if(week_no=="1")
      {
        e=s;
      }
      if(week_no=="2")
      {
      e=s+7;
  }
      if(week_no=="3")
    {  e=s+14;}
      if(week_no=="4")
     { e=s+21;
     }
var t=time.substring(10,19)

year+"-"+month+"-"+e+" "+t
 })

var offer_data = sqlContext.read.jdbc(url,"offer_data",prop)

offer_data=offer_data.filter($"campaign_rules"==="Cash loan" || $"campaign_rules"==="Limit enhancement" || $"campaign_rules"==="Card insurance") //filtering soft mandatory offers
offer_data=offer_data.withColumn("first_date",first_date(offer_data("campaign_rules")))
offer_data.registerTempTable("offer_data")
offer_data=sqlContext.sql(
  """SELECT campaign_rules,offer_id,campaign_id,gender,city,interval_gap,cap_type,card_type,SMS_text_1,SMS_text_2,day_of_the_week,week_no,relationship_type,merchant_name,time,merchant_category,campaign_type,
        from_unixtime(unix_timestamp(first_date,"MM/dd/yyyy"), 'EEEEE') AS first_day
      FROM offer_data"""
)
offer_data.show()
offer_data=offer_data.withColumn("delivery_date",calculating_date(offer_data("week_no"),offer_data("day_of_the_week"),offer_data("first_day"),offer_data("time")))

offer_data.show()
offer_data.registerTempTable("offer")

val cash_loan = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "cash_loan", "keyspace" -> "goals101"))
  .load() 

val card_insurance = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "card_insurance", "keyspace" -> "goals101"))
  .load() 

val limit_enhancement = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "limit_enhancement", "keyspace" -> "goals101"))
  .load() 

cash_loan.registerTempTable("cash_loan")
limit_enhancement.registerTempTable("limit_enhancement")
card_insurance.registerTempTable("card_insurance")

val a = 0;

if(cash_loan_month.contains(month))
{
var cash_loan_count=cash_loan.count()
for(a <- 0 to (cash_loan_count.toInt-1)) {

var ucic=cash_loan.collect()(a).getString(0)
var amount=cash_loan.collect()(a).getString(1)
var interest=cash_loan.collect()(a).getString(2)

var soft_inc=sqlContext.sql("select soft_mandatory,sms_sent from user_quota where ucic='"+ucic+"'")

var soft_mandatory_log = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
  .load()

var cash_loan_offer=sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='Cash loan'")  //filtering cash loan offer from offer_data table
cash_loan_offer.show()
var offer_id=cash_loan_offer.collect()(0).getInt(0)
 var campaign_id=cash_loan_offer.collect()(0).getInt(1)
 
 var delivery_date=cash_loan_offer.collect()(0).getString(3)

soft_mandatory_log.registerTempTable("soft_mandatory_log")

if (sqlContext.sql("select * from soft_mandatory_log where ucic='"+ucic+"'").count()==0)
 {    
  var SMS_text_1=cash_loan_offer.collect()(0).getString(4)
    SMS_text_1=SMS_text_1.replaceAll("<bn>",bank_name)
    SMS_text_1=SMS_text_1.replaceAll("<pql>",amount)
    SMS_text_1=SMS_text_1.replaceAll("<ir>",interest)
    
    
    var collection4 = sc.parallelize(Seq((0,ucic,campaign_id,delivery_date,offer_id,"null",SMS_text_1,0,ct))).toDF()
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
    collection3=collection3.withColumn("cash_loan",changing_name(collection3("_2")))
    collection3=collection3.select("ucic","cash_loan")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
  try{
var quota_inc=soft_inc.collect()(0).getString(0)
var sms_sent=soft_inc.collect()(0).getString(1)
if(!quota_inc.equals("null"))
{
  var quota_inc2=quota_inc.toInt + 1
  println(quota_inc2+"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

  var sms_sent2=sms_sent.toInt + 1
  var collection3 = sc.parallelize(Seq((ucic,quota_inc2,sms_sent2))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("soft_mandatory",changing_name(collection3("_2")))
    collection3=collection3.withColumn("sms_sent",changing_name(collection3("_3")))    
    collection3=collection3.select("ucic","soft_mandatory","sms_sent")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
}
}
catch{
  case x:Exception=>{

  var sms_sent=soft_inc.collect()(0).getString(1)
  var sms_sent2=sms_sent.toInt + 1
  var collection3 = sc.parallelize(Seq((ucic,"1",sms_sent2))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("soft_mandatory",changing_name(collection3("_2")))
    collection3=collection3.withColumn("sms_sent",changing_name(collection3("_3")))
    collection3=collection3.select("ucic","soft_mandatory","sms_sent")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
}}

  }
else{    
val soft_mandatory_log2 = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
  .load()
  val soft_mandatory_count=soft_mandatory_log2.count()
  for( r <- 0 to (soft_mandatory_count.toInt-1) ){
  if(ucic==soft_mandatory_log2.collect()(r).getString(0))
{  
  var xx=soft_mandatory_log2.collect()(r).getString(2)
 if(xx=="1")
  {
    var SMS_text_2=cash_loan_offer.collect()(0).getString(5)
    SMS_text_2=SMS_text_2.replaceAll("<bn>",bank_name)
    SMS_text_2=SMS_text_2.replaceAll("<pql>",amount)
    SMS_text_2=SMS_text_2.replaceAll("<ir>",interest)
var collection4 = sc.parallelize(Seq((0,ucic,campaign_id,delivery_date,offer_id,"null",SMS_text_2,0,ct))).toDF()
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
    collection3=collection3.withColumn("cash_loan",changing_name(collection3("_2")))
    collection3=collection3.select("ucic","cash_loan")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
try{
var quota_inc=soft_inc.collect()(0).getString(0)
var sms_sent=soft_inc.collect()(0).getString(1)
if(!quota_inc.equals("null"))
{
  var quota_inc2=quota_inc.toInt + 1
  println(quota_inc2+"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

  var sms_sent2=sms_sent.toInt + 1
  var collection3 = sc.parallelize(Seq((ucic,quota_inc2,sms_sent2))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("soft_mandatory",changing_name(collection3("_2")))
    collection3=collection3.withColumn("sms_sent",changing_name(collection3("_3")))    
    collection3=collection3.select("ucic","soft_mandatory","sms_sent")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
}
}
catch{
  case x:Exception=>{

  var sms_sent=soft_inc.collect()(0).getString(1)
  var sms_sent2=sms_sent.toInt + 1
  var collection3 = sc.parallelize(Seq((ucic,"1",sms_sent2))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("soft_mandatory",changing_name(collection3("_2")))
    collection3=collection3.withColumn("sms_sent",changing_name(collection3("_3")))
    collection3=collection3.select("ucic","soft_mandatory","sms_sent")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
}}

  }
  else{
var SMS_text_1=cash_loan_offer.collect()(0).getString(4)
SMS_text_1=SMS_text_1.replaceAll("<bn>",bank_name)
    SMS_text_1=SMS_text_1.replaceAll("<pql>",amount)
    SMS_text_1=SMS_text_1.replaceAll("<ir>",interest)
var collection4 = sc.parallelize(Seq((0,ucic,campaign_id,delivery_date,offer_id,"null",SMS_text_1,0,ct))).toDF()
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
    collection3=collection3.withColumn("cash_loan",changing_name(collection3("_2")))
    collection3=collection3.select("ucic","cash_loan")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
try{
var quota_inc=soft_inc.collect()(0).getString(0)
var sms_sent=soft_inc.collect()(0).getString(1)
if(!quota_inc.equals("null"))
{
  var quota_inc2=quota_inc.toInt + 1
  println(quota_inc2+"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

  var sms_sent2=sms_sent.toInt + 1
  var collection3 = sc.parallelize(Seq((ucic,quota_inc2,sms_sent2))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("soft_mandatory",changing_name(collection3("_2")))
    collection3=collection3.withColumn("sms_sent",changing_name(collection3("_3")))    
    collection3=collection3.select("ucic","soft_mandatory","sms_sent")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
}
}
catch{
  case x:Exception=>{

  var sms_sent=soft_inc.collect()(0).getString(1)
  var sms_sent2=sms_sent.toInt + 1
  var collection3 = sc.parallelize(Seq((ucic,"1",sms_sent2))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("soft_mandatory",changing_name(collection3("_2")))
    collection3=collection3.withColumn("sms_sent",changing_name(collection3("_3")))
    collection3=collection3.select("ucic","soft_mandatory","sms_sent")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
}}

  }}
}}}
}

val b = 0;
if(card_insurance_month.contains(month))
{
var card_insurance_count=cash_loan.count()
for(b <- 0 to (card_insurance_count.toInt-1)) {

var ucic=card_insurance.collect()(b).getString(0)
var flag=card_insurance.collect()(b).getString(1)

var soft_inc=sqlContext.sql("select soft_mandatory,sms_sent from user_quota where ucic='"+ucic+"'")

var soft_mandatory_log = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
  .load()

var card_insurance_offer=sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='Card insurance'")  //filtering card insurance offer
card_insurance_offer.show()
var offer_id=card_insurance_offer.collect()(0).getInt(0)
 var campaign_id=card_insurance_offer.collect()(0).getInt(1)
 
 var delivery_date=card_insurance_offer.collect()(0).getString(3)

soft_mandatory_log.registerTempTable("soft_mandatory_log")

if (sqlContext.sql("select * from soft_mandatory_log where ucic='"+ucic+"'").count()==0)
 {    
  var SMS_text_1=card_insurance_offer.collect()(0).getString(4)
    SMS_text_1=SMS_text_1.replaceAll("<bn>",bank_name)
    /*SMS_text_1=SMS_text_1.replaceAll("<pql>",amount)
    SMS_text_1=SMS_text_1.replaceAll("<ir>",interest)
    */
    
    var collection4 = sc.parallelize(Seq((0,ucic,campaign_id,delivery_date,offer_id,"null",SMS_text_1,0,ct))).toDF()
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
    collection3=collection3.withColumn("card_insurance",changing_name(collection3("_2")))
    collection3=collection3.select("ucic","card_insurance")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
try{
var quota_inc=soft_inc.collect()(0).getString(0)
var sms_sent=soft_inc.collect()(0).getString(1)
if(!quota_inc.equals("null"))
{
  var quota_inc2=quota_inc.toInt + 1
  println(quota_inc2+"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

  var sms_sent2=sms_sent.toInt + 1
  var collection3 = sc.parallelize(Seq((ucic,quota_inc2,sms_sent2))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("soft_mandatory",changing_name(collection3("_2")))
    collection3=collection3.withColumn("sms_sent",changing_name(collection3("_3")))    
    collection3=collection3.select("ucic","soft_mandatory","sms_sent")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
}
}
catch{
  case x:Exception=>{

  var sms_sent=soft_inc.collect()(0).getString(1)
  var sms_sent2=sms_sent.toInt + 1
  var collection3 = sc.parallelize(Seq((ucic,"1",sms_sent2))).toDF()     //deducting quota
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("soft_mandatory",changing_name(collection3("_2")))
    collection3=collection3.withColumn("sms_sent",changing_name(collection3("_3")))
    collection3=collection3.select("ucic","soft_mandatory","sms_sent")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
}}

  }
else{    
val soft_mandatory_log2 = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
  .load()
  val soft_mandatory_count=soft_mandatory_log2.count()
  for( r <- 0 to (soft_mandatory_count.toInt-1) ){
  if(ucic==soft_mandatory_log2.collect()(r).getString(0))
{  
  var xx=soft_mandatory_log2.collect()(r).getString(1)
 if(xx=="1")
  {
    var SMS_text_2=card_insurance_offer.collect()(0).getString(5)
    SMS_text_2=SMS_text_2.replaceAll("<bn>",bank_name)
    /*SMS_text_1=SMS_text_1.replaceAll("<pql>",amount)
    SMS_text_1=SMS_text_1.replaceAll("<ir>",interest)*/
var collection4 = sc.parallelize(Seq((0,ucic,campaign_id,delivery_date,offer_id,"null",SMS_text_2,0,ct))).toDF()
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
    collection3=collection3.withColumn("card_insurance",changing_name(collection3("_2")))
    collection3=collection3.select("ucic","card_insurance")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
try{
var quota_inc=soft_inc.collect()(0).getString(0)
var sms_sent=soft_inc.collect()(0).getString(1)
if(!quota_inc.equals("null"))
{
  var quota_inc2=quota_inc.toInt + 1
  println(quota_inc2+"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

  var sms_sent2=sms_sent.toInt + 1
  var collection3 = sc.parallelize(Seq((ucic,quota_inc2,sms_sent2))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("soft_mandatory",changing_name(collection3("_2")))
    collection3=collection3.withColumn("sms_sent",changing_name(collection3("_3")))    
    collection3=collection3.select("ucic","soft_mandatory","sms_sent")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
}
}
catch{
  case x:Exception=>{

  var sms_sent=soft_inc.collect()(0).getString(1)
  var sms_sent2=sms_sent.toInt + 1
  var collection3 = sc.parallelize(Seq((ucic,"1",sms_sent2))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("soft_mandatory",changing_name(collection3("_2")))
    collection3=collection3.withColumn("sms_sent",changing_name(collection3("_3")))
    collection3=collection3.select("ucic","soft_mandatory","sms_sent")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
}}

  }
  else{
var SMS_text_1=card_insurance_offer.collect()(0).getString(4)
SMS_text_1=SMS_text_1.replaceAll("<bn>",bank_name)
   /* SMS_text_2=SMS_text_2.replaceAll("<pql>",amount)
    SMS_text_2=SMS_text_2.replaceAll("<ir>",interest)*/
var collection4 = sc.parallelize(Seq((0,ucic,campaign_id,delivery_date,offer_id,"null",SMS_text_1,0,ct))).toDF()
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
    collection3=collection3.withColumn("card_insurance",changing_name(collection3("_2")))
    collection3=collection3.select("ucic","card_insurance")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
try{
var quota_inc=soft_inc.collect()(0).getString(0)
var sms_sent=soft_inc.collect()(0).getString(1)
if(!quota_inc.equals("null"))
{
  var quota_inc2=quota_inc.toInt + 1
  println(quota_inc2+"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

  var sms_sent2=sms_sent.toInt + 1
  var collection3 = sc.parallelize(Seq((ucic,quota_inc2,sms_sent2))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("soft_mandatory",changing_name(collection3("_2")))
    collection3=collection3.withColumn("sms_sent",changing_name(collection3("_3")))    
    collection3=collection3.select("ucic","soft_mandatory","sms_sent")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
}
}
catch{
  case x:Exception=>{

  var sms_sent=soft_inc.collect()(0).getString(1)
  var sms_sent2=sms_sent.toInt + 1
  var collection3 = sc.parallelize(Seq((ucic,"1",sms_sent2))).toDF()
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("soft_mandatory",changing_name(collection3("_2")))
    collection3=collection3.withColumn("sms_sent",changing_name(collection3("_3")))
    collection3=collection3.select("ucic","soft_mandatory","sms_sent")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
}}

  }}
}}}
}

val c = 0;
if(limit_enhancement_month.contains(month))
{
  println("yahoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo")
var limit_enhancement_count=limit_enhancement.count()
for(c <- 0 to (limit_enhancement_count.toInt-1)) {

var ucic=limit_enhancement.collect()(c).getString(0)


var soft_inc=sqlContext.sql("select soft_mandatory,sms_sent from user_quota where ucic='"+ucic+"'")
//if()
var amount=limit_enhancement.collect()(c).getString(1)
//var interest=cash_loan.collect()(a).getString(2)

offer_data.show()
var soft_mandatory_log = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
  .load()

var limit_enhancement_offer=sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='Limit enhancement' limit 1")  //filtering limit enhancement offer
offer_data.show()
limit_enhancement_offer.show()
var offer_id=limit_enhancement_offer.collect()(0).getInt(0)
 var campaign_id=limit_enhancement_offer.collect()(0).getInt(1)
 
 var delivery_date=limit_enhancement_offer.collect()(0).getString(3)

soft_mandatory_log.registerTempTable("soft_mandatory_log")

if (sqlContext.sql("select * from soft_mandatory_log where ucic='"+ucic+"'").count()==0)
 {
    limit_enhancement_offer.show()    
    var SMS_text_1=limit_enhancement_offer.collect()(0).getString(4)
    print(offer_id+"&&&"+delivery_date)   
    SMS_text_1=SMS_text_1.replaceAll("<bn>",bank_name)
    SMS_text_1=SMS_text_1.replaceAll("<lef>",amount)
    
    var collection4 = sc.parallelize(Seq((0,ucic,campaign_id,delivery_date,offer_id,"null",SMS_text_1,0,ct))).toDF()
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

var collection3 = sc.parallelize(Seq((ucic,"1"))).toDF()  //maintaining soft mandatory log 
    collection3=collection3.withColumn("ucic",changing_name(collection3("_1")))
    collection3=collection3.withColumn("limit_enhancement",changing_name(collection3("_2")))
    collection3=collection3.select("ucic","limit_enhancement")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()


  }
else{    
val soft_mandatory_log2 = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
  .load()
  val soft_mandatory_count=soft_mandatory_log2.count()
  for( r <- 0 to (soft_mandatory_count.toInt-1) ){
  if(ucic==soft_mandatory_log2.collect()(r).getString(0))
{  
  var xx=soft_mandatory_log2.collect()(r).getString(3)
 if(xx=="1")
  {
    var SMS_text_2=limit_enhancement_offer.collect()(0).getString(5)
    SMS_text_2=SMS_text_2.replaceAll("<bn>",bank_name)
    SMS_text_2=SMS_text_2.replaceAll("<lef>",amount)

var collection4 = sc.parallelize(Seq((0,ucic,campaign_id,delivery_date,offer_id,"null",SMS_text_2,0,ct))).toDF()
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
    collection3=collection3.withColumn("limit_enhancement",changing_name(collection3("_2")))
    collection3=collection3.select("ucic","limit_enhancement")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()

  }
  else{
var SMS_text_1=limit_enhancement_offer.collect()(0).getString(4)
SMS_text_1=SMS_text_1.replaceAll("<bn>",bank_name)
SMS_text_1=SMS_text_1.replaceAll("<lef>",amount)
//    SMS_text_2=SMS_text_2.replaceAll("<ir>",interest)
var collection4 = sc.parallelize(Seq((0,ucic,campaign_id,delivery_date,offer_id,"null",SMS_text_1,0,ct))).toDF()
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
    collection3=collection3.withColumn("limit_enhancement",changing_name(collection3("_2")))
    collection3=collection3.select("ucic","limit_enhancement")
    collection3.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
  }}
}}}



}

}}
