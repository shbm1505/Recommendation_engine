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

 object Time_update {
  def main(args: Array[String]) {

val conf = new SparkConf(true).set("spark.cassandra.connection.host", Config_file.cassandra_ip)

val sc = new SparkContext(conf)
Class.forName("com.mysql.jdbc.Driver")

val prop = new java.util.Properties
prop.setProperty("user",Config_file.mysql_username)
prop.setProperty("password",Config_file.mysql_password)

val url=Config_file.mysql_connection_string
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

var month=Array("01","02","03","04","05","06","07","08","09","10","11","12")
var january=Array("01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31")
var february=Array("01")
val time_simulator = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "time_simulator", "keyspace" -> Config_file.keyspace))
  .load()
val changing_name = udf((_1: String)=> {
        _1
  })

var current_month=time_simulator.collect()(0).getString(2)
var current_day=time_simulator.collect()(0).getString(1)
var week_day=time_simulator.collect()(0).getString(3)
if(week_day.equals("Sun"))
{
  Base_class.update_time_function("2016","Mon",sc)
}
else if(week_day.equals("Mon"))
{
 Base_class.update_time_function("2016","Tue",sc) 
  }
else if(week_day.equals("Tue"))
{
  Base_class.update_time_function("2016","Wed",sc) 
}
else if(week_day.equals("Wed"))
{
  Base_class.update_time_function("2016","Thu",sc)
 }
else if(week_day.equals("Thu"))
{
  Base_class.update_time_function("2016","Fri",sc)
}
else if(week_day.equals("Fri"))
{
  Base_class.update_time_function("2016","Sat",sc)

}
else
{
  Base_class.update_time_function("2016","Sun",sc)
}

if(current_month.equals("01"))
{
for(i <- 0 to (january.length - 1))
{
if(current_day.equals(january(i)) && i!=(january.length - 1))
{
  var new_day=january(i+1)

  var time_update_df = sc.parallelize(Seq(("2016",new_day))).toDF
  time_update_df=time_update_df.withColumn("year",changing_name(time_update_df("_1")))
  time_update_df=time_update_df.withColumn("day",changing_name(time_update_df("_2")))
  time_update_df=time_update_df.select("year","day")
  time_update_df.write .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "time_simulator", "keyspace" -> Config_file.keyspace))
  .mode(SaveMode.Append).save()
}
if(current_day.equals(january(january.length-1)))
{
 var new_day=february(0)
 var new_month=month(1)
  var time_update_df = sc.parallelize(Seq(("2016",new_day,new_month))).toDF
  time_update_df=time_update_df.withColumn("year",changing_name(time_update_df("_1")))
  time_update_df=time_update_df.withColumn("day",changing_name(time_update_df("_2")))
  time_update_df=time_update_df.withColumn("month",changing_name(time_update_df("_3")))
  time_update_df=time_update_df.select("year","day","month")
  time_update_df.write .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "time_simulator", "keyspace" -> Config_file.keyspace))
  .mode(SaveMode.Append).save() 
}
}
}
}}


