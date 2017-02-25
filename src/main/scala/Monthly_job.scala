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

 object Monthly_job {
  def main(args: Array[String]) {

val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
      
val sc = new SparkContext(conf)
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val change_name = udf((card_type: String) => {
  card_type 
})

Class.forName("com.mysql.jdbc.Driver")

val prop = new java.util.Properties
prop.setProperty("user","goals101")
prop.setProperty("password","goals123")

val url="jdbc:mysql://engage01.cfyrn3uyeiwt.us-west-2.rds.amazonaws:3306/engage_db"
import sqlContext.implicits._
val statement_credit_schema = StructType(Array(StructField("customer_code", StringType, true),StructField("accountno", StringType, true),StructField("month", StringType, true),StructField("tad", StringType, true),StructField("late_fee", StringType, true),
                           StructField("ovl_fee", StringType, true),StructField("othr_fee", StringType, true),StructField("mem_fee", StringType, true),StructField("fee_type", StringType, true),StructField("service_tax", StringType, true),StructField("interest", StringType, true),StructField("bc1", StringType, true),StructField("bc2", StringType, true),StructField("crlimit", StringType, true)
                           ,StructField("cash_limit", StringType, true),StructField("stm_date", StringType, true),StructField("pmt_due_date", StringType, true),StructField("unbilled_principal", StringType, true)))

val statement_credit_data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").schema(statement_credit_schema).load("hdfs://master:9000/goals101/credit_card_data/statement/files/*")
statement_credit_data.write .format("org.apache.spark.sql.cassandra")
 .options(Map( "table" -> "creditstatement", "keyspace" -> "goals101"))
 .mode(SaveMode.Append).save()
val account_credit_data = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "account", "keyspace" -> "goals101"))
  .load()
val creditstatement_credit_data = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "creditstatement", "keyspace" -> "goals101"))
  .load()
var intermediate_eventdriven = account_credit_data.as('a).join(creditstatement_credit_data.as('b), $"a.account_no" === $"b.accountno","inner")
intermediate_eventdriven =intermediate_eventdriven.select("ucic","a.account_no","cash_limit","crlimit")
intermediate_eventdriven.write .format("org.apache.spark.sql.cassandra")
 .options(Map( "table" -> "intermediate", "keyspace" -> "goals101"))
 .mode(SaveMode.Append).save()

}}






