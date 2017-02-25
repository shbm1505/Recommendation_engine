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
import org.apache.spark.sql.functions
import scala.io._
import scala.util._ 
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp} 
import scala.collection.mutable.Queue
import scala.util.control.Breaks._

 object Evaluating_quota {
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
  val time_simulator = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "time_simulator", "keyspace" -> "goals101"))
  .load()
  var month=time_simulator.collect()(0).getString(2)
  var year=time_simulator.collect()(0).getString(0) 
  var day=time_simulator.collect()(0).getString(1)
  val changing_name = udf((_1: String)=> {
        _1
  })



val offer_data = sqlContext.read.jdbc(url,"offer_data",prop)
var accounts_unblocked = sqlContext
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "account", "keyspace" -> "goals101"))
  .load() 

accounts_unblocked.registerTempTable("accounts_unblocked")
accounts_unblocked=sqlContext.sql("select type,ucic,account_no,add_on_card,card_type from accounts_unblocked where ucic in ('RBL000002483','RBL000002555','RBL000002556')")
val accounts_count=accounts_unblocked.count()
val a = 0;

for(a <- 0 to (accounts_count.toInt-1)) {
var ucic=accounts_unblocked.collect()(a).getString(1)
var ucics="'"+ucic+"'"

if(sqlContext.sql("select * from accounts_unblocked where ucic='"+ucic+"' and type='credit'").count()==1)        //checking whether user has one or two account
{
 Base_class.calculating_quota(ucic,"16",month,year,sc)
}

if(sqlContext.sql("select * from accounts_unblocked where ucic='"+ucic+"' and type='credit'").count()==1 && sqlContext.sql("select * from accounts_unblocked where ucic='"+ucic+"' and type='debit'").count()==0)
{
 Base_class.calculating_quota(ucic,"8",month,year,sc)
  }

if(sqlContext.sql("select * from accounts_unblocked where ucic='"+ucic+"' and type='credit'").count()==0 && sqlContext.sql("select * from accounts_unblocked where ucic='"+ucic+"' and type='debit'").count()==1)
{
  Base_class.calculating_quota(ucic,"8",month,year,sc)
 }
}}
}



