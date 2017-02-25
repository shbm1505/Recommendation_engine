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
import org.apache.spark.sql.functions
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


 object Config_file {
 
var bank_name="RBL bank"
var mysql_connection_string="jdbc:mysql://engage01.cfyrn3uyeiwt.us-west-2.rds.amazonaws.com:3306/engage_db?zeroDateTimeBehavior=convertToNull"
var cassandra_ip="127.0.0.1"
var mysql_username="goals101"
var mysql_password="goals123"
var env_variable="Test"
var keyspace="goals101"
var script_id_mandatory="Mandatory"
var script_id_soft_mandatory="Soft_Mandatory"
var script_id_tfidf="Tfidf"
var script_id_engine_rules="Engine_Rules"
var script_id_auto_recommendation="Auto_Recommendation"
var script_id_time_update="Time_update"
var script_id_nump_inactive="Nunp_inactive"
var script_id_evaluating_quota="Evaluating_quota"
 
}




