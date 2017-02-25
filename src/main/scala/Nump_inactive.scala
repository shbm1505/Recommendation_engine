package pl.japila.spark
import org.apache.spark.sql._
import com.datastax.spark.connector._
import com.datastax.driver.core._
import com.datastax.spark.connector.cql._
import org.apache.spark.{ SparkContext, SparkConf }
import com.datastax.spark.connector.cql.CassandraConnectorConf.CassandraSSLConf
import java.io.FileInputStream
import java.security.{ KeyStore, SecureRandom }
import javax.net.ssl.{ SSLContext, TrustManagerFactory }
import com.datastax.driver.core.QueryOptions._
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import com.datastax.driver.core._
import com.datastax.spark.connector._
import com.datastax.spark.connector.util.{ ConfigParameter, ReflectionUtil }
import com.datastax.spark.connector.rdd._
import org.apache.commons.configuration.ConfigurationException
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType };
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ DataType, StringType }
import org.apache.spark.unsafe.types.UTF8String
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.input_file_name
import scala.util.Try
import org.apache.spark.sql.functions.{ lit, udf }
import org.apache.spark.sql
import java.lang.Object
import org.apache.spark.sql.functions._
import scala.io._
import scala.util._
import org.apache.spark.sql.functions.{ from_unixtime, unix_timestamp }
import scala.collection.mutable.Queue
import scala.util.control.Breaks._
import java.util.Calendar
import java.text.SimpleDateFormat

object Nump_inactive {
  def main(args: Array[String]) {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", Config_file.cassandra_ip)
    val sc = new SparkContext(conf)

    Class.forName("com.mysql.jdbc.Driver")
    val prop = new java.util.Properties
    prop.setProperty("user", Config_file.mysql_username)
    prop.setProperty("password", Config_file.mysql_password)
    var bank_name = Config_file.bank_name
    val url = "jdbc:mysql://engage01.cfyrn3uyeiwt.us-west-2.rds.amazonaws.com:3306/engage_db?zeroDateTimeBehavior=convertToNull"
    val ran = scala.util.Random
    val format = new SimpleDateFormat("d-M-y")
    var ct2 = format.format(Calendar.getInstance().getTime())
    var year2 = ct2.substring(4, 8)
    var month2 = ct2.substring(2, 3)
    var day2 = ct2.substring(0, 1)
    var current_time = year2 + "-" + month2 + "-" + day2
    val changing_name = udf((_1: String) => {
      _1
    })

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val time_simulator = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "time_simulator", "keyspace" -> Config_file.keyspace))
      .load()
    var month = time_simulator.collect()(0).getString(2)
    var year = time_simulator.collect()(0).getString(0)
    var day = time_simulator.collect()(0).getString(1)
    current_time = year + "-" + month + "-" + day
    val first_date = udf((offer_name: String) => {
      month + "/" + day + "/" + year
    })
    val calculating_date = udf((week_no: String, day_of_the_week: String, time: String) => {
      var t = time.substring(10, 19)
      year + "-" + month + "-" + day + " " + t
    })

    var offer_data = sqlContext.read.jdbc(url, "offer_data", prop)

    offer_data = offer_data.withColumn("delivery_date", calculating_date(offer_data("week_no"), offer_data("day_of_the_week"), offer_data("time")))
    offer_data.registerTempTable("raw_offer")
    offer_data.registerTempTable("offer")

    var customer = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "customer", "keyspace" -> Config_file.keyspace))
      .load()
    customer.registerTempTable("customer")
    customer = sqlContext.sql("select ucic,age,city,cleaned_city_name,employment_status,gender,last_txn_date_cred,last_txn_date_debit,marital_status,persona,verify from customer where ucic in ('RBL000002260','RBL000002331','RBL000002363','RBL000002371','RBL000002426','RBL000002440','RBL002196181','RBL000005534')")
    var customer_inactive = customer.filter($"last_txn_date_cred" !== "NA") //filtering all non-nump credit users
    var customer_nump = customer.filter($"last_txn_date_cred" === "NA") //filtering all nump credit users
    customer_inactive.registerTempTable("customer_inactive")
    customer_nump.registerTempTable("customer_nump")
    customer_inactive = sqlContext.sql("select ucic from customer_inactive where months_between(current_date(),last_txn_date_cred)>3")
    var customer_nump_count = customer_nump.rdd.count
    var customer_inactive_count = customer_inactive.rdd.count
    val a = 0;
    for (a <- 0 to (customer_nump_count.toInt - 1)) {

      var ucic = customer_nump.collect()(a).getString(0)
      var credit_nump_inactive = sqlContext
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "credit_nump_inactive", "keyspace" -> Config_file.keyspace))
        .load()

      var credit_nump_bank = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='Bank offer wall'") //filtering bank offer wall
      var credit_nump_spend = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='Spend offer wall'") //filtering spend offer wall
      var credit_nump_card_replacement = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='NUMP card replacement'") //filtering card replacement
      credit_nump_inactive.registerTempTable("credit_nump_inactive")

      if (sqlContext.sql("select * from credit_nump_inactive where ucic='" + ucic + "'").rdd.count == 0) {
        var offer_id = credit_nump_bank.collect()(0).getInt(0)
        var campaign_id = credit_nump_bank.collect()(0).getInt(1)
        var delivery_date = credit_nump_bank.collect()(0).getString(3)
        var SMS_text_1 = credit_nump_bank.collect()(0).getString(4)
        SMS_text_1 = SMS_text_1.replaceAll("<bn>", bank_name)
        Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop,sc)
        Base_class.log_nump_inactive(ucic, "1",sc)

      } else {
        val credit_nump_inactive2 = sqlContext
          .read
          .format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "credit_nump_inactive", "keyspace" -> Config_file.keyspace))
          .load()
        val credit_nump_inactive_count = credit_nump_inactive2.rdd.count
        for (r <- 0 to (credit_nump_inactive_count.toInt - 1)) {
          if (ucic == credit_nump_inactive2.collect()(r).getString(0)) {

            var nump_cond_flag = credit_nump_inactive2.collect()(r).getString(2)
            if (nump_cond_flag == "1") {
              var offer_id = credit_nump_spend.collect()(0).getInt(0)
              var campaign_id = credit_nump_spend.collect()(0).getInt(1)
              var delivery_date = credit_nump_spend.collect()(0).getString(3)
              var SMS_text_2 = credit_nump_spend.collect()(0).getString(5)
              SMS_text_2 = SMS_text_2.replaceAll("<bn>", bank_name)

              Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_2, current_time, url, prop,sc)
              Base_class.log_nump_inactive(ucic, "2",sc)

            } else if (nump_cond_flag == "2") {
              
              var offer_id = credit_nump_card_replacement.collect()(0).getInt(0)
              var campaign_id = credit_nump_card_replacement.collect()(0).getInt(1)
              var delivery_date = credit_nump_card_replacement.collect()(0).getString(3)
              var SMS_text_1 = credit_nump_card_replacement.collect()(0).getString(5)
              SMS_text_1 = SMS_text_1.replaceAll("<bn>", bank_name)
              Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop,sc)
              Base_class.log_nump_inactive(ucic, "3",sc)

            } else if (nump_cond_flag == "3") {
              var offer_id = credit_nump_bank.collect()(0).getInt(0)
              var campaign_id = credit_nump_bank.collect()(0).getInt(1)
              var delivery_date = credit_nump_bank.collect()(0).getString(3)
              var SMS_text_2 = credit_nump_bank.collect()(0).getString(5)
              SMS_text_2 = SMS_text_2.replaceAll("<bn>", bank_name)
              Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_2, current_time, url, prop,sc)
              Base_class.log_nump_inactive(ucic, "1",sc)
            } else {
              var offer_id = credit_nump_bank.collect()(0).getInt(0)
              var campaign_id = credit_nump_bank.collect()(0).getInt(1)
              var delivery_date = credit_nump_bank.collect()(0).getString(3)

              var SMS_text_1 = credit_nump_bank.collect()(0).getString(4)
              SMS_text_1 = SMS_text_1.replaceAll("<bn>", bank_name)
              Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop,sc)
              Base_class.log_nump_inactive(ucic, "1",sc)
            }
          }
        }
      }
    }

    val b = 0;
    for (b <- 0 to (customer_inactive_count.toInt - 1)) {

      var ucic = customer_inactive.collect()(b).getString(0)
      var credit_nump_inactive = sqlContext
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "credit_nump_inactive", "keyspace" -> Config_file.keyspace))
        .load()

      var credit_nump_bank = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='Bank offer wall'")
      var credit_nump_spend = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='Spend offer wall'")
      credit_nump_inactive.registerTempTable("credit_nump_inactive")

      if (sqlContext.sql("select * from credit_nump_inactive where ucic='" + ucic + "'").rdd.count == 0) {
        var offer_id = credit_nump_bank.collect()(0).getInt(0)
        var campaign_id = credit_nump_bank.collect()(0).getInt(1)
        var delivery_date = credit_nump_bank.collect()(0).getString(3)
        var SMS_text_1 = credit_nump_bank.collect()(0).getString(4)
        SMS_text_1 = SMS_text_1.replaceAll("<bn>", bank_name)
        Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop,sc)

        Base_class.log_inactive(ucic, "1",sc)

      } else {
        val credit_nump_inactive2 = sqlContext
          .read
          .format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "credit_nump_inactive", "keyspace" -> Config_file.keyspace))
          .load()
        val credit_nump_inactive_count = credit_nump_inactive2.rdd.count
        for (i <- 0 to (credit_nump_inactive_count.toInt - 1)) {
          if (ucic == credit_nump_inactive2.collect()(i).getString(0)) {

            var inactive_cond_flag = credit_nump_inactive2.collect()(i).getString(1)
            if (inactive_cond_flag == "1") {
              var offer_id = credit_nump_spend.collect()(0).getInt(0)
              var campaign_id = credit_nump_spend.collect()(0).getInt(1)
              var delivery_date = credit_nump_spend.collect()(0).getString(3)
              var SMS_text_2 = credit_nump_spend.collect()(0).getString(5)
              SMS_text_2 = SMS_text_2.replaceAll("<bn>", bank_name)
              Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_2, current_time, url, prop,sc)
              Base_class.log_inactive(ucic, "2",sc)
            } else if (inactive_cond_flag == "2") {
              var offer_id = credit_nump_bank.collect()(0).getInt(0)
              var campaign_id = credit_nump_bank.collect()(0).getInt(1)
              var delivery_date = credit_nump_bank.collect()(0).getString(3)
              var SMS_text_2 = credit_nump_bank.collect()(0).getString(5)
              SMS_text_2 = SMS_text_2.replaceAll("<bn>", bank_name)
              Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_2, current_time, url, prop,sc)
              Base_class.log_inactive(ucic, "3",sc)
            } else {
              var offer_id = credit_nump_bank.collect()(0).getInt(0)
              var campaign_id = credit_nump_bank.collect()(0).getInt(1)
              var delivery_date = credit_nump_bank.collect()(0).getString(3)
              var SMS_text_1 = credit_nump_bank.collect()(0).getString(4)
              SMS_text_1 = SMS_text_1.replaceAll("<bn>", bank_name)
              Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop,sc)
              Base_class.log_inactive(ucic, "1",sc)

            }
          }
        }
      }
    }

    customer_inactive = customer.filter($"last_txn_date_debit" !== "NA")
    customer_nump = customer.filter($"last_txn_date_debit" === "NA")
    customer_inactive.registerTempTable("customer_inactive")
    customer_nump.registerTempTable("customer_nump")
    customer_inactive = sqlContext.sql("select ucic from customer_inactive where months_between(current_date(),last_txn_date_debit)>3") //filtering all inactive debit users 
    customer_nump_count = customer_nump.rdd.count
    customer_inactive_count = customer_inactive.rdd.count

    val j = 0;
    for (j <- 0 to (customer_nump_count.toInt - 1)) {

      var ucic = customer_nump.collect()(j).getString(0)
      var debit_nump_inactive = sqlContext
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "debit_nump_inactive", "keyspace" -> Config_file.keyspace))
        .load()
      var debit_trending_offer = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='Bank offer wall'")
      var debit_nump_spend = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='Spend offer wall'")
      var credit_nump_tax_saving = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from raw_offer where campaign_rules='NUNP Tax saving'")

      debit_nump_inactive.registerTempTable("debit_nump_inactive")
      if (sqlContext.sql("select * from debit_nump_inactive where ucic='" + ucic + "'").rdd.count == 0) {
      var offer_id = debit_trending_offer.collect()(0).getInt(0)
      var campaign_id = debit_trending_offer.collect()(0).getInt(1)
      var delivery_date = debit_trending_offer.collect()(0).getString(3)
      var SMS_text_1 = debit_trending_offer.collect()(0).getString(4)
        SMS_text_1 = SMS_text_1.replaceAll("<bn>", bank_name)
        Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop,sc)
        Base_class.log_nump_inactive_debit(ucic, "1",sc)
      } else {
        var k = 0
        val debit_nump_inactive2 = sqlContext
          .read
          .format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "debit_nump_inactive", "keyspace" -> Config_file.keyspace))
          .load()
        val debit_nump_inactive_count = debit_nump_inactive2.rdd.count
        for (k <- 0 to (debit_nump_inactive_count.toInt - 1)) {
          if (ucic == debit_nump_inactive2.collect()(k).getString(0)) {
            var nunp_cond_flag = debit_nump_inactive2.collect()(k).getString(2)
            if (nunp_cond_flag == "1") {
             var offer_id = debit_nump_spend.collect()(0).getInt(0)
             var campaign_id = debit_nump_spend.collect()(0).getInt(1)
             var delivery_date = debit_nump_spend.collect()(0).getString(3)
             var SMS_text_2 = debit_nump_spend.collect()(0).getString(5)
              SMS_text_2 = SMS_text_2.replaceAll("<bn>", bank_name)
              Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_2, current_time, url, prop,sc)
              Base_class.log_nump_inactive_debit(ucic, "2",sc)
            } else if (nunp_cond_flag == "2") {
              var offer_id = credit_nump_tax_saving.collect()(0).getInt(0)
              var campaign_id = credit_nump_tax_saving.collect()(0).getInt(1)
              var delivery_date = credit_nump_tax_saving.collect()(0).getString(3)
              var SMS_text_1 = credit_nump_tax_saving.collect()(0).getString(5)
              SMS_text_1 = SMS_text_1.replaceAll("<bn>", bank_name)
              Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop,sc)
              Base_class.log_nump_inactive_debit(ucic, "3",sc)
            } else if (nunp_cond_flag == "3") {
              var offer_id = debit_trending_offer.collect()(0).getInt(0)
              var campaign_id = debit_trending_offer.collect()(0).getInt(1)
              var delivery_date = debit_trending_offer.collect()(0).getString(3)
              var SMS_text_2 = debit_trending_offer.collect()(0).getString(5)
              SMS_text_2 = SMS_text_2.replaceAll("<bn>", bank_name)
              Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_2, current_time, url, prop,sc)
              Base_class.log_nump_inactive_debit(ucic, "1",sc)
            } else {
              var offer_id = debit_trending_offer.collect()(0).getInt(0)
              var campaign_id = debit_trending_offer.collect()(0).getInt(1)
              var delivery_date = debit_trending_offer.collect()(0).getString(3)
              var SMS_text_1 = debit_trending_offer.collect()(0).getString(4)
              SMS_text_1 = SMS_text_1.replaceAll("<bn>", bank_name)
              Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop,sc)
              Base_class.log_nump_inactive_debit(ucic, "1",sc)
            }
          }
        }
      }
    }

    val l = 0;
    for (l <- 0 to (customer_inactive_count.toInt - 1)) {

      var ucic = customer_inactive.collect()(l).getString(0)
      var debit_nump_inactive = sqlContext
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "debit_nump_inactive", "keyspace" -> Config_file.keyspace))
        .load()

      var debit_trending_offer = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from raw_offer where campaign_rules='Trending offer'") //filtering trending offer from offer_data
      debit_nump_inactive.registerTempTable("debit_nump_inactive")
      if (sqlContext.sql("select * from debit_nump_inactive where ucic='" + ucic + "'").rdd.count == 0) {
      
       var offer_id = debit_trending_offer.collect()(0).getInt(0)
       var campaign_id = debit_trending_offer.collect()(0).getInt(1)
       var delivery_date = current_time
       var SMS_text_1 = debit_trending_offer.collect()(0).getString(3)
        SMS_text_1 = SMS_text_1.replaceAll("<bn>", bank_name)

        Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop,sc)
        Base_class.log_nump_inactive_debit(ucic, "1",sc)
      } else {
       var debit_nump_inactive2 = sqlContext
          .read
          .format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "debit_nump_inactive", "keyspace" -> Config_file.keyspace))
          .load()
        var s = 0
        var debit_nump_inactive_count = debit_nump_inactive2.rdd.count
        for (s <- 0 to (debit_nump_inactive_count.toInt - 1)) {
          if (ucic == debit_nump_inactive2.collect()(s).getString(0)) {

            var inactive_cond_flag = debit_nump_inactive2.collect()(s).getString(1)
            if (inactive_cond_flag == "1") {
             var offer_id = debit_trending_offer.collect()(0).getInt(0)
             var campaign_id = debit_trending_offer.collect()(0).getInt(1)
             var delivery_date = debit_trending_offer.collect()(0).getString(3)

             var SMS_text_2 = debit_trending_offer.collect()(0).getString(5)
              SMS_text_2 = SMS_text_2.replaceAll("<bn>", bank_name)
              Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_2, current_time, url, prop,sc)
              Base_class.log_inactive_debit(ucic, "1",sc)
            } else {
             var offer_id = debit_trending_offer.collect()(0).getInt(0)
             var campaign_id = debit_trending_offer.collect()(0).getInt(1)
             var delivery_date = debit_trending_offer.collect()(0).getString(3)
             var SMS_text_1 = debit_trending_offer.collect()(0).getString(4)
              SMS_text_1 = SMS_text_1.replaceAll("<bn>", bank_name)
              Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop,sc)
              Base_class.log_inactive_debit(ucic, "1",sc)
            }
          }
        }
      }
    }
  }
}


