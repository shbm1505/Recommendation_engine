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

object Engine_rules {
  def main(args: Array[String]) {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", Config_file.cassandra_ip)

    val sc = new SparkContext(conf)

    Class.forName("com.mysql.jdbc.Driver")

    val prop = new java.util.Properties
    prop.setProperty("user", Config_file.mysql_username)
    prop.setProperty("password", Config_file.mysql_password)

    val url = Config_file.mysql_connection_string

    val changing_name = udf((_1: String) => {
      _1
    })

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    var month = ""
    var year = ""
    var day = ""
    var current_time = ""
    var script_id=Config_file.script_id_engine_rules
    if (Config_file.env_variable =="Test") {
      val time_simulator = sqlContext
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "time_simulator", "keyspace" -> Config_file.keyspace))
        .load()

      month = time_simulator.collect()(0).getString(2)
      year = time_simulator.collect()(0).getString(0)
      day = time_simulator.collect()(0).getString(1)
      current_time = year + "-" + month + "-" + day
    } else {
      val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      current_time = format.format(Calendar.getInstance().getTime())
      year = current_time.substring(0, 4)
      month = current_time.substring(5, 7)
      day = current_time.substring(8, 10)
    }
    try
    {
    var bank_name = Config_file.bank_name
    val user_quota = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "user_quota", "keyspace" -> Config_file.keyspace))
      .load()
    user_quota.registerTempTable("user_quota")

    val first_date = udf((offer_name: String) => {
      month + "/" + day + "/" + year
    })
    val calculating_date = udf((time: String) => {
      var t = time.substring(10, 19)
      year + "-" + month + "-" + day + " " + t
    })

    var offer_data = sqlContext.read.jdbc(url, "offer_data", prop)
    offer_data = offer_data.filter($"campaign_rules" === "Auto Insurance renewal" || $"campaign_rules" === "Health insurance renewal" || $"campaign_rules" === "Life insurance")

    offer_data = offer_data.withColumn("delivery_date", calculating_date(offer_data("time")))
    offer_data.registerTempTable("offer")
    var financial_profile = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "finalcial_profile2", "keyspace" -> Config_file.keyspace))
      .load()

    financial_profile.registerTempTable("financial_profile")
    val financial_profile_auto = financial_profile.filter($"date_of_txn_auto" !== "null")

    val financial_auto_count = financial_profile_auto.count()

    var auto_ucic = sqlContext.sql("select ucic from financial_profile where datediff(current_date(),date_of_txn_auto)=335 or datediff(current_date(),date_of_txn_auto)=358 ") //filtering all users having auto insurance
    var auto_offer = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='Auto Insurance renewal'") //filtering auto insurance offer

    auto_ucic.registerTempTable("auto_ucic")

    var auto_ucic_count = auto_ucic.count()

    val financial_log = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "financial_log", "keyspace" -> Config_file.keyspace))
      .load()
    val financial_count = financial_log.count()
    financial_log.registerTempTable("financial_log")
    var i = 0;
    for (i <- 0 to (auto_ucic_count.toInt - 1)) {

      var ucic = auto_ucic.collect()(i).getString(0)
      var soft_inc = sqlContext.sql("select soft_mandatory,sms_sent from user_quota where ucic='" + ucic + "'")
      var offer_id = auto_offer.collect()(0).getInt(0)
      var campaign_id = auto_offer.collect()(0).getInt(1)

      var delivery_date = auto_offer.collect()(0).getString(3)

      financial_log.registerTempTable("financial_log")

      if (financial_log.rdd.isEmpty()) {
        var SMS_text_1 = auto_offer.collect()(0).getString(4)
        SMS_text_1 = SMS_text_1.replace("<bn>", bank_name)

        Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop, sc)

        Base_class.log_financial_auto(ucic, "1", sc)
        Base_class.update_user_quota_soft_mandatory(soft_inc, ucic, sc)
      } else {
        if (financial_log.filter($"ucic" === ucic).count()==0) {

          var SMS_text_1 = auto_offer.collect()(0).getString(4)
          SMS_text_1 = SMS_text_1.replace("<bn>", bank_name)

          Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop, sc)

          Base_class.log_financial_auto(ucic, "1", sc)
          Base_class.update_user_quota_soft_mandatory(soft_inc, ucic, sc)
        } else {
          var j = 0;
          for (j <- 0 to (financial_count.toInt - 1)) {
            if (ucic == financial_log.collect()(j).getString(0)) {
              var auto_log = financial_log.collect()(j).getString(1)
              if (auto_log == "1") {
                var SMS_text_1 = auto_offer.collect()(0).getString(5)
                SMS_text_1 = SMS_text_1.replace("<bn>", bank_name)

                Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop, sc)

                Base_class.log_financial_auto(ucic, "2", sc)
                Base_class.update_user_quota_soft_mandatory(soft_inc, ucic, sc)
              } else {
                var SMS_text_2 = auto_offer.collect()(0).getString(5)
                SMS_text_2 = SMS_text_2.replace("<bn>", bank_name)

                Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_2, current_time, url, prop, sc)

                Base_class.log_financial_auto(ucic, "1", sc)
                Base_class.update_user_quota_soft_mandatory(soft_inc, ucic, sc)
              }
            }
          }
        }
      }
    }

    val financial_profile_health = financial_profile.filter($"date_of_txn_health" !== "null") //filtering all users having health insurance

    val financial_health_count = financial_profile_health.count()
    var health_offer = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='Health insurance renewal'") //filtering health insurance offer

    var health_ucic = sqlContext.sql("select ucic from financial_profile where datediff(current_date(),date_of_txn_health)=335 or datediff(current_date(),date_of_txn_health)=358 ")
    var health_ucic_count = health_ucic.count()
    var k = 0;
    for (k <- 0 to (health_ucic_count.toInt - 1)) {

      var ucic = health_ucic.collect()(k).getString(0)
      var soft_inc = sqlContext.sql("select soft_mandatory,sms_sent from user_quota where ucic='" + ucic + "'")

      var offer_id = health_offer.collect()(0).getInt(0)
      var campaign_id = health_offer.collect()(0).getInt(1)
      var delivery_date = health_offer.collect()(0).getString(3)

      if (financial_log.rdd.isEmpty()) {
        var SMS_text_1 = health_offer.collect()(0).getString(4)
        SMS_text_1 = SMS_text_1.replace("<bn>", bank_name)

        Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop, sc)

        Base_class.log_financial_health(ucic, "1", sc)
        Base_class.update_user_quota_soft_mandatory(soft_inc, ucic, sc)
      } else {
        if (financial_log.filter($"ucic" === ucic).count()==0) {
          var SMS_text_1 = health_offer.collect()(0).getString(4)
          SMS_text_1 = SMS_text_1.replace("<bn>", bank_name)

          Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop, sc)

          Base_class.log_financial_health(ucic, "1", sc)
          Base_class.update_user_quota_soft_mandatory(soft_inc, ucic, sc)
        } else {
          var j = 0;
          for (j <- 0 to (financial_count.toInt - 1)) {
            if (ucic == financial_log.collect()(j).getString(0)) {
              var health_log = financial_log.collect()(j).getString(2)
              if (health_log == "1") {
                var SMS_text_1 = health_offer.collect()(0).getString(4)
                SMS_text_1 = SMS_text_1.replace("<bn>", bank_name)

                Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop, sc)

                Base_class.log_financial_health(ucic, "2", sc)
                Base_class.update_user_quota_soft_mandatory(soft_inc, ucic, sc)
              } else {
                var SMS_text_2 = health_offer.collect()(0).getString(5)
                SMS_text_2 = SMS_text_2.replace("<bn>", bank_name)

                Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_2, current_time, url, prop, sc)

                Base_class.log_financial_health(ucic, "1", sc)
                Base_class.update_user_quota_soft_mandatory(soft_inc, ucic, sc)
              }
            }
          }

          val financial_profile_life = financial_profile.filter($"date_of_txn_life" !== "null")

          val financial_life_count = financial_profile_life.count()
          var life_offer = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='Life insurance'")
          val c = 0;

          var life_ucic = sqlContext.sql("select ucic from financial_profile where datediff(current_date(),date_of_txn_life)=335 or datediff(current_date(),date_of_txn_life)=358 ")
          var life_ucic_count = life_ucic.count()
          for (c <- 0 to (life_ucic_count.toInt - 1)) {

            var ucic = life_ucic.collect()(c).getString(0)
            var soft_inc = sqlContext.sql("select soft_mandatory,sms_sent from user_quota where ucic='" + ucic + "'")

            var offer_id = life_offer.collect()(0).getInt(0)
            var campaign_id = life_offer.collect()(0).getInt(1)
            var delivery_date = life_offer.collect()(0).getString(3)

            financial_log.registerTempTable("financial_log")

            if (financial_log.rdd.isEmpty()) {
              var SMS_text_1 = life_offer.collect()(0).getString(4)
              SMS_text_1 = SMS_text_1.replace("<bn>", bank_name)

              Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop, sc)

              Base_class.log_financial_life(ucic, "1", sc)
              Base_class.update_user_quota_soft_mandatory(soft_inc, ucic, sc)
            } else {
              if (financial_log.filter($"ucic" === ucic).count()==0) {
                var SMS_text_1 = life_offer.collect()(0).getString(4)
                SMS_text_1 = SMS_text_1.replace("<bn>", bank_name)

                Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop, sc)

                Base_class.log_financial_life(ucic, "1", sc)
                Base_class.update_user_quota_soft_mandatory(soft_inc, ucic, sc)
              } else {
                for (r <- 0 to (financial_count.toInt - 1)) {
                  if (ucic == financial_log.collect()(r).getString(0)) {
                    var life_log = financial_log.collect()(r).getString(3)
                    if (life_log == "1") {
                      var SMS_text_1 = life_offer.collect()(0).getString(4)
                      SMS_text_1 = SMS_text_1.replace("<bn>", bank_name)

                      Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop, sc)

                      Base_class.log_financial_life(ucic, "2", sc)
                      Base_class.update_user_quota_soft_mandatory(soft_inc, ucic, sc)
                    } else {
                      var SMS_text_2 = life_offer.collect()(0).getString(5)
                      SMS_text_2 = SMS_text_2.replace("<bn>", bank_name)

                      Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_2, current_time, url, prop, sc)

                      Base_class.log_financial_life(ucic, "1", sc)
                      Base_class.update_user_quota_soft_mandatory(soft_inc, ucic, sc)

                    }
                  }
                }
              }

            }
          }
        }
      }
    }
}
 catch {
        case ex: Exception => {

          Base_class.log_exception(ex,script_id,current_time,sc)

        }
      }

  }
}


