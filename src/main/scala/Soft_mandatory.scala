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

object Soft_mandatory {
  def main(args: Array[String]) {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", Config_file.cassandra_ip)

    val sc = new SparkContext(conf)

    Class.forName("com.mysql.jdbc.Driver")

    val prop = new java.util.Properties
    prop.setProperty("user", Config_file.mysql_username)
    prop.setProperty("password", Config_file.mysql_password)

    val url = Config_file.mysql_connection_string

    var bank_name = Config_file.bank_name
    var script_id=Config_file.script_id_soft_mandatory

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    var month=""
    var day=""
    var year=""
    var current_time=""
    if(Config_file.env_variable.equals("Test"))
    {
    val time_simulator = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "time_simulator", "keyspace" -> "goals101"))
      .load()

    month = time_simulator.collect()(0).getString(2)
    year = time_simulator.collect()(0).getString(0)
    day = time_simulator.collect()(0).getString(1)
    current_time = year + "-" + month + "-" + day
    }
     else
    {
      val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      current_time=format.format(Calendar.getInstance().getTime())
      year=current_time.substring(0,4)
      month=current_time.substring(5,7)
      day=current_time.substring(8,10)
    }
    var cash_loan_month = "[02,04,06,08,10,12]"
    var limit_enhancement_month = "[01,04,07,11]"
    var card_insurance_month = "[06,12]"
    try
     {
var calculating_date = udf((week_no: String, day_of_the_week: String, first_day: String, time: String) => {     
  var z = new Array[String](10)
  var arr_days = new Array[String](10)

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
         case ex:Exception=>{

   }}

    var e=0;
    var s=0;
      for(l<-c to 6)
      {
         arr_days(g)=arr(l)
    g=g+1;
      }
      for(i<-0 to c-1)
      {
         arr_days(g)=z(i)
         g=g+1
      }

      for(j<-0 to 6)
      {
         if(arr_days(j)==day_of_the_week)
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



    val user_quota = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "user_quota", "keyspace" -> "goals101"))
      .load()

    user_quota.registerTempTable("user_quota")
    val first_date = udf((offer_name: String) => {
      month + "/" + day + "/" + year
    })

    val changing_name = udf((_1: String) => {
      _1
    })

    var offer_data = sqlContext.read.jdbc(url, "offer_data", prop)

    offer_data = offer_data.filter($"campaign_rules" === "Limit enhancement" || $"campaign_rules" === "Cash loan" || $"campaign_rules" === "Card insurance") //filtering soft mandatory offers
    offer_data = offer_data.withColumn("first_date", first_date(offer_data("campaign_rules")))
    offer_data.registerTempTable("offer_data")
    offer_data = sqlContext.sql(
      """SELECT campaign_rules,offer_id,campaign_id,gender,city,interval_gap,cap_type,card_type,SMS_text_1,SMS_text_2,day_of_the_week,week_no,relationship_type,merchant_name,time,merchant_category,campaign_type,
        from_unixtime(unix_timestamp(first_date,"MM/dd/yyyy"), 'EEEEE') AS first_day
      FROM offer_data""")
    offer_data = offer_data.withColumn("delivery_date", calculating_date(offer_data("week_no"), offer_data("day_of_the_week"), offer_data("first_day"), offer_data("time")))
    offer_data.registerTempTable("offer")
    var limit_enhancement_offer = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='Limit enhancement' limit 1") //filtering limit enhancement offer
    var cash_loan_offer = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='Cash loan'")
    var card_insurance_offer = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from offer where campaign_rules='Card insurance'")
        
    val cash_loan = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "cash_loan", "keyspace" -> "goals101"))
      .load()

    val card_insurance = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "card_insurance", "keyspace" -> "goals101"))
      .load()

    val limit_enhancement = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "limit_enhancement", "keyspace" -> "goals101"))
      .load()

    cash_loan.registerTempTable("cash_loan")
    limit_enhancement.registerTempTable("limit_enhancement")
    card_insurance.registerTempTable("card_insurance")
    var soft_mandatory_log = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
      .load()
    var soft_mandatory_count = soft_mandatory_log.count()
    if (cash_loan_month.contains(month)) {
      var cash_loan_count = cash_loan.count()

      var i = 0
      for (i <- 0 to (cash_loan_count.toInt - 1)) {

        var ucic = cash_loan.collect()(i).getString(0)
        
        var amount = cash_loan.collect()(i).getString(1)
        var interest = cash_loan.collect()(i).getString(2)
        var soft_inc = sqlContext.sql("select soft_mandatory,sms_sent from user_quota where ucic='" + ucic + "'")

        var offer_id = cash_loan_offer.collect()(0).getInt(0)
        var campaign_id = cash_loan_offer.collect()(0).getInt(1)

        var delivery_date = cash_loan_offer.collect()(0).getString(3)

        soft_mandatory_log.registerTempTable("soft_mandatory_log")

        if (sqlContext.sql("select * from soft_mandatory_log where ucic='" + ucic + "'").count() == 0) {
          var SMS_text_1 = cash_loan_offer.collect()(0).getString(4)
          SMS_text_1 = SMS_text_1.replaceAll("<bn>", bank_name)
          SMS_text_1 = SMS_text_1.replaceAll("<pql>", amount)
          SMS_text_1 = SMS_text_1.replaceAll("<ir>", interest)
          Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, delivery_date, url, prop, sc)
          Base_class.log_cash_loan(ucic, "1", sc)

        } else {
          var j = 0
          for (j <- 0 to (soft_mandatory_count.toInt - 1)) {
            if (ucic == soft_mandatory_log.collect()(j).getString(0)) {
              var card_insurance_log = soft_mandatory_log.collect()(j).getString(2)
              if (card_insurance_log == "1") {
                var SMS_text_2 = cash_loan_offer.collect()(0).getString(5)
                SMS_text_2 = SMS_text_2.replaceAll("<bn>", bank_name)
                SMS_text_2 = SMS_text_2.replaceAll("<pql>", amount)
                SMS_text_2 = SMS_text_2.replaceAll("<ir>", interest)
                Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_2, current_time, url, prop, sc)

                Base_class.log_cash_loan(ucic, "2", sc)

              } else {
                var SMS_text_1 = cash_loan_offer.collect()(0).getString(4)
                SMS_text_1 = SMS_text_1.replaceAll("<bn>", bank_name)
                SMS_text_1 = SMS_text_1.replaceAll("<pql>", amount)
                SMS_text_1 = SMS_text_1.replaceAll("<ir>", interest)
                Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop, sc)
                Base_class.log_cash_loan(ucic, "1", sc)

              }
            }
          }
        }
        
  Base_class.update_user_quota_soft_mandatory(soft_inc, ucic, sc)
      }
    }

    if (card_insurance_month.contains(month)) {
      var card_insurance_count = cash_loan.count()
      var j = 0
      for (j <- 0 to (card_insurance_count.toInt - 1)) {


        var ucic = card_insurance.collect()(j).getString(0)
        var flag = card_insurance.collect()(j).getString(1)
        var soft_inc = sqlContext.sql("select soft_mandatory,sms_sent from user_quota where ucic='" + ucic + "'")
        var offer_id = card_insurance_offer.collect()(0).getInt(0)
        var campaign_id = card_insurance_offer.collect()(0).getInt(1)
        var delivery_date = card_insurance_offer.collect()(0).getString(3)

        soft_mandatory_log.registerTempTable("soft_mandatory_log")

        if (sqlContext.sql("select * from soft_mandatory_log where ucic='" + ucic + "'").count() == 0) {
          var SMS_text_1 = card_insurance_offer.collect()(0).getString(4)
          SMS_text_1 = SMS_text_1.replaceAll("<bn>", bank_name)
          Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop, sc)

          Base_class.log_card_insurance(ucic, "1", sc)

        } else {
          var j = 0
          for (j <- 0 to (soft_mandatory_count.toInt - 1)) {
            if (ucic == soft_mandatory_log.collect()(j).getString(0)) {
              var card_insurance_log = soft_mandatory_log.collect()(j).getString(1)
              if (card_insurance_log == "1") {
                var SMS_text_2 = card_insurance_offer.collect()(0).getString(5)
                SMS_text_2 = SMS_text_2.replaceAll("<bn>", bank_name)

                Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_2, current_time, url, prop, sc)

                Base_class.log_card_insurance(ucic, "2", sc)

              } else {
                var SMS_text_1 = card_insurance_offer.collect()(0).getString(4)
                SMS_text_1 = SMS_text_1.replaceAll("<bn>", bank_name)

                Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop, sc)

                Base_class.log_card_insurance(ucic, "1", sc)

              }
            }
          }
        }
        Base_class.update_user_quota_soft_mandatory(soft_inc, ucic, sc)
      }
    }

    if (limit_enhancement_month.contains(month)) {

      var limit_enhancement_count = limit_enhancement.count()
      var k = 0
      for (k <- 0 to (limit_enhancement_count.toInt - 1)) {

        var ucic = limit_enhancement.collect()(k).getString(0)
        var amount = limit_enhancement.collect()(k).getString(1)
   
        var offer_id = limit_enhancement_offer.collect()(0).getInt(0)
        var campaign_id = limit_enhancement_offer.collect()(0).getInt(1)

        var delivery_date = limit_enhancement_offer.collect()(0).getString(3)

        soft_mandatory_log.registerTempTable("soft_mandatory_log")

        if (sqlContext.sql("select * from soft_mandatory_log where ucic='" + ucic + "'").count() == 0) {
          var SMS_text_1 = limit_enhancement_offer.collect()(0).getString(4)

          SMS_text_1 = SMS_text_1.replaceAll("<bn>", bank_name)
          SMS_text_1 = SMS_text_1.replaceAll("<lef>", amount)

          Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop, sc)

          Base_class.log_limit_enhancement(ucic, "1", sc)

        } else {
          var k = 0
          for (k <- 0 to (soft_mandatory_count.toInt - 1)) {
            if (ucic == soft_mandatory_log.collect()(k).getString(0)) {
              var limit_enhancement_log = soft_mandatory_log.collect()(k).getString(3)
              if (limit_enhancement_log == "1") {
                var SMS_text_2 = limit_enhancement_offer.collect()(0).getString(5)
                SMS_text_2 = SMS_text_2.replaceAll("<bn>", bank_name)
                SMS_text_2 = SMS_text_2.replaceAll("<lef>", amount)

                Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_2, current_time, url, prop, sc)

                Base_class.log_limit_enhancement(ucic, "2", sc)

              } else {
                var SMS_text_1 = limit_enhancement_offer.collect()(0).getString(4)
                SMS_text_1 = SMS_text_1.replaceAll("<bn>", bank_name)
                SMS_text_1 = SMS_text_1.replaceAll("<lef>", amount)
                Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop, sc)

                Base_class.log_limit_enhancement(ucic, "1", sc)

              }
            }
          }
        }
        var soft_inc = sqlContext.sql("select soft_mandatory,sms_sent from user_quota where ucic='" + ucic + "'")

        Base_class.update_user_quota_soft_mandatory(soft_inc, ucic, sc)
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



