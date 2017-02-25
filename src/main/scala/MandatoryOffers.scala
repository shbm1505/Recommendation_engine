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
import Config_file._

object MandatoryOffers {
  def main(args: Array[String]) {


    val conf = new SparkConf(true).set("spark.cassandra.connection.host", Config_file.cassandra_ip)
    val sc = new SparkContext(conf)

    Class.forName("com.mysql.jdbc.Driver")

    val prop = new java.util.Properties    
    prop.setProperty("user", Config_file.mysql_username)
    prop.setProperty("password", Config_file.mysql_password)

    var bank_name = Config_file.bank_name
    var script_id=Config_file.script_id_mandatory

    val url = "jdbc:mysql://engage01.cfyrn3uyeiwt.us-west-2.rds.amazonaws.com:3306/engage_db?zeroDateTimeBehavior=convertToNull"

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    var month=""
    var year=""
    var day=""
    var current_time=""
    var offer_id = 0
    var campaign_id = 0
    var delivery_date = ""
    var SMS_text_1 = ""
    var SMS_text_2 = ""
    var offer_id2 = 0
    var campaign_id2 = 0
    var delivery_date2 = ""
    var offer_rotation_val = ""
    
    if(Config_file.env_variable.equals("Test"))
    {
      val time_simulator = sqlContext        //dataframe to store dates from time_simulator table
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "time_simulator", "keyspace" -> Config_file.keyspace))
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

    try {

    val first_date = udf((campaign_rule: String) => {
      month + "/" + day + "/" + year
    })

    val changing_name = udf((_1: String) => {
      _1
    })

    var calculating_date = udf((week_no: String, day_of_the_week: String, first_day: String, time: String) => {   //function to calculate delivery date
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
           Base_class.log_exception(ex,script_id,current_time,sc)
	}}

	var current_day=0;
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
        current_day=s;
      }
      if(week_no=="2")
      {
      current_day=s+7;
      }
      if(week_no=="3")
     { 
      current_day=s+14;}
      if(week_no=="4")
     { 
	current_day=s+21;
     }
	var t=time.substring(10,19)
	year+"-"+month+"-"+current_day+" "+t
    
    })

    val offer_data = sqlContext.read.jdbc(url, "offer_data", prop)
    var mandatory_offers = offer_data.filter($"campaign_type" === "mandatory")
    mandatory_offers = mandatory_offers.withColumn("first_date", first_date(mandatory_offers("campaign_rules")))
    mandatory_offers.registerTempTable("mandatory_offers")
    mandatory_offers = sqlContext.sql(
      """SELECT campaign_rules,offer_id,campaign_id,gender,city,interval_gap,cap_type,card_type,SMS_text_1,SMS_text_2,day_of_the_week,week_no,relationship_type,merchant_name,time,merchant_category,campaign_type,
        from_unixtime(unix_timestamp(first_date,"MM/dd/yyyy"), 'EEEEE') AS first_day
      FROM mandatory_offers""")
    
   mandatory_offers = mandatory_offers.withColumn("delivery_date", calculating_date(mandatory_offers("week_no"), mandatory_offers("day_of_the_week"), mandatory_offers("first_day"), mandatory_offers("time")))
   
   mandatory_offers.registerTempTable("mandatory_offers")

    var accounts_unblocked = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "account", "keyspace" -> Config_file.keyspace))
      .load()

    accounts_unblocked = accounts_unblocked.filter($"type" === "credit")  //filtering credit card users

    accounts_unblocked.registerTempTable("accounts_unblocked_credit")

    accounts_unblocked = sqlContext.sql("select type,ucic,account_no,add_on_card,card_type from accounts_unblocked_credit where ucic in ('RBL001796042','RBL001198253','RBL000002483','RBL000002555','RBL000002556','RBL000002557','RBL000002562','RBL000002563','RBL000002564','RBL000002566','RBL000002573','RBL000002578','RBL000002579','RBL000002580','RBL000002581','RBL000002583','RBL000002584','RBL000002586','RBL000002588','RBL000002589','RBL000002590','RBL000002593','RBL000002595','RBL000002596','RBL000002597','RBL000002598','RBL000002601','RBL000002605','RBL000002607','RBL000002608','RBL000002609','RBL000002615','RBL000002616','RBL000002618','RBL000002619','RBL000002622','RBL000002623','RBL000002625','RBL000002628','RBL000002633','RBL000002634','RBL000002635','RBL000002636','RBL000002637','RBL000002638','RBL000002639','RBL000002640','RBL000002641','RBL000002642','RBL000002644','RBL000002645','RBL000002658','RBL000002659','RBL000002661','RBL000002665','RBL000000148','RBL000000265','RBL000000309','RBL000000620','RBL000001963')")

    var accounts_count = accounts_unblocked.count()
    var mandatory_log = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "mandatory_log", "keyspace" -> Config_file.keyspace))
      .load()

    mandatory_log.registerTempTable("mandatory_log")
    var spend_offer = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from mandatory_offers where campaign_rules='Spend offer wall'")

    offer_id = spend_offer.collect()(0).getInt(0)
    campaign_id = spend_offer.collect()(0).getInt(1)
    delivery_date = spend_offer.collect()(0).getString(3)

    var bank_offer = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from mandatory_offers where campaign_rules='Bank offer wall'")

    var program_offer = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from mandatory_offers where campaign_rules='Program Update'")
    var i = 0;

    for (i <- 0 to (accounts_count.toInt - 1)) {

      var ucic = accounts_unblocked.collect()(i).getString(1)
      var card_types = "'" + accounts_unblocked.collect()(i).getString(4) + "'"

      var feature_offer = sqlContext.sql("select offer_id,campaign_id,time,delivery_date,SMS_text_1,SMS_text_2 from mandatory_offers where campaign_rules='Credit card Feature offer' and card_type=" + card_types)
      var count_mandatory_log = mandatory_log.filter($"ucic" === ucic).count()

      if (mandatory_log.filter($"ucic" === ucic).count() == 0) {

       SMS_text_1 = mandatory_offers.collect()(0).getString(4)
       SMS_text_1 = SMS_text_1.replace("<bn>", bank_name)
       Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop,sc)  //writing output message to mysql table

        offer_id2 = feature_offer.collect()(0).getInt(0)
        campaign_id2 = feature_offer.collect()(0).getInt(1)
        delivery_date2 = feature_offer.collect()(0).getString(3)
        SMS_text_2 = feature_offer.collect()(0).getString(5)
        SMS_text_2 = SMS_text_2.replace("<bn>", bank_name)

        Base_class.save_message(ucic, campaign_id2, delivery_date2, offer_id2, SMS_text_2, current_time, url, prop,sc)
        Base_class.log_mandatory(ucic, "1", "1",sc)  //maintaining mandatory log
      } else {

        var mandatory_logs = mandatory_log.filter($"ucic" === ucic)
        var sms_text_rotation = mandatory_logs.collect()(0).getString(2)
        var offer_rotation = mandatory_logs.collect()(0).getString(1)
        if (sms_text_rotation == "1") {
          SMS_text_1 = mandatory_offers.collect()(0).getString(5)
          SMS_text_1 = SMS_text_1.replace("<bn>", bank_name)
          Base_class.log_sms_text_rotation(ucic, "2",sc)
        } else {
          SMS_text_1 = mandatory_offers.collect()(0).getString(5)
          SMS_text_1 = SMS_text_1.replace("<bn>", bank_name)
          Base_class.log_sms_text_rotation(ucic, "1",sc)
        }
        Base_class.save_message(ucic, campaign_id, delivery_date, offer_id, SMS_text_1, current_time, url, prop,sc)
        if (offer_rotation == "1") {
          offer_id2 = bank_offer.collect()(0).getInt(0)
          campaign_id2 = bank_offer.collect()(0).getInt(1)
          delivery_date2 = bank_offer.collect()(0).getString(3)
          SMS_text_2 = bank_offer.collect()(0).getString(5)
          SMS_text_2 = SMS_text_2.replace("<bn>", bank_name)
          offer_rotation_val = "2"

        }
        if (offer_rotation == "2") {
          offer_id2 = program_offer.collect()(0).getInt(0)
          campaign_id2 = program_offer.collect()(0).getInt(1)
          delivery_date2 = program_offer.collect()(0).getString(3)
          SMS_text_2 = program_offer.collect()(0).getString(5)
          SMS_text_2 = SMS_text_2.replace("<bn>", bank_name)
          offer_rotation_val = "3"

        }
        if (offer_rotation == "3") {
          offer_id2 = feature_offer.collect()(0).getInt(0)
          campaign_id2 == feature_offer.collect()(0).getInt(1)
          delivery_date2 = feature_offer.collect()(0).getString(3)
          SMS_text_2 = feature_offer.collect()(0).getString(5)
          SMS_text_2 = SMS_text_2.replace("<bn>", bank_name)
          offer_rotation_val = "1"
        }
        Base_class.log_offer_rotation(ucic, offer_rotation_val,sc)
        Base_class.save_message(ucic, campaign_id2, delivery_date2, offer_id2, SMS_text_2, current_time, url, prop,sc)
      }

      var update_quota_df = sc.parallelize(Seq((ucic, "2", "2", "8", month, year))).toDF()      //maintaining user quota
      update_quota_df = update_quota_df.withColumn("ucic", changing_name(update_quota_df("_1")))
      update_quota_df = update_quota_df.withColumn("mandatory", changing_name(update_quota_df("_2")))
      update_quota_df = update_quota_df.withColumn("sms_sent", changing_name(update_quota_df("_3")))
      update_quota_df = update_quota_df.withColumn("month", changing_name(update_quota_df("_4")))
      update_quota_df = update_quota_df.withColumn("year", changing_name(update_quota_df("_5")))
      update_quota_df = update_quota_df.select("ucic", "mandatory", "sms_sent", "month", "year")
      update_quota_df.write.format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "user_quota", "keyspace" -> Config_file.keyspace))
        .mode(SaveMode.Append).save()
    }
  }
  catch {
        case ex: Exception => {

          Base_class.log_exception(ex,script_id,current_time,sc)

        }
      }
  }
}



