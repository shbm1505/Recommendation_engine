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
import java.io.Serializable

 object Base_class {

//class Base_class extends java.io.Serializable 
val changing_name = udf((_1: String)=> {
        _1
  })



def save_message(ucic: String, campaign_id: Int, delivery_date: String, offer_id: Int, SMS_text_1: String, current_time: String, url: String, prop: java.util.Properties, sc: SparkContext): Unit = {
      
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      var final_df = sc.parallelize(Seq((0, ucic, campaign_id, delivery_date, offer_id, "null", SMS_text_1, 0, current_time))).toDF()
      final_df = final_df.withColumn("msg_system_id", changing_name(final_df("_1")))
      final_df = final_df.withColumn("customer_code", changing_name(final_df("_2")))
      final_df = final_df.withColumn("campaign_id", changing_name(final_df("_3")))
      final_df = final_df.withColumn("delivery_date", changing_name(final_df("_4")))
      final_df = final_df.withColumn("offer_id", changing_name(final_df("_5")))
      final_df = final_df.withColumn("secondary_offers", changing_name(final_df("_6")))
      final_df = final_df.withColumn("message_text", changing_name(final_df("_7")))
      final_df = final_df.withColumn("status", changing_name(final_df("_8")))
      final_df = final_df.withColumn("created_on", changing_name(final_df("_9")))
      final_df = final_df.select("msg_system_id", "customer_code", "campaign_id", "delivery_date", "offer_id", "secondary_offers", "message_text", "status", "created_on")
      final_df.write.mode("append").jdbc(url, "message_to_be_sent_system", prop)

    }

def log_cash_loan(ucic:String,cash_loan:String,sc: SparkContext) : Unit = {
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._

        var log_cash_loan_df = sc.parallelize(Seq((ucic,cash_loan))).toDF()
        log_cash_loan_df=log_cash_loan_df.withColumn("ucic",changing_name(log_cash_loan_df("_1")))
        log_cash_loan_df=log_cash_loan_df.withColumn("cash_loan",changing_name(log_cash_loan_df("_2")))
        log_cash_loan_df=log_cash_loan_df.select("ucic","cash_loan")
        log_cash_loan_df.write .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
        .mode(SaveMode.Append).save()
          
       }

def log_limit_enhancement(ucic:String,limit_enhancement:String,sc: SparkContext) : Unit = {
        
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._

        var log_limit_enhancement_df = sc.parallelize(Seq((ucic,limit_enhancement))).toDF()
        log_limit_enhancement_df=log_limit_enhancement_df.withColumn("ucic",changing_name(log_limit_enhancement_df("_1")))
        log_limit_enhancement_df=log_limit_enhancement_df.withColumn("limit_enhancement",changing_name(log_limit_enhancement_df("_2")))
        log_limit_enhancement_df=log_limit_enhancement_df.select("ucic","limit_enhancement")
        log_limit_enhancement_df.write .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
        .mode(SaveMode.Append).save()
          
       }
def log_card_insurance(ucic:String,card_insurance:String,sc: SparkContext) : Unit = {
        
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._
        var log_card_insurance_df = sc.parallelize(Seq((ucic,card_insurance))).toDF()
        log_card_insurance_df=log_card_insurance_df.withColumn("ucic",changing_name(log_card_insurance_df("_1")))
        log_card_insurance_df=log_card_insurance_df.withColumn("card_insurance",changing_name(log_card_insurance_df("_2")))
        log_card_insurance_df=log_card_insurance_df.select("ucic","card_insurance")
        log_card_insurance_df.write .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
        .mode(SaveMode.Append).save()
          
       }

def user_quota_new(ucic:String,sc: SparkContext) : Unit = {

        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._
        var log_card_insurance_df = sc.parallelize(Seq((ucic,1,1))).toDF()
        log_card_insurance_df=log_card_insurance_df.withColumn("ucic",changing_name(log_card_insurance_df("_1")))
        log_card_insurance_df=log_card_insurance_df.withColumn("soft_mandatory",changing_name(log_card_insurance_df("_2")))
        log_card_insurance_df=log_card_insurance_df.withColumn("sms_sent",changing_name(log_card_insurance_df("_3")))
        log_card_insurance_df=log_card_insurance_df.select("ucic","soft_mandatory","sms_sent")
        log_card_insurance_df.write .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> "soft_mandatory_log", "keyspace" -> "goals101"))
        .mode(SaveMode.Append).save()

       }


def save_message_auto(ucic: String, campaign_id: Int, delivery_date: String, offer_id: Int, SMS_text_1: String, current_time: String, url: String, prop: java.util.Properties, secondary_offers: String,sc:SparkContext): Unit = {
      
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      var final_df = sc.parallelize(Seq((0, ucic, campaign_id, delivery_date, offer_id,secondary_offers, SMS_text_1, 0, current_time))).toDF()
      final_df = final_df.withColumn("msg_system_id", changing_name(final_df("_1")))
      final_df = final_df.withColumn("customer_code", changing_name(final_df("_2")))
      final_df = final_df.withColumn("campaign_id", changing_name(final_df("_3")))
      final_df = final_df.withColumn("delivery_date", changing_name(final_df("_4")))
      final_df = final_df.withColumn("offer_id", changing_name(final_df("_5")))
      final_df = final_df.withColumn("secondary_offers", changing_name(final_df("_6")))
      final_df = final_df.withColumn("message_text", changing_name(final_df("_7")))
      final_df = final_df.withColumn("status", changing_name(final_df("_8")))
      final_df = final_df.withColumn("created_on", changing_name(final_df("_9")))
      final_df = final_df.select("msg_system_id", "customer_code", "campaign_id", "delivery_date", "offer_id", "secondary_offers", "message_text", "status", "created_on")
      final_df.write.mode("append").jdbc(url, "message_to_be_sent_system", prop)

    }

def update_time_function(year:String,day:String,sc:SparkContext) : Unit = {

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  var time_update_df = sc.parallelize(Seq((year,day))).toDF
  time_update_df=time_update_df.withColumn("year",changing_name(time_update_df("_1")))
  time_update_df=time_update_df.withColumn("week_day",changing_name(time_update_df("_2")))
  time_update_df=time_update_df.select("year","week_day")
  time_update_df.write .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "time_simulator", "keyspace" -> "goals101"))
  .mode(SaveMode.Append).save()

  
}

def log_nump_inactive(ucic: String, nump_cond: String,sc:SparkContext): Unit = {
      
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._      
      var nunp_df = sc.parallelize(Seq((ucic, nump_cond))).toDF()
      nunp_df = nunp_df.withColumn("ucic", changing_name(nunp_df("_1")))
      nunp_df = nunp_df.withColumn("nump_cond", changing_name(nunp_df("_2")))
      nunp_df = nunp_df.select("ucic", "nump_cond")
      nunp_df.write.format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "credit_nump_inactive", "keyspace" -> "goals101"))
        .mode(SaveMode.Append).save()
    }

def log_inactive(ucic: String, inactive_cond: String,sc:SparkContext): Unit = {
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._      
    var  inactive_df = sc.parallelize(Seq((ucic, inactive_cond))).toDF()
      inactive_df = inactive_df.withColumn("ucic", changing_name(inactive_df("_1")))
      inactive_df = inactive_df.withColumn("inactive_cond", changing_name(inactive_df("_2")))
      inactive_df = inactive_df.select("ucic", "inactive_cond")
      inactive_df.write.format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "credit_nump_inactive", "keyspace" -> "goals101"))
        .mode(SaveMode.Append).save()
    }

def log_nump_inactive_debit(ucic: String, nump_cond: String,sc:SparkContext): Unit = {
      
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._      
      var nunp_df = sc.parallelize(Seq((ucic, nump_cond))).toDF()
      nunp_df = nunp_df.withColumn("ucic", changing_name(nunp_df("_1")))
      nunp_df = nunp_df.withColumn("nump_cond", changing_name(nunp_df("_2")))
      nunp_df = nunp_df.select("ucic", "nump_cond")
      nunp_df.write.format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "debit_nump_inactive", "keyspace" -> "goals101"))
        .mode(SaveMode.Append).save()
    }

def log_inactive_debit(ucic: String, inactive_cond: String,sc:SparkContext): Unit = {
      
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._      
      var inactive_df = sc.parallelize(Seq((ucic, inactive_cond))).toDF()
      inactive_df = inactive_df.withColumn("ucic", changing_name(inactive_df("_1")))
      inactive_df = inactive_df.withColumn("inactive_cond", changing_name(inactive_df("_2")))
      inactive_df = inactive_df.select("ucic", "inactive_cond")
      inactive_df.write.format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "debit_nump_inactive", "keyspace" -> "goals101"))
        .mode(SaveMode.Append).save()
    }

def log_financial_auto(ucic: String, auto: String,sc:SparkContext): Unit = {
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    var financial_auto_df = sc.parallelize(Seq((ucic,auto))).toDF()
    financial_auto_df=financial_auto_df.withColumn("ucic",changing_name(financial_auto_df("_1")))
    financial_auto_df=financial_auto_df.withColumn("auto",changing_name(financial_auto_df("_2")))
    financial_auto_df=financial_auto_df.select("ucic","auto")
    financial_auto_df.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "financial_log", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()

    }
  

def log_financial_health(ucic: String, health: String,sc:SparkContext): Unit = {
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    var financial_health_df = sc.parallelize(Seq((ucic,health))).toDF()
    financial_health_df=financial_health_df.withColumn("ucic",changing_name(financial_health_df("_1")))
    financial_health_df=financial_health_df.withColumn("health",changing_name(financial_health_df("_2")))
    financial_health_df=financial_health_df.select("ucic","health")
    financial_health_df.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "financial_log", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
    
    }
def log_financial_life(ucic: String, life: String,sc:SparkContext): Unit = {
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    var financial_life_df = sc.parallelize(Seq((ucic,life))).toDF()
    financial_life_df=financial_life_df.withColumn("ucic",changing_name(financial_life_df("_1")))
    financial_life_df=financial_life_df.withColumn("life",changing_name(financial_life_df("_2")))
    financial_life_df=financial_life_df.select("ucic","life")
    financial_life_df.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "financial_log", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()
    
    }

def calculating_quota(ucic:String,total_sms:String,month:String,year: String,sc:SparkContext) : Unit = {
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    var quota = sc.parallelize(Seq((ucic,total_sms,month,year))).toDF()
    quota=quota.withColumn("ucic",changing_name(quota("_1")))
    quota=quota.withColumn("total_sms",changing_name(quota("_2")))
    quota=quota.withColumn("month",changing_name(quota("_3")))
    quota=quota.withColumn("year",changing_name(quota("_4")))
    quota=quota.select("ucic","total_sms","month","year")
      quota.write .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
    .mode(SaveMode.Append).save()

       }


def log_exception(error: Exception,script_id: String,current_time: String,sc: SparkContext): Unit = {

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  var log_exception_df = sc.parallelize(Seq((error,script_id,current_time))).toDF
  log_exception_df=log_exception_df.withColumn("error",changing_name(log_exception_df("_1")))
  log_exception_df=log_exception_df.withColumn("script_id",changing_name(log_exception_df("_2")))
  log_exception_df=log_exception_df.withColumn("current_time",changing_name(log_exception_df("_2")))
  log_exception_df=log_exception_df.select("ucic","offer_id")
  log_exception_df.write .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "exception_log", "keyspace" -> "goals101"))
  .mode(SaveMode.Append).save()

    }

def log_user_offer(ucic: String, offer_id: String,sc:SparkContext): Unit = {
  
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  var user_offer_df = sc.parallelize(Seq((ucic,offer_id))).toDF
  user_offer_df=user_offer_df.withColumn("ucic",changing_name(user_offer_df("_1")))
  user_offer_df=user_offer_df.withColumn("offer_id",changing_name(user_offer_df("_2")))
  user_offer_df=user_offer_df.select("ucic","offer_id")
  user_offer_df.write .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "user_offer_log", "keyspace" -> "goals101"))
  .mode(SaveMode.Append).save()
  
  }


def log_mandatory(ucic: String, offer_rotation: String, sms_text_rotation: String,sc:SparkContext): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    var mandatory_df = sc.parallelize(Seq((ucic, offer_rotation, sms_text_rotation))).toDF()
    mandatory_df = mandatory_df.withColumn("ucic", changing_name(mandatory_df("_1")))
    mandatory_df = mandatory_df.withColumn("offer_rotation", changing_name(mandatory_df("_2")))
    mandatory_df = mandatory_df.withColumn("sms_text_rotation", changing_name(mandatory_df("_3")))
    mandatory_df = mandatory_df.select("ucic", "offer_rotation", "sms_text_rotation")
    mandatory_df.write.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "mandatory_log", "keyspace" -> "goals101"))
      .mode(SaveMode.Append).save()

    }

def log_sms_text_rotation(ucic: String, sms_text_rotation: String,sc:SparkContext): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._      
    var mandatory_df = sc.parallelize(Seq((ucic, sms_text_rotation))).toDF()
    mandatory_df = mandatory_df.withColumn("ucic", changing_name(mandatory_df("_1")))
    mandatory_df = mandatory_df.withColumn("sms_text_rotation", changing_name(mandatory_df("_2")))
    mandatory_df = mandatory_df.select("ucic", "sms_text_rotation")
    mandatory_df.write.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "mandatory_log", "keyspace" -> "goals101"))
      .mode(SaveMode.Append).save()
    }

def log_offer_rotation(ucic: String, offer_rotation: String,sc:SparkContext): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    var mandatory_df = sc.parallelize(Seq((ucic, offer_rotation))).toDF()
    mandatory_df = mandatory_df.withColumn("ucic", changing_name(mandatory_df("_1")))
    mandatory_df = mandatory_df.withColumn("offer_rotation", changing_name(mandatory_df("_2")))
    mandatory_df = mandatory_df.select("ucic", "offer_rotation")
    mandatory_df.write.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "mandatory_log", "keyspace" -> "goals101"))
      .mode(SaveMode.Append).save()
    }

  
def update_user_quota_soft_mandatory(df: org.apache.spark.sql.DataFrame,ucic: String, sc: SparkContext): Unit = {

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  var quota_incs=""
      try{
  var quota_inc=df.collect()(0).getString(0)
  quota_incs=quota_inc
  var sms_sent=df.collect()(0).getString(1)
        if(!quota_inc.equals("null"))
            {
    var quota_inc2=quota_inc.toInt + 1
    
    var sms_sent2=sms_sent.toInt + 1
    var soft_mandatory_df = sc.parallelize(Seq((ucic,quota_inc2,sms_sent2))).toDF()
      soft_mandatory_df=soft_mandatory_df.withColumn("ucic",changing_name(soft_mandatory_df("_1")))
      soft_mandatory_df=soft_mandatory_df.withColumn("soft_mandatory",changing_name(soft_mandatory_df("_2")))
      soft_mandatory_df=soft_mandatory_df.withColumn("sms_sent",changing_name(soft_mandatory_df("_3")))
      soft_mandatory_df=soft_mandatory_df.select("ucic","soft_mandatory","sms_sent")
      soft_mandatory_df.write .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
      .mode(SaveMode.Append).save()
              }
  }
  catch{
    case x:Exception=>{
    
        var sms_sents=0
  try{

  var sms_sent=df.collect()(0).getInt(1)
  sms_sents=sms_sent
     }
  catch{
    case y:Exception=>{

    var soft_mandatory_df = sc.parallelize(Seq((ucic,"1","1"))).toDF()
      soft_mandatory_df=soft_mandatory_df.withColumn("ucic",changing_name(soft_mandatory_df("_1")))
      soft_mandatory_df=soft_mandatory_df.withColumn("soft_mandatory",changing_name(soft_mandatory_df("_2")))
      soft_mandatory_df=soft_mandatory_df.withColumn("sms_sent",changing_name(soft_mandatory_df("_3")))
      soft_mandatory_df=soft_mandatory_df.select("ucic","soft_mandatory","sms_sent")
      soft_mandatory_df.write .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
      .mode(SaveMode.Append).save()
  }
  }
    var sms_sent2=sms_sents.toInt + 1
    var soft_mandatory_df = sc.parallelize(Seq((ucic,"1",sms_sent2))).toDF()
      soft_mandatory_df=soft_mandatory_df.withColumn("ucic",changing_name(soft_mandatory_df("_1")))
      soft_mandatory_df=soft_mandatory_df.withColumn("soft_mandatory",changing_name(soft_mandatory_df("_2")))
      soft_mandatory_df=soft_mandatory_df.withColumn("sms_sent",changing_name(soft_mandatory_df("_3")))
      soft_mandatory_df=soft_mandatory_df.select("ucic","soft_mandatory","sms_sent")
      soft_mandatory_df.write .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
      .mode(SaveMode.Append).save()
  }}
  }

def update_user_quota_auto(df: org.apache.spark.sql.DataFrame,ucic: String, sc: SparkContext): Unit = {

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  var quota_incs=""
      try{
  	var quota_inc=df.collect()(0).getString(0)
  	quota_incs=quota_inc
  	var sms_sent=df.collect()(0).getString(1)
        if(!quota_inc.equals("null"))
            {
    		var quota_inc2=quota_inc + 1
    
    		var sms_sent2=sms_sent + 1
    		var auto_reco_df = sc.parallelize(Seq((ucic,quota_inc2,sms_sent2))).toDF()
      		auto_reco_df=auto_reco_df.withColumn("ucic",changing_name(auto_reco_df("_1")))
      		auto_reco_df=auto_reco_df.withColumn("auto_reco",changing_name(auto_reco_df("_2")))
      		auto_reco_df=auto_reco_df.withColumn("sms_sent",changing_name(auto_reco_df("_3")))
      		auto_reco_df=auto_reco_df.select("ucic","auto_reco","sms_sent")
      		auto_reco_df.write .format("org.apache.spark.sql.cassandra")
      		.options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
      		.mode(SaveMode.Append).save()
            }
       }
  	catch{
    		case x:Exception=>{
       		var sms_sents=0
  		try{

  			var sms_sent=df.collect()(0).getInt(1)
  			sms_sents=sms_sent
     		   }
  		catch{
    			case y:Exception=>{

    			var auto_reco_df = sc.parallelize(Seq((ucic,"1","1"))).toDF()
      			auto_reco_df=auto_reco_df.withColumn("ucic",changing_name(auto_reco_df("_1")))
      			auto_reco_df=auto_reco_df.withColumn("auto_reco",changing_name(auto_reco_df("_2")))
      			auto_reco_df=auto_reco_df.withColumn("sms_sent",changing_name(auto_reco_df("_3")))
      			auto_reco_df=auto_reco_df.select("ucic","auto_reco","sms_sent")
      			auto_reco_df.write .format("org.apache.spark.sql.cassandra")
      			.options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
      			.mode(SaveMode.Append).save()
  				}
 			 }
    	var sms_sent2=sms_sents.toInt + 1
    	var auto_reco_df = sc.parallelize(Seq((ucic,"1",sms_sent2))).toDF()
      	auto_reco_df=auto_reco_df.withColumn("ucic",changing_name(auto_reco_df("_1")))
      	auto_reco_df=auto_reco_df.withColumn("auto_reco",changing_name(auto_reco_df("_2")))
      	auto_reco_df=auto_reco_df.withColumn("sms_sent",changing_name(auto_reco_df("_3")))
      	auto_reco_df=auto_reco_df.select("ucic","auto_reco","sms_sent")
      	auto_reco_df.write .format("org.apache.spark.sql.cassandra")
      	.options(Map( "table" -> "user_quota", "keyspace" -> "goals101"))
      	.mode(SaveMode.Append).save()
    
  }}
  }


}

