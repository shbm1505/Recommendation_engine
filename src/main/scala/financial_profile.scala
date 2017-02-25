import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, DateType };
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ DataType, StringType, DoubleType }
import org.apache.spark.sql.functions._

object financial_profile {

  def main(args: Array[String]) {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val transactionDF = sqlContext.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "transaction", "keyspace" -> "goals101"))
      .load()

    val accountDF = sqlContext.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "account", "keyspace" -> "goals101"))
      .load()

    val financialDF = sqlContext.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "financial_profile3", "keyspace" -> "goals101"))
      .load() 

    financialDF.registerTempTable("financialDF")   

    val txn_ucic = transactionDF.join(accountDF, transactionDF("account_no") === accountDF("account_no"))
    txn_ucic.registerTempTable("txn_ucic")




    val txn_ucic_only = txn_ucic.select($"ucic").dropDuplicates()

    txn_ucic_only.registerTempTable("txn_ucic_only")

    //val ucic_c = sqlContext.sql("select count(*) from txn_ucic_only")

    //ucic_c.show()
    //txn_ucic_only.show()


    //LIFE INSURANCE

    val life_ins_txn = txn_ucic.filter($"cleaned_merchant_name".contains("life ins"))
    //life_ins_txn.show()

    val life_ins_data_naming = life_ins_txn.select($"ucic" as "ucic_life", $"amount" as "amount_life",$"date_of_txn" as "date_of_txn_life")
    //life_ins_data.show()
    
    
    val life_ins_data = txn_ucic_only.join(life_ins_data_naming, txn_ucic_only("ucic") === life_ins_data_naming("ucic_life"),"left").select($"ucic",$"amount_life",$"date_of_txn_life")
 
    life_ins_data.show()

    val financial_lifeins = life_ins_data.withColumn("has_life_ins", when($"date_of_txn_life".isNull, 0).otherwise(1))
    financial_lifeins.show()

    //financial_lifeins.registerTempTable("financial_lifeins")

    
    
    
    //HEALTH INSURANCE

    val health_ins_txn = txn_ucic.filter($"cleaned_merchant_name".contains("health"))
    //health_ins_txn.show()

    val health_ins_data_naming = health_ins_txn.select($"ucic" as "ucic_health", $"amount" as "amount_health",$"date_of_txn" as "date_of_txn_health")
    
    
    val life_health_data = financial_lifeins.join(health_ins_data_naming, financial_lifeins("ucic") === health_ins_data_naming("ucic_health"),"left").select($"ucic",$"amount_life",$"date_of_txn_life",$"has_life_ins",$"amount_health",$"date_of_txn_health")
    life_health_data.show()

     val financial_life_health = life_health_data.withColumn("has_health_ins", when($"date_of_txn_health".isNull, 0).otherwise(1))

    //financial_healthins.registerTempTable("financial_healthins")


    

    //AUTO INSURANCE


    val financial_autoins = sqlContext.sql("select ucic as ucic_auto, amount as amount_auto, date_of_txn as date_of_txn_auto from txn_ucic where cleaned_merchant_category = 'Insurance' and (cleaned_merchant_name not like '%life ins%' or cleaned_merchant_name not like '%health%')")

    val life_health_auto_data = financial_life_health.join(financial_autoins, financial_life_health("ucic") === financial_autoins("ucic_auto"), "left").select($"ucic",$"amount_life",$"date_of_txn_life",$"has_life_ins",$"amount_health",$"date_of_txn_health",$"has_health_ins",$"amount_auto",$"date_of_txn_auto")
    
    val financial_all = life_health_auto_data.withColumn("has_auto_ins", when($"date_of_txn_auto".isNull, 0).otherwise(1))

    
    
/*

    val financial_all2 = financial_all.withColumn("amount_auto2", $"amount_auto".cast(IntegerType)).drop("amount_auto").withColumnRenamed("amount_auto2", "amount_auto")
    val financial_all3 = financial_all.withColumn("amount_health2", $"amount_health".cast(IntegerType)).drop("amount_health").withColumnRenamed("amount_health2", "amount_health")
    val financial_all4 = financial_all.withColumn("amount_life2", $"amount_life".cast(IntegerType)).drop("amount_life").withColumnRenamed("amount_life2", "amount_life")

   */
    financial_all.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "financial_profile3", "keyspace" -> "goals101"))
      .save()



    
  }
}