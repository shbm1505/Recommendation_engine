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
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions.{ from_unixtime, unix_timestamp }
import org.apache.spark.sql.functions.{ rank, desc }
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ StructType, StructField, LongType }

object user_merchant_score {

  def main(args: Array[String]) {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val transactionDF = sqlContext.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "transaction", "keyspace" -> "goals101"))
      .load()

    transactionDF.registerTempTable("transactionDF")

    val accountDF = sqlContext.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "account", "keyspace" -> "goals101"))
      .load()

    val txn_ucic = transactionDF.join(accountDF, transactionDF("account_no") === accountDF("account_no"))
    txn_ucic.registerTempTable("txn_ucic")



    val user_merchant_cat = sqlContext.sql("select ucic, cleaned_merchant_category, cleaned_merchant_name, count(*) as merchant_cat_txncount, max(date_of_txn) as lasttxnday,datediff(now(),max(date_of_txn)) as days_from_lasttxn from txn_ucic group by ucic, cleaned_merchant_category,cleaned_merchant_name ")
    user_merchant_cat.show()
    //user_merchant_cat.registerTempTable("user_merchant_cat")
    //print(merchant_cat.count())



    val freq_cat = List("Dining & Bar", "Entertainment", "Groceries & Supermar", "Utilities", "Petrol")

    val merchant_cat_recency = user_merchant_cat.withColumn("Recency", when($"cleaned_merchant_category" isin (freq_cat: _*), exp(-($"days_from_lasttxn") / 14)).otherwise(exp(-($"days_from_lasttxn") / 60)))
    //merchant_cat_recency.show()
    //merchant_cat_recency.registerTempTable("merchant_cat_recency")

    val merchant_cat_score = merchant_cat_recency.select($"ucic",$"cleaned_merchant_category",$"cleaned_merchant_name", ($"merchant_cat_txncount"*$"Recency") as "merchant_weight")
    merchant_cat_score.show()
    merchant_cat_score.registerTempTable("merchant_cat_score")


    val distinctcat = List("ECom", "Other Stores", "Electronic & Compute", "Auto", "Quasi- Cash ", "Insurance ", "Healthcare", "Airlines", "Dining & Bar", "Groceries & Supermar", "Telecom", "F & F", "Travel Agencies", "ATM", "Apparel", "Petrol", "Utilities", "Road", "Courier", "Jewellary", "Education", "Hotel", "Misc", "Entertainment", "Direct Mktg", "POS", "Cash", "Railways", "DTH ", "Car Rental")
    var i = ""

    val schema_string = "id,ucic,cleaned_merchant_category,cleaned_merchant_name,merchant_weight,quartile"
    val schema_rdd = StructType(schema_string.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    var dfcombined = sqlContext.createDataFrame(sc.emptyRDD[Row], schema_rdd)

    for (i <- distinctcat) {
      var df1 = sqlContext.sql("select * from merchant_cat_score where cleaned_merchant_category = '" + i + "'")

      val df1count = df1.count()

      val schema = df1.schema
      val rows = df1.rdd.zipWithUniqueId.map {
        case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)
        }
      val dfWithID = sqlContext.createDataFrame(
        rows, StructType(StructField("id", LongType, false) +: schema.fields))
      //dfWithPK.filter($"id" < 10).show()

      var cutoff = df1count / 4
      var dfsorted = dfWithID.sort($"merchant_weight".desc)
      var dfquartile = dfsorted.withColumn("quartile", when(($"id" < cutoff), 1)
        .when(($"id" >= cutoff) && ($"id" < (cutoff * 2)), 2)
        .when(($"id" >= (cutoff * 2)) && ($"id" < (cutoff * 3)), 3)
        .otherwise(4))

      dfquartile.show()

      dfcombined = dfcombined.unionAll(dfquartile)
      dfcombined.show()

    }

    try {

    dfcombined.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "user_category_merchant_weight2", "keyspace" -> "goals101"))
      .save()
    }catch {
      case x: Exception => {

        //Config_file.exception_logging(x, script_id, current_time, sc)
      }
    }


    



  }
}
