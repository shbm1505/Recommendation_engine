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

object merchant_categories {

  def main(args: Array[String]) {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val transactionDF = sqlContext.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "transaction", "keyspace" -> "goals101"))
      .load()

    transactionDF.registerTempTable("transactionDF")

    val merchantDF = sqlContext.sql("SELECT cleaned_merchant_category , cleaned_merchant_name , amount , date_of_txn , date_format(date_of_txn, 'EEEE') as weekday ,date_format(date_of_txn, 'd') as date, month(date_of_txn) as month , year(date_of_txn) as year, account_no from transactionDF")
    merchantDF.show()

    
    val merchantDF2 = merchantDF.withColumn("week_no" , when( ($"date"<8) , 1)
												.when( ($"date">=8) && ($"date"<15), 2)
												.when( ($"date">=15) && ($"date"<22), 3)
												.otherwise(4)) 
    merchantDF2.show()




  }
}
