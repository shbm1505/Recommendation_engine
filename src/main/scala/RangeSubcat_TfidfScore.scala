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

object RangeSubcat_TfidfScore {
  def main(args: Array[String]) {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val transactionDF = sqlContext.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "transaction", "keyspace" -> "goals101"))
      .load()

    val merchant_tagsDF = sqlContext.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "merchant_tags", "keyspace" -> "goals101"))
      .load()

    val accountDF = sqlContext.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "account", "keyspace" -> "goals101"))
      .load()

    val txn_ucic = transactionDF.join(accountDF, transactionDF("account_no") === accountDF("account_no"))

    val txn_merchantcat = txn_ucic.join(merchant_tagsDF, txn_ucic("cleaned_merchant_name") === merchant_tagsDF("display_merchant_name"))

    txn_merchantcat.registerTempTable("txn_merchantcat")

    val tfDF = sqlContext.sql("select ucic, range_subcat, count(range_subcat) as tf from txn_merchantcat where ucic is not null group by range_subcat, ucic")

    val total_docsDF = sqlContext.sql("select count(distinct ucic) as total_docs from txn_merchantcat")
    total_docsDF.show()

    val tempdf = sqlContext.sql("select distinct ucic, range_subcat from txn_merchantcat")
    tempdf.registerTempTable("tempdf")
    val doc_freqDF = sqlContext.sql("select range_subcat, count(*) as doc_freq from tempdf group by range_subcat")
    //doc_freqDF.show()
    doc_freqDF.registerTempTable("doc_freqDF")

    val doc_freqDF2 = sqlContext.sql("select range_subcat as rsub, doc_freq,log(" + total_docsDF.collect()(0).getLong(0) + "/doc_freq) as idf from doc_freqDF")
    //doc_freqDF2.show()

    val tfidfDF = tfDF.join(doc_freqDF2, tfDF("range_subcat") === doc_freqDF2("rsub"))
    tfidfDF.registerTempTable("tfidfDF")

    tfidfDF.show()

    val tfidfFinal = sqlContext.sql("select ucic, range_subcat, tf,doc_freq, idf, tf*idf as tf_idfscore from tfidfDF")
    tfidfFinal.show()
    tfidfFinal.registerTempTable("tfidfFinal")


try {		
    tfidfFinal.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "tfidf_score", "keyspace" -> "goals101"))
      .save()

	}
	catch {
		case x: Exception=>{}
	}	

  }
}

