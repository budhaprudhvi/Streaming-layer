package Zeppelin.Notebook

object Textfile {
  
import com.google.gson.Gson
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark
import org.bson.Document
import org.apache.spark.SparkConf
import com.mongodb.spark._
import com.mongodb.spark.config._

//sc.getConf

 val input = sc.textFile("/users/prudhvi/downloads/sample.txt")
val conf = new SparkConf().setAppName("InjestToMongo").setMaster("local[2]")

 val writeConf = WriteConfig.create("pqr","hjk", "mongodb://localhost:27017", 10, WriteConcernConfig.create(conf).writeConcern)

case class Policies1(policy_name: String, meta_level: String, premium_Values: String)

def convertToDoc(line1: String) =
      {

        val values = line1.split("\\|")

        val police1 = Policies1(values(0).trim, values(1).trim(), values(2).trim)

        val gson1 = new Gson
        val x = gson1.toJson(police1)

        Document.parse(x)
      }
      
 val rdd_doc = input.map(m => {

      convertToDoc(m)
    })

    rdd_doc.collect()
    MongoSpark.save(rdd_doc, writeConf)
}