package obesity.project.obesityproject

import obesity.project.obesityproject.ObesityDataset
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession, AnalysisException}

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  *  (+ select LocalApp when prompted)
  */
object LocalApp extends App {
  val (inputFile, outputFile) = (args(0), args(1))
  val spark = SparkSession.builder().appName("Obesity Dataset").master("local[*]").getOrCreate()

  Runner.run(spark, inputFile, outputFile)

  spark.stop()
}

object Runner {
  def run(sp: SparkSession, inputFile: String, outputFile: String): Unit = {
    val customSchema = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("Gender", StringType, true),
      StructField("Age", FloatType, true),
      StructField("Height", FloatType, true),
      StructField("Weight", FloatType, true),
      StructField("family_history_with_overweight", BooleanType, true),
      StructField("FAVC", BooleanType, true),
      StructField("FCVC", FloatType, true),
      StructField("NCP", FloatType, true),
      StructField("CAEC", StringType, true),
      StructField("SMOKE", BooleanType, true),
      StructField("CH2O", FloatType, true),
      StructField("SCC", BooleanType, true),
      StructField("FAF", FloatType, true),
      StructField("TUE", FloatType, true),
      StructField("CALC", StringType, true),
      StructField("MTRANS", StringType, true),
      StructField("NObeyesdad", StringType, true),
    ))
    val dataFrame: DataFrame = sp.read.option("header", true).schema(customSchema).csv(inputFile)
    val ob: ObesityDataset = new ObesityDataset(dataFrame)
    ob.addBMIColumn()
    ob.addAgeGroups(
      Seq("Child" -> (0, 15), "Teen" -> (15, 19), "Adult" -> (19, 45), "Old" -> (45, 100))
    )

    try
      println(ob.getStatisticFromGroupBy("min", "AgeGroups", "Age"))
    catch {
      case analysisException: AnalysisException => println("BŁĄD!! Dla wybranej kolumny nie można obliczyć podanej statystyki.")
    }

    println(ob.getDataFrame.show(5))

  }
}
