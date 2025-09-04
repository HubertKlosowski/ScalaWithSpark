package obesity.project.obesityproject


import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row


class ObesityDatasetTest extends AnyFunSuite with SharedSparkContext {
  test("Test method getStatisticFromGroupBy for all statistics") {
    val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    val input = Seq(
      ("M", 25, 180.0, 80.0),
      ("F", 30, 160.0, 60.0),
      ("M", 24, 193.0, 90.0)
    ).toDF("Gender", "Age", "Height", "Weight")

    val obesityDataset: ObesityDataset = new ObesityDataset(input)
    val minimumAges = obesityDataset.getStatisticFromGroupBy(
      stat = "min", groupingColumn = "Gender", statColumn = "Age"
    ).as[(String, Int)].collect().toMap

    assert(minimumAges.getOrElse("M", -1) == 24)
    assert(minimumAges.getOrElse("F", -1) == 30)

    val maximumAges = obesityDataset.getStatisticFromGroupBy(
      stat = "max", groupingColumn = "Gender", statColumn = "Age"
    ).as[(String, Int)].collect().toMap

    assert(maximumAges.getOrElse("M", -1) == 25)
    assert(maximumAges.getOrElse("F", -1) == 30)

    val meanWeight = obesityDataset.getStatisticFromGroupBy(
      stat = "mean", groupingColumn = "Gender", statColumn = "Weight"
    ).as[(String, Double)].collect().toMap

    assert(meanWeight.getOrElse("M", -1) == 85.0)
    assert(meanWeight.getOrElse("F", -1) == 60.0)

    val sumWeight = obesityDataset.getStatisticFromGroupBy(
      stat = "sum", groupingColumn = "Gender", statColumn = "Weight"
    ).as[(String, Double)].collect().toMap

    assert(sumWeight.getOrElse("M", -1) == 170.0)
    assert(sumWeight.getOrElse("F", -1) == 60.0)

    val countGender = obesityDataset.getStatisticFromGroupBy(
      stat = "count", groupingColumn = "Gender"
    ).as[(String, Long)].collect().toMap

    assert(countGender.getOrElse("M", -1) == 2L)
    assert(countGender.getOrElse("F", -1) == 1L)
  }

  test("Test adding BMI to df") {
    val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    val input = Seq(
      ("M", 25, 1.8, 80.0),
      ("F", 30, 1.60, 60.0),
      ("M", 24, 1.93, 90.0)
    ).toDF("Gender", "Age", "Height", "Weight")

    val obesityDataset: ObesityDataset = new ObesityDataset(input)
    obesityDataset.addBMIColumn()
    val bmi = obesityDataset.getDataFrame.select("BMI").as[Double].collect()

    assert(obesityDataset.getDataFrame.columns.contains("BMI"))
    assert(bmi(0) - 24.69 < 1e-2)
    assert(bmi(1) - 23.43 < 1e-2)
    assert(bmi(2) - 24.16 < 1e-2)
  }

  test("Test adding age groups to df") {
    val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    val input = Seq(
      ("M", 25, 1.8, 80.0),
      ("F", 60, 1.60, 60.0),
      ("M", 14, 1.93, 90.0)
    ).toDF("Gender", "Age", "Height", "Weight")

    val obesityDataset: ObesityDataset = new ObesityDataset(input)
    obesityDataset.addAgeGroups(
      Seq("Child" -> (0, 15), "Teen" -> (15, 19), "Adult" -> (19, 45), "Old" -> (45, 100))
    )
    val ageGroups = obesityDataset.getDataFrame.select("AgeGroups").as[String].collect()

    assert(obesityDataset.getDataFrame.columns.contains("AgeGroups"))
    assert(ageGroups(0) == "Adult")
    assert(ageGroups(1) == "Old")
    assert(ageGroups(2) == "Child")
  }

  test("Test counting null\'s in df") {
    val sparkSession: SparkSession = SparkSession.builder().getOrCreate()

    val schema = StructType(Seq(
      StructField("Gender", StringType, true),
      StructField("Age", IntegerType, true),
      StructField("Height", DoubleType, true),
      StructField("Weight", DoubleType, true)
    ))

    val input = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(Seq(
      Row("M", 25, 1.8, null),
      Row("F", 60, 1.60, 60.0),
      Row("M", 14, 1.93, null)
    )), schema)

    val obesityDataset: ObesityDataset = new ObesityDataset(input)
    val nullCounts: Long = obesityDataset.countNullInColumn("Weight")
    val notNullCounts: Long = obesityDataset.countNotNullInColumn("Weight")

    assert(nullCounts == 2L)
    assert(notNullCounts == 1L)
  }
}
