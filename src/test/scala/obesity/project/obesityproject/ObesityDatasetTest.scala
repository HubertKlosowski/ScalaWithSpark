package obesity.project.obesityproject


import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite

class ObesityDatasetTest extends AnyFunSuite with SharedSparkContext {
  private val obesityDataset: ObesityDataset = ObesityDataset()

  test("ObesityDataset.getStatisticFromGroupBy sum") {
    val result = obesityDataset.getStatisticFromGroupBy(
      stat = "sum",
      groupingColumn = "Gender",
      statColumn = "Weight"
    )
    assert(result.isInstanceOf[DataFrame])
  }

  test("ObesityDataset.getStatisticFromGroupBy mean") {
    val result = obesityDataset.getStatisticFromGroupBy(
      stat = "mean",
      groupingColumn = "Gender",
      statColumn = "Weight"
    )
    assert(result.isInstanceOf[DataFrame])
  }

  test("ObesityDataset.getStatisticFromGroupBy count") {
    val result = obesityDataset.getStatisticFromGroupBy(
      stat = "count",
      groupingColumn = "Gender"
    )
    assert(result.isInstanceOf[DataFrame])
  }

  test("ObesityDataset.getDataFrame") {
    val df = obesityDataset.getDataFrame
  }

  test("ObesityDataset.countNullInColumn") {

    val notNull = obesityDataset.countNullInColumn("Gender")
  }

  test("ObesityDataset.countNotNullInColumn") {
    val nullColumn = obesityDataset.countNotNullInColumn("Gender")
  }
}
