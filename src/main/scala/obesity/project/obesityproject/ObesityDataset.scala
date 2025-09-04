package obesity.project.obesityproject

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}

class ObesityDataset(private var dataFrame: DataFrame) {
  private var datasetInfo: Map[String, String] = (dataFrame.columns zip List(
    "Id",
    "płeć badanego",
    "wiek osoby (lata).",
    "wzrost osoby (metry).",
    "waga osoby (kilogramy).",
    "informacja, czy ktoś z rodziny ma historię nadwagi.",
    "czy osoba często je wysokokaloryczne posiłki.",
    "częstotliwość spożywania warzyw.",
    "liczba głównych posiłków dziennie.",
    "podjadanie między posiłkami.",
    "czy osoba pali papierosy.",
    "ilość spożywanej wody.",
    "czy osoba monitoruje liczbę kalorii w diecie.",
    "częstotliwość aktywności fizycznej.",
    "czas spędzany dziennie przed ekranami.",
    "częstotliwość spożywania alkoholu.",
    "najczęściej używany środek transportu (Car, Motorbike, Bike, Public_Transportation, Walking).",
    "docelowa kolumna (etykieta) – poziom otyłości."
  )).toMap

  def getStatisticFromGroupBy(stat: String = "mean", groupingColumn: String = "Gender", statColumn: String = ""): DataFrame = {
    val grouped = dataFrame.groupBy(groupingColumn)
    stat match {
      case "mean" => grouped.mean(statColumn)
      case "sum" => grouped.sum(statColumn)
      case "count" => grouped.count()
      case "min" => grouped.min(statColumn)
      case "max" => grouped.max(statColumn)
      case _ => throw new IllegalArgumentException("BLAD!! Niepoprawna nazwa statystyki.")
    }
  }

  def addBMIColumn(): Unit = {
    if (!dataFrame.columns.contains("BMI")) {
      val bmi = dataFrame("Weight") / (dataFrame("Height") * dataFrame("Height"))
      dataFrame = dataFrame.withColumn("BMI", bmi)
      datasetInfo = datasetInfo ++ Map("BMI" -> "wartość BMI = waga / (wzrost * wzrost)")
    }
  }

  def addAgeGroups(groups: Seq[(String, (AnyVal, AnyVal))]): Unit = {
    if (!dataFrame.columns.contains("AgeGroups")) {
      val init = lit("Unknown")
      val ageGroups = groups.foldLeft(init) { case (colChain, (label, (low, high))) =>
        when(col("Age") >= low && col("Age") < high, label).otherwise(colChain)
      }
      dataFrame = dataFrame.withColumn("AgeGroups", ageGroups)
      datasetInfo = datasetInfo ++ Map("AgeGroups" -> "podział wieku na grupy.")
    }
  }

  def describeColumn(columnName: String): DataFrame = {
    dataFrame.describe(columnName)
  }

  def countNotNullInColumn(columnName: String): Long = dataFrame.filter(col(columnName).isNotNull).count()

  def countNullInColumn(columnName: String): Long = dataFrame.filter(col(columnName).isNull).count()

  def getDataFrame: DataFrame = dataFrame

  override def toString: String = {
    val stringBuilder: StringBuilder = new StringBuilder("Zbiór Obesity\n")
    stringBuilder.append("Nazwa kolumny | Opis\n")
    for ((column_name, desc) <- datasetInfo) stringBuilder.append(s"$column_name | $desc \n")
    stringBuilder.toString()
  }
}
