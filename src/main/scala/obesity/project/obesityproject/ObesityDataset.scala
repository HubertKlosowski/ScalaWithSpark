package obesity.project.obesityproject

import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer

class ObesityDataset(private var dataFrame: DataFrame) {
  private var desc: ListBuffer[String] = ListBuffer(
    "Id",
    "płeć badanego (np. Male/Female)",
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
  )

  def getStatisticFromGroupBy(stat: String = "mean", groupingColumn: String = "Gender", columnToGroup: String = ""): DataFrame = {
    val grouped = dataFrame.groupBy(groupingColumn)
    stat match {
      case "mean" => grouped.mean(columnToGroup)
      case "sum" => grouped.sum(columnToGroup)
      case "count" => grouped.count()
      case _ => throw new IllegalArgumentException("BLAD!! Niepoprawna nazwa statystyki.")
    }
  }

  def addBMIColumn(): Unit = {
    val bmi = dataFrame("Weight") / (dataFrame("Height") * dataFrame("Height"))
    dataFrame = dataFrame.withColumn("BMI", bmi)
    desc.append("wartość BMI = (waga / (wzrost * wzrost))")
  }

  override def toString: String = {
    val stringBuilder: StringBuilder = new StringBuilder("Obesity Dataset Info\n")
    stringBuilder.append("Nazwa kolumny | Typ kolumny | Opis\n")
    for ((el, el_desc) <- dataFrame.schema zip desc) stringBuilder.append(s"${el.name} | ${el.dataType} | ${el_desc} \n")
    stringBuilder.toString()
  }
}
