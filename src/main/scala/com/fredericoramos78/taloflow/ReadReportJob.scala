package com.fredericoramos78.taloflow

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object ValueStateJob {


  val ROW_ID_POS = 0
  val BILL_REFDATE_POS = 6
  val ACCOUNT_ID_POS = 8
  val TYPE_OF_COST_POS = 9
  val EVENT_DATE_POS = 10
  val PRODUCT_POS = 12
  val USAGE_TYPE_POS = 13
  val USAGE_OPERATION_POS = 14
  val AZ_CODE_POS = 15
  val RESOURCE_ID_POS = 16
  val USAGE_MULTIPLIER_POS = 18
  val USAGE_AMOUNT_POS = 19
  val GROSS_RATE_POS = 21
  val GROSS_AMOUNT_POS = 22
  val NET_RATE_POS = 23
  val NET_AMOUNT_POS = 24


  def main(args: Array[String]): Unit = {
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Report structure used:
    // rowId: LineItemId (pos #0)
    // billRefDate: bill/BillingPeriodStartDate (pos #6),
    // accountId: lineItem/UsageAccountId (pos #8)
    // typeOfCost: lineItem/LineItemType (pos #9)
    // eventDate: lineItem/UsageStartDate (pos #10)
    // product: lineItem/ProductCode (pos #12)
    // usageType: lineItem/UsageType (pos #13)
    // usageOperation: lineItem/Operation (pos #14)
    // azCode: lineItem/AvailabilityZone (pos #15)
    // resourceId: lineItem/ResourceId (pos #16)
    // usageMultiplier: lineItem/NormalizationFactor (pos #18)
    // usageAmount: lineItem/NormalizedUsageAmount (pos #19)
    // grossRate: lineItem/UnblendedRate (pos #21)
    // grossCost: lineItem/UnblendedCost (pos #22)
    // netRate: lineItem/BlendedRate (pos #23)
    // netCost: lineItem/BlendedCost (pos #24)
    val source1 = env
      .readTextFile("./src/main/resources/report_1.csv")
      .map(row => {
        val splittedRow = row.split(",")
        ( splittedRow(0), splittedRow(0), splittedRow(0), splittedRow(0), splittedRow(0),
          splittedRow(0), splittedRow(0), splittedRow(0), splittedRow(0), splittedRow(0),
          splittedRow(0), splittedRow(0), splittedRow(0), splittedRow(0), splittedRow(0) )
      })

    // Tuple (String, Int)
    val translatedSource = source1
      .map(row => {
        val splittedRow = row.split(",")
        splittedRow(4).toUpperCase() match {
          case "YES" => (splittedRow(0), splittedRow(7).toInt)
          case _ => ("", -1)
        }
    }).filter(_._2 > 0)

      //translatedSource.print()

      translatedSource
      .keyBy(_._1)
//          .flatMapWithState[(String, Int), Int]( (element, acc) => {
//        (Seq[(String, Int)]((element._1, element._2 + acc.getOrElse(0))), Some(element._2 + acc.getOrElse(0)))
//      })
      .window(TumblingProcessingTimeWindows.of(Time.milliseconds(500)))
      .sum(1)
          .print()

    // execute program
    env.execute("ValueState example")
  }
}
