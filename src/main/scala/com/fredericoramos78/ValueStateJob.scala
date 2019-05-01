package com.fredericoramos78

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object ValueStateJob {

  def main(args: Array[String]): Unit = {
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source1 = env.readTextFile(" D:/fred/work/fredericoramos78/flink/wordCount/src/main/resources/assignment1.txt")

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
