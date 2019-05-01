package com.fredericoramos78.model

object CabInfo {

  def of(arrayOfValues: Seq[String]): CabInfo = {
    val riddingInfo: (Option[String], Option[String], Int) = arrayOfValues(4).toUpperCase() match {
      case "YES" => (Some(arrayOfValues(5)), Some(arrayOfValues(6)), arrayOfValues(7).toInt)
      case _ => (None, None, 0)
    }

    CabInfo(arrayOfValues(0), arrayOfValues(1), arrayOfValues(2), arrayOfValues(3),
      riddingInfo._1.isDefined, riddingInfo._1, riddingInfo._2, riddingInfo._3)
  }
}

case class CabInfo(cabId: String, plateNumber: String , cabType: String , driverName: String,
                   ongoingTrip: Boolean, originLocation: Option[String] = None,
                   destinationLocation: Option[String] = None,
                   passengerCount: Int = 0)
