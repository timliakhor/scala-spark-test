package com.epam

import org.joda.time.DateTime
import org.scalatest.FunSuite

class TestDates extends FunSuite {

  test("Example.test") {
    val weatherDate = DateTime.parse("2020-06-21")
    val checkinDate = DateTime.parse("2020-06-20")
    assert(!checkinDate.isAfter(weatherDate), true)
  }
}
