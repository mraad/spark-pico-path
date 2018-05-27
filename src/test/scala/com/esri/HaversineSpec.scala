package com.esri

import org.scalatest.{FlatSpec, Matchers}

class HaversineSpec extends FlatSpec with Matchers {

  it should "test haversine" in {
    Haversine.distance(36.12, -86.67, 33.94, -118.40) shouldBe 2886448.4 +- 0.1
  }

}
