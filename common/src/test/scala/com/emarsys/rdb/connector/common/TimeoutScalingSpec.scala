package com.emarsys.rdb.connector.common

import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._

class TimeoutScalingSpec extends WordSpec with Matchers {
  "#completionTimeout" should {
    "scale with id" in {
      completionTimeout(60.seconds) shouldBe 60.seconds
    }
  }

  "#idleTimaout" should {
    "scale to 99%" in {
      idleTimeout(1000.seconds) shouldBe 990.seconds
    }

    "scale down at least a second" in {
      idleTimeout(5.seconds) shouldBe 4.seconds
    }

    "never reach 0" in {
      idleTimeout(1.second) shouldBe 1.second
    }
  }

  "#queryTimaout" should {
    "scale to 98%" in {
      queryTimeout(1000.seconds) shouldBe 980.seconds
    }

    "scale down at least two seconds" in {
      queryTimeout(5.seconds) shouldBe 3.seconds
    }

    "never reach 0" in {
      queryTimeout(2.seconds) shouldBe 1.second
    }
  }
}
