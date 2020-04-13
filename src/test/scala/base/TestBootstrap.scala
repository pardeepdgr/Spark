package base

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

abstract class TestBootstrap extends FlatSpec with Matchers with BeforeAndAfter with MockitoSugar
