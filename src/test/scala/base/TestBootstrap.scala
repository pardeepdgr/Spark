package base

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

abstract class TestBootstrap extends AnyFlatSpec with Matchers with BeforeAndAfter
