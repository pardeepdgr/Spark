package com.learning.helper

object CustomUdf {

  def upper(): String => String = {
    val toUpper: String => String = s => s.toUpperCase
    toUpper
  }

}
