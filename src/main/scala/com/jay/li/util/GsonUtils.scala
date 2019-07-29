package com.jay.li.util

import com.google.gson.Gson

object GsonUtils {

  private val gson = new Gson()

  def writeValueAsString(any: Any): String = {
    gson.toJson(any)
  }


  def jsonStr2Class[T](jsonStr: String, t: Class[T]): T = {
    gson.fromJson(jsonStr, t)
  }

}
