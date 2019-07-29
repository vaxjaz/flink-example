package com.jay.li.util

import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.Date

object DateUtils {

  def nowDayTimeStamp(): Long = {
    date2Timestamp(LocalDateTime.now())
  }

  def nowDayTime(): Long = {
    date2Timestamp(LocalDate.now())
  }

  def anyDayBeforeTodayTimeStamp(offset: Int): Long = {
    date2Timestamp(LocalDate.now().minusDays(offset))
  }

  private def date2Timestamp(date: LocalDate): Long = {
    date.atStartOfDay(ZoneId.of("Asia/Shanghai")).toInstant.toEpochMilli
  }

  private def date2Timestamp(date: LocalDateTime): Long = {
    date.atZone(ZoneId.of("Asia/Shanghai")).toInstant.toEpochMilli
  }

}
