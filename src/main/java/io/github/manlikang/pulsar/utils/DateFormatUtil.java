package io.github.manlikang.pulsar.utils;

import lombok.experimental.UtilityClass;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author fuhan
 * @date 2022/12/1 - 19:24
 */
@UtilityClass
public class DateFormatUtil {

  private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public String format(Date date) {
    return DATE_FORMAT.format(date);
  }
}
