package io.github.manlikang.pulsar.utils;

import lombok.experimental.UtilityClass;

import java.util.UUID;

/**
 * @author fuhan
 * @date 2022/12/1 - 18:58
 */
@UtilityClass
public class TraceIdUtils {

  public String get() {
    return UUID.randomUUID().toString().replace("-", "");
  }
}
