package io.github.manlikang.pulsar.reconsume;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author fuhan
 * @date 2022/8/17
 */
@Getter
@AllArgsConstructor
public enum ReconsumeStrategyEnum {

  /** 立即重试策略 */
  active,

  /** 固定时间间隔策略 */
  fixedInterval,

  /** 递进时间间隔策略 */
  progressive,

  /** 固定倍率时间间隔 */
  fixedRate,

  /** 直接丢弃 */
  discard
}
