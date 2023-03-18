package io.github.manlikang.pulsar.reconsume;

import lombok.Data;

import java.io.Serializable;

/**
 * @author fuhan
 * @date 2022/8/17 - 17:47
 */
@Data
public class ReconsumeStrategyProp implements Serializable {

  private static final long serialVersionUID = -2176281193491169743L;
  /** 重试策略递进时间间隔数组： 单位：毫秒 */
  private long[] progressiveTimeIntervalMs;

  /** 固定时间间隔重试策略的时间间隔 ,单位： 毫秒 */
  private long fixedTimeIntervalMs;

  /** 固定倍率重试策略的 倍率 */
  private double fixedRate;
}
