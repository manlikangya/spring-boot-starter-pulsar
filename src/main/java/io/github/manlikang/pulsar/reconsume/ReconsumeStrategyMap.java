package io.github.manlikang.pulsar.reconsume;


import io.github.manlikang.pulsar.reconsume.strategy.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author fuhan
 * @date 2022/8/17 - 18:15
 */
public class ReconsumeStrategyMap {

  private static final Map<ReconsumeStrategyEnum, ReconsumeStrategy> STRATEGY_MAP =
      new ConcurrentHashMap<>(16);

  static {
    STRATEGY_MAP.put(ReconsumeStrategyEnum.active, new ActiveRetryStrategy());
    STRATEGY_MAP.put(ReconsumeStrategyEnum.fixedInterval, new FixedIntervalRetryStrategy());
    STRATEGY_MAP.put(ReconsumeStrategyEnum.progressive, new ProgressiveRetryStrategy());
    STRATEGY_MAP.put(ReconsumeStrategyEnum.fixedRate, new FixedRateRetryStrategy());
    STRATEGY_MAP.put(ReconsumeStrategyEnum.discard, new DiscardRetryStrategy());
  }

  public static ReconsumeStrategy get(ReconsumeStrategyEnum reconsumeStrategyEnum) {
    final ReconsumeStrategy reconsumeStrategy = STRATEGY_MAP.get(reconsumeStrategyEnum);
    if (reconsumeStrategy == null) {
      throw new IllegalArgumentException(reconsumeStrategyEnum.name() + " is not exist");
    }
    return reconsumeStrategy;
  }
}
