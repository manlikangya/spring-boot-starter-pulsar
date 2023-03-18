package io.github.manlikang.pulsar.reconsume;


import io.github.manlikang.pulsar.properties.consumer.ConsumerDefaultProperties;
import io.github.manlikang.pulsar.properties.consumer.ConsumerSpecialProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.springframework.stereotype.Component;

/**
 * @author fuhan
 * @date 2022/12/1 - 16:58
 */
@Component
@Slf4j
public class ReconsumeHandler {

  public <T> void reconsume(
      Consumer<T> consumer,
      Message<T> msg,
      ConsumerSpecialProperties properties,
      ConsumerDefaultProperties defaultConfig) {
    final ReconsumeStrategyEnum strategyEnum = getReconsumeStrategyEnum(properties, defaultConfig);
    ReconsumeStrategyMap.get(strategyEnum)
        .reconsumeLater(consumer, msg, getReconsumeStrategyProp(properties, defaultConfig));
  }

  public <T> void reconsume(
      Consumer<T> consumer,
      Messages<T> messages,
      ConsumerSpecialProperties properties,
      ConsumerDefaultProperties defaultConfig) {
    final ReconsumeStrategyEnum strategyEnum = getReconsumeStrategyEnum(properties, defaultConfig);
    ReconsumeStrategyMap.get(strategyEnum)
        .reconsumeLater(consumer, messages, getReconsumeStrategyProp(properties, defaultConfig));
  }

  private static ReconsumeStrategyEnum getReconsumeStrategyEnum(
      ConsumerSpecialProperties properties, ConsumerDefaultProperties defaultConfig) {
    ReconsumeStrategyEnum strategyEnum = properties.getReconsumeStrategy();
    if (strategyEnum == null) {
      strategyEnum = defaultConfig.getReconsumeStrategy();
    }
    if (strategyEnum == null) {
      strategyEnum = ReconsumeStrategyEnum.progressive;
    }
    return strategyEnum;
  }

  public ReconsumeStrategyProp getReconsumeStrategyProp(
      ConsumerSpecialProperties properties, ConsumerDefaultProperties defaultConfig) {
    ReconsumeStrategyProp strategyProp = new ReconsumeStrategyProp();
    long fixedTimeIntervalMs = 5000;
    if (properties.getFixedReconsumeIntervalMs() > 0) {
      fixedTimeIntervalMs = properties.getFixedReconsumeIntervalMs();
    } else if (defaultConfig.getFixedReconsumeIntervalMs() > 0) {
      fixedTimeIntervalMs = defaultConfig.getFixedReconsumeIntervalMs();
    }

    long[] progressiveReconsumeIntervalMs = {
      1000,
      5 * 1000,
      10 * 1000,
      30 * 1000,
      60 * 1000,
      120 * 1000,
      300 * 1000,
      600 * 1000,
      1800 * 1000,
      3600 * 1000,
      2 * 3600 * 1000
    };
    if (properties.getProgressiveReconsumeIntervalMs() != null
        && properties.getProgressiveReconsumeIntervalMs().length > 0) {
      progressiveReconsumeIntervalMs = properties.getProgressiveReconsumeIntervalMs();
    }
    if (defaultConfig.getProgressiveReconsumeIntervalMs() != null
        && defaultConfig.getProgressiveReconsumeIntervalMs().length > 0) {
      progressiveReconsumeIntervalMs = defaultConfig.getProgressiveReconsumeIntervalMs();
    }

    double fixedRate = 1.5D;
    if (properties.getFixedRateReconsume() > 0) {
      fixedRate = properties.getFixedRateReconsume();
    } else if (defaultConfig.getFixedRateReconsume() > 0) {
      fixedRate = defaultConfig.getFixedRateReconsume();
    }
    strategyProp.setFixedTimeIntervalMs(fixedTimeIntervalMs);
    strategyProp.setProgressiveTimeIntervalMs(progressiveReconsumeIntervalMs);
    strategyProp.setFixedRate(fixedRate);
    return strategyProp;
  }
}
