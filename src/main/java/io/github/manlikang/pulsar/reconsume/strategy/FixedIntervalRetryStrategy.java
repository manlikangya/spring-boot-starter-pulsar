package io.github.manlikang.pulsar.reconsume.strategy;


import io.github.manlikang.pulsar.reconsume.ReconsumeStrategy;
import io.github.manlikang.pulsar.reconsume.ReconsumeStrategyProp;
import io.github.manlikang.pulsar.utils.PulsarMessageUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;

import java.util.concurrent.TimeUnit;

/**
 * 固定时间间隔重试策略
 *
 * @author fuhan
 * @date 2022/8/17 - 17:33
 */
@Slf4j
public class FixedIntervalRetryStrategy implements ReconsumeStrategy {

  @Override
  public <T> void reconsumeLater(
      Consumer<T> consumer, Message<T> message, ReconsumeStrategyProp reconsumeStrategyProp) {
    final long fixedTimeInterval = reconsumeStrategyProp.getFixedTimeIntervalMs();
    consumer
        .reconsumeLaterAsync(message, fixedTimeInterval, TimeUnit.MILLISECONDS)
        .whenComplete(
            (empty, throwable) -> {
              if (throwable != null) {
                log.error("pulsar消息重试处理失败,消息ID:{}", message.getMessageId(), throwable);
              }
            });
  }

  @Override
  public <T> void reconsumeLater(
      Consumer<T> consumer, Messages<T> messageList, ReconsumeStrategyProp reconsumeStrategyProp) {
    final long fixedTimeInterval = reconsumeStrategyProp.getFixedTimeIntervalMs();
    consumer
        .reconsumeLaterAsync(messageList, fixedTimeInterval, TimeUnit.MILLISECONDS)
        .whenComplete(
            (empty, throwable) -> {
              if (throwable != null) {
                log.error(
                    "pulsar消息重试处理失败,消息ID:{}",
                    PulsarMessageUtils.getMessageId(messageList),
                    throwable);
              }
            });
  }
}
