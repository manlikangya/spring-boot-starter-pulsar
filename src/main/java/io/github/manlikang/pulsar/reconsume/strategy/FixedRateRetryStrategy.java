package io.github.manlikang.pulsar.reconsume.strategy;


import io.github.manlikang.pulsar.reconsume.ReconsumeMessagesImpl;
import io.github.manlikang.pulsar.reconsume.ReconsumeStrategy;
import io.github.manlikang.pulsar.reconsume.ReconsumeStrategyProp;
import io.github.manlikang.pulsar.utils.PulsarMessageUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author fuhan
 * @date 2022/9/5 - 10:19
 */
@Slf4j
public class FixedRateRetryStrategy implements ReconsumeStrategy {

  /** 最大重试时间间隔 */
  private static final long MAX_TIME_INTERVALS = 60 * 60 * 24 * 1000;

  /** 开始时间间隔 1分钟 */
  private static final long START_TIME_INTERVALS = 60 * 1000;

  private static final String DELAY_TIME = "DELAY_TIME";

  private static boolean isNotLong(String str) {
    try {
      Long.parseLong(str);
      return false;
    } catch (NumberFormatException e) {
      return true;
    }
  }

  @Override
  public <T> void reconsumeLater(
      Consumer<T> consumer, Message<T> message, ReconsumeStrategyProp reconsumeStrategyProp) {
    final long nextConsumerLaterMs = getNextConsumerLaterMs(message, reconsumeStrategyProp);
    log.info("message[{}]下次更新时间在:{}ms后",message.getMessageId().toString(), nextConsumerLaterMs);
    consumer
        .reconsumeLaterAsync(message, nextConsumerLaterMs, TimeUnit.MILLISECONDS)
        .whenComplete(
            (empty, throwable) -> {
              if (throwable != null) {
                log.error(
                    "pulsar消息发往重试队列失败, messageId:{}", message.getMessageId().toString(), throwable);
              }
            });
  }

  @Override
  public <T> void reconsumeLater(
      Consumer<T> consumer, Messages<T> messageList, ReconsumeStrategyProp reconsumeStrategyProp) {
    Map<Long, ReconsumeMessagesImpl<T>> messagesMap = new HashMap<>(16);
    for (final Message<T> message : messageList) {
      final long nextConsumerLaterMs = getNextConsumerLaterMs(message, reconsumeStrategyProp);
      ReconsumeMessagesImpl<T> messages = messagesMap.get(nextConsumerLaterMs);
      if (Objects.isNull(messages)) {
        messages = new ReconsumeMessagesImpl<>();
      }
      messages.add(message);
      messagesMap.put(nextConsumerLaterMs, messages);
    }
    messagesMap.forEach(
        (nextConsumerLaterMs, trackMessages) -> {
          log.info("message[{}]下次更新时间在:{}ms后", PulsarMessageUtils.getMessageLog(trackMessages), nextConsumerLaterMs);
          consumer
              .reconsumeLaterAsync(trackMessages, nextConsumerLaterMs, TimeUnit.MILLISECONDS)
              .whenComplete(
                  (empty, throwable) -> {
                    if (throwable != null) {
                      log.error(
                          "pulsar消息发往重试队列失败,消息ID:{}",
                          PulsarMessageUtils.getMessageId(messageList),
                          throwable);
                    }
                  });
        });
  }

  private long getNextTimeIntervals(String lastTimeIntervalTimeStr, double fixedRate) {
    if (StringUtils.isBlank(lastTimeIntervalTimeStr) || isNotLong(lastTimeIntervalTimeStr)) {
      return START_TIME_INTERVALS;
    }
    final long lastTimeIntervalTime = Long.parseLong(lastTimeIntervalTimeStr);
    long thisTimeIntervalTime = (long) (lastTimeIntervalTime * fixedRate);
    return Math.min(thisTimeIntervalTime, MAX_TIME_INTERVALS);
  }

  private long getNextConsumerLaterMs(
      Message<?> message, ReconsumeStrategyProp reconsumeStrategyProp) {
    final double fixedRate = reconsumeStrategyProp.getFixedRate();
    long nextTimeIntervals = START_TIME_INTERVALS;
    if (message.hasProperty(DELAY_TIME)) {
      // 取出来的是毫秒值
      final String lastTimeIntervalTimeStr = message.getProperty(DELAY_TIME);
      log.info("message[{}]存在延时时间:{}",message.getMessageId().toString(), lastTimeIntervalTimeStr);
      nextTimeIntervals = getNextTimeIntervals(lastTimeIntervalTimeStr, fixedRate);
    }
    return nextTimeIntervals;
  }
}
