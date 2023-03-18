package io.github.manlikang.pulsar.reconsume.strategy;


import io.github.manlikang.pulsar.reconsume.ReconsumeMessagesImpl;
import io.github.manlikang.pulsar.reconsume.ReconsumeStrategy;
import io.github.manlikang.pulsar.reconsume.ReconsumeStrategyProp;
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
 * @date 2022/8/17 - 17:33
 */
@Slf4j
public class ProgressiveRetryStrategy implements ReconsumeStrategy {

  /** 重试间隔最大支持24小时 */
  private static final long MAX_TIME_INTERVALS = 60 * 60 * 24 * 1000;

  private static final String DELAY_TIME = "DELAY_TIME";

  @Override
  public <T> void reconsumeLater(
      Consumer<T> consumer, Message<T> message, ReconsumeStrategyProp reconsumeStrategyProp) {
    final long nextConsumerLaterMs = getNextConsumerLaterMs(message, reconsumeStrategyProp);
    consumer
        .reconsumeLaterAsync(message, nextConsumerLaterMs, TimeUnit.MILLISECONDS)
        .whenComplete(
            (empty, throwable) -> {
              if (throwable != null) {
                log.error("pulsar消息发往重试队列失败,消息ID:{}", message.getMessageId(), throwable);
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
          consumer
              .reconsumeLaterAsync(trackMessages, nextConsumerLaterMs, TimeUnit.MILLISECONDS)
              .whenComplete(
                  (empty, throwable) -> {
                    if (throwable != null) {
                      log.error("pulsar消息发往重试队列失败", throwable);
                    }
                  });
        });
  }

  private long getNextConsumerLaterMs(
      Message<?> message, ReconsumeStrategyProp reconsumeStrategyProp) {
    final long[] progressiveTimeIntervals = reconsumeStrategyProp.getProgressiveTimeIntervalMs();
    long nextTimeIntervals = progressiveTimeIntervals[0];
    if (message.hasProperty(DELAY_TIME)) {
      // 取出来的是毫秒值
      final String lastTimeIntervalTimeStr = message.getProperty(DELAY_TIME);
      nextTimeIntervals = getNextTimeIntervals(lastTimeIntervalTimeStr, progressiveTimeIntervals);
    }
    return nextTimeIntervals;
  }

  private long getNextTimeIntervals(
      String lastTimeIntervalTimeStr, long[] progressiveTimeIntervals) {
    if (StringUtils.isBlank(lastTimeIntervalTimeStr) || isNotLong(lastTimeIntervalTimeStr)) {
      return progressiveTimeIntervals[0];
    }
    final long lastTimeIntervalTime = Long.parseLong(lastTimeIntervalTimeStr);
    final int lastTimeIndex = getIndex(lastTimeIntervalTime, progressiveTimeIntervals);
    if (lastTimeIndex == -1) {
      return progressiveTimeIntervals[0];
    }
    int thisIndex = lastTimeIndex + 1;

    if (thisIndex < progressiveTimeIntervals.length) {
      long progressiveTimeInterval = progressiveTimeIntervals[thisIndex];
      if (progressiveTimeInterval > MAX_TIME_INTERVALS) {
        progressiveTimeInterval = MAX_TIME_INTERVALS;
      }
      return progressiveTimeInterval;
    }
    return progressiveTimeIntervals[progressiveTimeIntervals.length - 1];
  }

  private static int getIndex(long intervalTime, long[] progressiveTimeIntervals) {
    for (int i = 0; i < progressiveTimeIntervals.length; i++) {
      if (progressiveTimeIntervals[i] == intervalTime) {
        return i;
      }
    }
    return -1;
  }

  private static boolean isNotLong(String str) {
    try {
      Long.parseLong(str);
      return false;
    } catch (NumberFormatException e) {
      return true;
    }
  }
}
