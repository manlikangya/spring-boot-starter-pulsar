package io.github.manlikang.pulsar.reconsume.strategy;


import io.github.manlikang.pulsar.reconsume.ReconsumeStrategy;
import io.github.manlikang.pulsar.reconsume.ReconsumeStrategyProp;
import io.github.manlikang.pulsar.utils.PulsarMessageUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;

import java.util.List;

/**
 * @author fuhan
 * @date 2022/11/7 - 10:20
 */
@Slf4j
public class DiscardRetryStrategy implements ReconsumeStrategy {
  @Override
  public <T> void reconsumeLater(
      Consumer<T> consumer, Message<T> message, ReconsumeStrategyProp reconsumeStrategyProp) {
    final MessageId messageId = message.getMessageId();
    log.warn("消息消费失败, 直接丢弃消息:{}", messageId.toString());
    try {
      consumer.acknowledge(message);
    } catch (PulsarClientException e) {
      log.error("批量发送ACK确认标识失败，消息ID:{}", messageId, e);
    }
  }

  @Override
  public <T> void reconsumeLater(
      Consumer<T> consumer, Messages<T> messageList, ReconsumeStrategyProp reconsumeStrategyProp) {
    final List<String> messageId = PulsarMessageUtils.getMessageId(messageList);
    log.warn("消息消费失败, 直接丢弃消息:{}", messageId);
    try {
      consumer.acknowledge(messageList);
    } catch (PulsarClientException e) {
      log.error("批量发送ACK确认标识失败，消息ID:{}", messageId, e);
    }
  }
}
