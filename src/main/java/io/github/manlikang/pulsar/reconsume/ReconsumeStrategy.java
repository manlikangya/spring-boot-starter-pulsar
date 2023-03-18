package io.github.manlikang.pulsar.reconsume;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;

/**
 * @author fuhan
 * @date 2022/8/17
 */
public interface ReconsumeStrategy {

  /**
   * 延时多久之后进行重新消费
   *
   * @param message 消费失败的消息
   * @param consumer 消费者
   * @param reconsumeStrategyProp 重试策略属性
   */
  <T> void reconsumeLater(
      Consumer<T> consumer, Message<T> message, ReconsumeStrategyProp reconsumeStrategyProp);

  /**
   * 延时多久之后进行重新消费
   *
   * @param messageList 消费失败的消息集合
   * @param consumer 消费者
   * @param reconsumeStrategyProp 重试策略属性
   */
  <T> void reconsumeLater(
      Consumer<T> consumer, Messages<T> messageList, ReconsumeStrategyProp reconsumeStrategyProp);
}
