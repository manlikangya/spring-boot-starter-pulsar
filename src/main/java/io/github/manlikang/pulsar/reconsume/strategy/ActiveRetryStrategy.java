package io.github.manlikang.pulsar.reconsume.strategy;


import io.github.manlikang.pulsar.reconsume.ReconsumeStrategy;
import io.github.manlikang.pulsar.reconsume.ReconsumeStrategyProp;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;

/**
 * 立即重试策略
 *
 * @author fuhan
 * @date 2022/8/17 - 17:10
 */
public class ActiveRetryStrategy implements ReconsumeStrategy {

  @Override
  public <T> void reconsumeLater(
      Consumer<T> consumer, Message<T> msg, ReconsumeStrategyProp reconsumeStrategyProp) {
    consumer.negativeAcknowledge(msg);
  }

  @Override
  public <T> void reconsumeLater(
      Consumer<T> consumer, Messages<T> messageList, ReconsumeStrategyProp reconsumeStrategyProp) {
    consumer.negativeAcknowledge(messageList);
  }
}
