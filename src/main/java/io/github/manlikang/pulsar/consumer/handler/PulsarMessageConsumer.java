package io.github.manlikang.pulsar.consumer.handler;


import io.github.manlikang.pulsar.message.PulsarMessage;

/**
 * 使用该类接收消息，必须设置 batchReceive = false , wrapped = true
 *
 * @author fuhan
 * @date 2022/9/9
 */
public interface PulsarMessageConsumer {

  /**
   * 监听消息
   *
   * @param pulsarMessage 消息原始格式
   */
  void onMessage(PulsarMessage<byte[]> pulsarMessage);
}
