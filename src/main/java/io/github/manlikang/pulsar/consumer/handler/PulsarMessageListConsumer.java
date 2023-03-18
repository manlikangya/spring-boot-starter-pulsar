package io.github.manlikang.pulsar.consumer.handler;


import io.github.manlikang.pulsar.message.PulsarMessage;

import java.util.List;

/**
 * 使用该类接收消息，必须设置 batchReceive = true (默认值) , wrapped = true
 *
 * @author fuhan
 * @date 2022/9/9
 */
public interface PulsarMessageListConsumer {

  /**
   * 监听消息
   *
   * @param pulsarMessages 消息原始格式
   */
  void onMessage(List<PulsarMessage<byte[]>> pulsarMessages);
}
