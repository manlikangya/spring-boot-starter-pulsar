package io.github.manlikang.pulsar.consumer.handler;

import java.util.List;

/**
 * 使用该类接收消息，必须设置 batchReceive = true (默认值) , wrapped = false (默认值)
 *
 * @author fuhan
 * @date 2022/9/9
 */
public interface PulsarStringListConsumer {

  /**
   * 监听消息
   *
   * @param msgList 消息字符串格式
   */
  void onMessage(List<String> msgList);
}
