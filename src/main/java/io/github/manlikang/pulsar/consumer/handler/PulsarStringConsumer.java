package io.github.manlikang.pulsar.consumer.handler;

/**
 * 使用该类接收消息，必须设置 batchReceive = false , wrapped = false (默认值)
 *
 * @author fuhan
 * @date 2022/9/9
 */
public interface PulsarStringConsumer {

  /**
   * 监听消息
   *
   * @param msg 消息字符串格式
   */
  void onMessage(String msg);
}
