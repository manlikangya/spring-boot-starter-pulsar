package io.github.manlikang.pulsar.producer.sender;


import io.github.manlikang.pulsar.message.SendFailMessage;

/**
 * @author fuhan
 * @date 2022/12/1
 */
public interface MessageSendFailHandler {

  /**
   * 处理发送失败的消息
   *
   * @param sendFailMessage 消息
   */
  void handler(SendFailMessage sendFailMessage);
}
