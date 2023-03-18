package io.github.manlikang.pulsar.producer.sender;


import io.github.manlikang.pulsar.message.SendFailMessage;
import lombok.extern.slf4j.Slf4j;

/**
 * @author fuhan
 * @date 2022/12/1 - 17:58
 */
@Slf4j
public class DefaultMessageSendFailHandler implements MessageSendFailHandler {
  @Override
  public void handler(SendFailMessage sendFailMessage) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("Topic[").append(sendFailMessage.getTopic()).append("]");
    stringBuilder.append(sendFailMessage.isDelay() ? "延时" : "实时");
    stringBuilder.append("消息");
    stringBuilder.append(sendFailMessage.isAsync() ? "异步" : "同步");
    stringBuilder.append("发送失败, 消息: ");
    stringBuilder.append(sendFailMessage.getMsg());
    log.error("{}", stringBuilder, sendFailMessage.getThrowable());
  }
}
