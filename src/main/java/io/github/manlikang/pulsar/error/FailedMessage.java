package io.github.manlikang.pulsar.error;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;

/**
 * @author fuhan
 * @date 2022/8/18 - 19:19
 */
public class FailedMessage {
  private final Exception exception;
  private final Consumer<?> consumer;
  private final Message<?> message;
  private final Messages<?> messages;

  public FailedMessage(
      Exception exception, Consumer<?> consumer, Message<?> message, Messages<?> messages) {
    this.exception = exception;
    this.consumer = consumer;
    this.message = message;
    this.messages = messages;
  }

  public Exception getException() {
    return exception;
  }

  public Consumer<?> getConsumer() {
    return consumer;
  }

  public Message<?> getMessage() {
    return message;
  }

  public Messages<?> getMessages() {
    return messages;
  }
}
