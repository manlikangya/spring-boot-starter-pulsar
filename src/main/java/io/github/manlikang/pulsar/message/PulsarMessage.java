package io.github.manlikang.pulsar.message;

import lombok.Data;
import org.apache.pulsar.client.api.MessageId;

import java.util.Map;

/**
 * PulsarMessage
 *
 * @param <T> 泛型
 * @author fuhan
 */
@Data
public class PulsarMessage<T> {

  private T value;
  private Map<String, String> properties;
  private String topicName;
  private String key;
  private MessageId messageId;
  private long sequenceId;
  private String producerName;
  private long publishTime;
  private long eventTime;
}
