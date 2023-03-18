package io.github.manlikang.pulsar.producer;


import io.github.manlikang.pulsar.exception.TopicProducerNotConfigException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * @author 付涵
 * @param <T> 消息类型
 */
@Component
public class PulsarTemplate<T> {

  private final ProducerCollector producerCollector;

  public PulsarTemplate(ProducerCollector producerCollector) {
    this.producerCollector = producerCollector;
  }

  @SuppressWarnings("all")
  public MessageId send(String topic, T msg) throws PulsarClientException {
    return getProducer(topic).send(msg);
  }

  @SuppressWarnings("all")
  public CompletableFuture<MessageId> sendAsync(String topic, T message) {
    return getProducer(topic).sendAsync(message);
  }

  @SuppressWarnings("all")
  public TypedMessageBuilder<T> createMessage(String topic, T message) {
    return getProducer(topic).newMessage().value(message);
  }

  @SuppressWarnings("all")
  public TypedMessageBuilder<T> createTagsMessage(String topic, Collection<String> tags, T message) {
    TypedMessageBuilder builder = getProducer(topic).newMessage();
    if (!CollectionUtils.isEmpty(tags)) {
      tags.forEach(tag -> builder.property(tag, "TAGS"));
    }
    return builder.value(message);
  }

  public Producer getProducer(String topic) {
    final Producer producer = producerCollector.getProducer(topic);
    if (producer == null) {
      throw new TopicProducerNotConfigException("[" + topic + "] producer not config!");
    }
    return producer;
  }
}
