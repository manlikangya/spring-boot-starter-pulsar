package io.github.manlikang.pulsar.producer;


import io.github.manlikang.pulsar.constant.Serialization;

/**
 * @author fuhan
 * @date 2022/8/10 - 18:37
 */
public class ProducerHolder {

  private final String topic;
  private final Class<?> clazz;
  private final Serialization serialization;

  private final ProducerConfiguration producerConfiguration;

  public ProducerHolder(
      String topic,
      Class<?> clazz,
      Serialization serialization,
      ProducerConfiguration producerConfiguration) {
    this.topic = topic;
    this.clazz = clazz;
    this.serialization = serialization;
    this.producerConfiguration = producerConfiguration;
  }

  public String getTopic() {
    return topic;
  }

  public Class<?> getClazz() {
    return clazz;
  }

  public Serialization getSerialization() {
    return serialization;
  }

  public ProducerConfiguration getProducerConfiguration() {
    return producerConfiguration;
  }
}
