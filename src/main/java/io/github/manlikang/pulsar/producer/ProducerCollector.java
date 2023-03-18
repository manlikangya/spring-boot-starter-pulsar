package io.github.manlikang.pulsar.producer;


import io.github.manlikang.pulsar.constant.Serialization;
import io.github.manlikang.pulsar.error.exception.ProducerInitException;
import io.github.manlikang.pulsar.properties.producer.ProducerProperties;
import io.github.manlikang.pulsar.properties.producer.TopicProperties;
import io.github.manlikang.pulsar.utils.SchemaUtils;
import io.github.manlikang.pulsar.utils.TraceIdUtils;
import io.github.manlikang.pulsar.utils.UrlBuildService;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.stereotype.Component;
import org.springframework.util.StringValueResolver;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author fuhan
 * @date 2022/8/10 - 17:47
 */
@Component
@Slf4j
public class ProducerCollector
    implements ApplicationContextAware,
        EmbeddedValueResolverAware,
        InitializingBean,
        DisposableBean {

  @Resource private PulsarClient pulsarClient;
  @Resource private UrlBuildService urlBuildService;

  @Resource private ProducerProperties producerProperties;

  private final Map<String, Producer> producers = new ConcurrentHashMap<>();

  private StringValueResolver stringValueResolver;

  private static ApplicationContext applicationContext;

  public Producer getProducer(String topic) {
    return producers.get(stringValueResolver.resolveStringValue(topic));
  }

  @Override
  public void setEmbeddedValueResolver(StringValueResolver stringValueResolver) {
    this.stringValueResolver = stringValueResolver;
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext0) throws BeansException {
    applicationContext = applicationContext0;
  }

  @Override
  public void destroy() throws Exception {
    producers.forEach(
        (topic, producer) -> {
          if (producer.isConnected()) {
            producer.closeAsync();
          }
        });
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    registerProducer();
  }

  public void registerProducer() throws PulsarClientException {
    if (!producerProperties.isEnabled()) {
      log.info("producer is disable.");
      return;
    }
    final List<TopicProperties> topics = producerProperties.getTopics();
    if (topics == null || topics.size() == 0) {
      return;
    }

    int defaultSendTimeOutMs = 30000;
    if (producerProperties.getSendTimeoutMs() > 0) {
      defaultSendTimeOutMs = producerProperties.getSendTimeoutMs();
    }
    final CompressionType compressionType = producerProperties.getCompressionType();
    for (final TopicProperties properties : topics) {
      final ProducerConfiguration configuration =
          ProducerConfiguration.builder()
              .persistent(properties.isPersistent())
              .batchingEnabled(properties.isBatchingEnabled())
              .maxPendingMessages(properties.getMaxPendingMessages())
              .blockIfQueueFull(properties.isBlockIfQueueFull())
              .compressionType(
                  Objects.isNull(properties.getCompressionType())
                      ? compressionType
                      : properties.getCompressionType())
              .sendTimeoutMs(
                  properties.getSendTimeoutMs() > 0
                      ? properties.getSendTimeoutMs()
                      : defaultSendTimeOutMs)
              .build();
      final String topicName = stringValueResolver.resolveStringValue(properties.getName());
      ProducerHolder producerHolder =
          new ProducerHolder(
              topicName, properties.getMsgClass(), Serialization.JSON, configuration);
      try {
        final Producer<?> producer = buildProducer(producerHolder);
        producers.put(topicName, producer);
        log.info("[{}] producer init success", topicName);
      } catch (Exception e) {
        log.error("[{}] producer init fail:{}", topicName, e.getMessage());
        throw e;
      }
    }
  }

  private Producer<?> buildProducer(ProducerHolder holder) {
    try {
      final ProducerBuilder<?> producerBuilder = pulsarClient.newProducer(getSchema(holder));
      final ProducerConfiguration producerConfiguration = holder.getProducerConfiguration();
      log.info(
          "Initializing Producer for Topic:[{}],  {}", holder.getTopic(), producerConfiguration);
      final Producer<?> producer =
          producerBuilder
              .topic(
                  urlBuildService.buildTopicUrl(
                      holder.getTopic(), producerConfiguration.isPersistent()))
              .enableBatching(producerConfiguration.isBatchingEnabled())
              .producerName(getProducerName())
              .maxPendingMessages(producerConfiguration.getMaxPendingMessages())
              .blockIfQueueFull(producerConfiguration.isBlockIfQueueFull())
              .compressionType(producerConfiguration.getCompressionType())
              .sendTimeout(producerConfiguration.getSendTimeoutMs(), TimeUnit.MILLISECONDS)
              .create();
      log.info("Successes to init producer for Topic[{}]", holder.getTopic());
      return producer;
    } catch (PulsarClientException e) {
      if (e.getMessage().contains("Not Found")) {
        throw new ProducerInitException(
            "Failed to init producer, Not found topic:" + holder.getTopic());
      }
      throw new ProducerInitException("Failed to init producer", e);
    }
  }

  private Schema<?> getSchema(ProducerHolder holder) throws RuntimeException {
    return SchemaUtils.getSchema(holder.getSerialization(), holder.getClazz());
  }

  private String getProducerName() {
    final String applicationName = applicationContext.getApplicationName();
    final String substring = TraceIdUtils.get().substring(0, 6);
    return applicationName + substring;
  }
}
