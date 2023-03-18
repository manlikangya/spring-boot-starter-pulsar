package io.github.manlikang.pulsar.consumer;


import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import io.github.manlikang.pulsar.constant.Serialization;
import io.github.manlikang.pulsar.consumer.handler.PulsarMessageConsumer;
import io.github.manlikang.pulsar.consumer.handler.PulsarMessageListConsumer;
import io.github.manlikang.pulsar.consumer.handler.PulsarStringConsumer;
import io.github.manlikang.pulsar.consumer.handler.PulsarStringListConsumer;
import io.github.manlikang.pulsar.error.exception.ConsumerInitException;
import io.github.manlikang.pulsar.properties.consumer.ConsumerDefaultProperties;
import io.github.manlikang.pulsar.properties.consumer.ConsumerProperties;
import io.github.manlikang.pulsar.properties.consumer.ConsumerSpecialProperties;
import io.github.manlikang.pulsar.utils.PulsarMessageUtils;
import io.github.manlikang.pulsar.utils.SchemaUtils;
import io.github.manlikang.pulsar.utils.SpringHolder;
import io.github.manlikang.pulsar.utils.UrlBuildService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.*;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author fuhan
 * @date 2022/8/16 - 15:33
 */
@Component
@Slf4j
@DependsOn({"pulsarClient"})
public class ConsumerAggregator {

  @Resource private PulsarClient pulsarClient;
  @Resource private ConsumerProperties consumerProperties;
  @Resource private UrlBuildService urlBuildService;

  @Resource private ConsumerBatchHandler consumerBatchHandler;

  @Resource private ConsumerDefaultProperties defaultProperties;
  private List<Consumer<?>> consumers;

  @Resource private ConsumerSingleHandler consumerSingleHandler;

  @EventListener(ApplicationReadyEvent.class)
  public void init() {
    if (!defaultProperties.isEnable()) {
      log.info("Pulsar consumer is disable.");
      return;
    }
    final List<ConsumerSpecialProperties> consumersList = consumerProperties.getConsumers();
    if (CollectionUtils.isEmpty(consumersList)) {
      return;
    }
    consumers =
        consumersList.stream()
            .map(this::subscribe)
            .filter(list -> Objects.nonNull(list) && list.size() > 0)
            .flatMap(List::stream)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
  }

  private List<Consumer<?>> subscribe(ConsumerSpecialProperties properties) {
    final ConsumerDefaultProperties defaultConfig = defaultProperties;
    try {
      if (!properties.isEnable()) {
        log.info("[Pulsar]消费者：{}被禁用，无需初始化", properties.getConsumerBeanName());
        return null;
      }
      boolean batchReceive = isBatchReceive(properties);
      final ConsumerBuilder<?> consumerBuilder =
          pulsarClient
              .newConsumer(SchemaUtils.getSchema(Serialization.JSON, byte[].class))
              .subscriptionName(
                  urlBuildService.buildPulsarSubscriptionName(
                      properties.getSubscriptionName(), properties.getTopic()))
              .topic(
                  urlBuildService.buildTopicUrl(properties.getTopic(), properties.isPersistent()))
              .subscriptionInitialPosition(properties.getSubscriptionInitialPosition())
              .enableRetry(properties.isEnableRetry())
              .subscriptionType(properties.getSubscriptionType());
      // 设置超时时间
      buildConsumerAckTimeOut(consumerBuilder, properties, defaultConfig);
      // 设置消息重试规则
      if (properties.isEnableRetry()) {
        buildDeadLetterPolicy(consumerBuilder, properties, defaultConfig);
      }
      // 设置消息批量接收规则
      if (batchReceive) {
        buildBatchReceivePolicy(consumerBuilder, properties, defaultConfig);
      }
      // 构建接收队列大小
      buildReceiveQueueSize(consumerBuilder, properties, defaultConfig);
      // 构建消费者自定义订阅属性，如消息标签等
      this.buildConsumerSubProperties(consumerBuilder, properties, defaultConfig);
      return buildMessageConsumer(consumerBuilder, properties, defaultConfig, batchReceive);
    } catch (PulsarClientException e) {
      if (e.getMessage().contains("Not Found")) {
        throw new ConsumerInitException(
            " Topic not found:"
                + urlBuildService.buildTopicUrl(properties.getTopic(), properties.isPersistent()),
            e);
      }
      throw new ConsumerInitException(
          "Failed to init Topic["
              + properties.getTopic()
              + "] consumer:["
              + properties.getConsumerName()
              + "].",
          e);
    }
  }

  private void buildReceiveQueueSize(
      ConsumerBuilder<?> consumerBuilder,
      ConsumerSpecialProperties properties,
      ConsumerDefaultProperties defaultConfig) {
    if (properties.getReceiveQueueSize() > 0) {
      consumerBuilder.receiverQueueSize(properties.getReceiveQueueSize());
      log.info("Topic[{}]接收队列大小设置为:{}", properties.getTopic(), properties.getReceiveQueueSize());
      return;
    }
    if (defaultConfig.getReceiveQueueSize() > 0) {
      consumerBuilder.receiverQueueSize(defaultConfig.getReceiveQueueSize());
      log.info("Topic[{}]接收队列大小设置为:{}", properties.getTopic(), defaultConfig.getReceiveQueueSize());
      return;
    }
    consumerBuilder.receiverQueueSize(1000);
    log.info("Topic[{}]接收队列大小设置为:{}", properties.getTopic(), 1000);
  }

  private List<Consumer<?>> buildMessageConsumer(
      ConsumerBuilder<?> consumerBuilder,
      ConsumerSpecialProperties properties,
      ConsumerDefaultProperties defaultConfig,
      boolean batchReceive)
      throws PulsarClientException {
    final int consumerThreads = defaultConfig.getConsumerThreads();
    String consumerName = urlBuildService.buildPulsarConsumerName(properties.getConsumerName());
    List<Consumer<?>> consumerList = new ArrayList<>(consumerThreads);
    if (batchReceive) {
      for (int i = 0; i < defaultConfig.getConsumerNum(); i++) {
        consumerBuilder.consumerName(consumerName + "-" + i);
        final Consumer<?> consumer = consumerBuilder.subscribe();
        consumerBatchHandler.batchConsume(consumer, properties, defaultConfig);
        consumerList.add(consumer);
      }
      log.info(
          "Topic[{}]正在使用批量消费模式, 消费者订阅数:{}, 单个订阅处理线程数:{}",
          properties.getTopic(),
          defaultConfig.getConsumerNum(),
          consumerThreads);
      return consumerList;
    }
    for (int i = 0; i < defaultConfig.getConsumerNum(); i++) {
      consumerBuilder.consumerName(consumerName + "-" + i);
      final Consumer<?> consumer = consumerBuilder.subscribe();
      consumerSingleHandler.receive(consumer, properties, defaultConfig);
      consumerList.add(consumer);
    }
    log.info(
        "Topic[{}]正在使用单条消费模式, 消费者订阅数:{}, 单个订阅处理线程数:{}",
        properties.getTopic(),
        defaultConfig.getConsumerNum(),
        consumerThreads);
    return consumerList;
  }

  private void buildBatchReceivePolicy(
      ConsumerBuilder<?> consumerBuilder,
      ConsumerSpecialProperties properties,
      ConsumerDefaultProperties defaultConfig) {
    int maxNumBytes = 10 * 1024 * 1024;
    if (properties.getBatchReceiveMaxNumBytes() > 0) {
      maxNumBytes = properties.getBatchReceiveMaxNumBytes();
    } else if (defaultConfig.getBatchReceiveMaxNumBytes() > 0) {
      maxNumBytes = defaultConfig.getBatchReceiveMaxNumBytes();
    }

    int maxNumMessages = 100;
    if (properties.getBatchReceiveMaxNumMessages() > 0) {
      maxNumMessages = properties.getBatchReceiveMaxNumMessages();
    } else if (defaultConfig.getBatchReceiveMaxNumMessages() > 0) {
      maxNumMessages = defaultConfig.getBatchReceiveMaxNumMessages();
    }

    int timeout = 100;
    if (properties.getBatchReceiveTimeoutMs() > 0) {
      timeout = properties.getBatchReceiveTimeoutMs();
    } else if (defaultConfig.getBatchReceiveTimeoutMs() > 0) {
      timeout = defaultConfig.getBatchReceiveTimeoutMs();
    }

    BatchReceivePolicy batchReceivePolicy =
        new BatchReceivePolicy.Builder()
            .maxNumBytes(maxNumBytes)
            .maxNumMessages(maxNumMessages)
            .timeout(timeout, TimeUnit.MILLISECONDS)
            .build();
    log.info("Topic[{}]批量接收策略设置为:{}", properties.getTopic(), JSON.toJSONString(batchReceivePolicy));
    consumerBuilder.batchReceivePolicy(batchReceivePolicy);
  }

  private void buildDeadLetterPolicy(
      ConsumerBuilder<?> consumerBuilder,
      ConsumerSpecialProperties properties,
      ConsumerDefaultProperties defaultConfig) {
    int maxRedeliverCount = 16;
    if (properties.getMaxRedeliverCount() > 0) {
      maxRedeliverCount = properties.getMaxRedeliverCount();
    } else if (defaultConfig.getMaxRedeliverCount() > 0) {
      maxRedeliverCount = defaultConfig.getMaxRedeliverCount();
    }
    DeadLetterPolicy.DeadLetterPolicyBuilder deadLetterBuilder =
        DeadLetterPolicy.builder().maxRedeliverCount(maxRedeliverCount);
    if (StringUtils.isNoneBlank(properties.getRetryLetterTopic())) {
      deadLetterBuilder.retryLetterTopic(
          urlBuildService.buildTopicUrl(properties.getRetryLetterTopic()));
    }
    if (StringUtils.isNoneBlank(properties.getDeadLetterTopic())) {
      deadLetterBuilder.deadLetterTopic(
          urlBuildService.buildTopicUrl(properties.getDeadLetterTopic()));
    }
    final DeadLetterPolicy deadLetterPolicy = deadLetterBuilder.build();
    log.info("Topic[{}]死信消息策略设置为:{}", properties.getTopic(), JSON.toJSONString(deadLetterPolicy));
    consumerBuilder.deadLetterPolicy(deadLetterPolicy);
  }

  private void buildConsumerAckTimeOut(
      ConsumerBuilder<?> consumerBuilder,
      ConsumerSpecialProperties properties,
      ConsumerDefaultProperties defaultConfig) {
    if (properties.getAckTimeoutMs() > 0) {
      log.info("Topic[{}]已设置确认超时时间：{}ms", properties.getTopic(), properties.getAckTimeoutMs());
      consumerBuilder.ackTimeout(properties.getAckTimeoutMs(), TimeUnit.MILLISECONDS);
      return;
    }
    if (defaultConfig.getAckTimeoutMs() > 0) {
      log.info("Topic[{}]已设置确认超时时间：{}ms", properties.getTopic(), defaultConfig.getAckTimeoutMs());
      consumerBuilder.ackTimeout(defaultConfig.getAckTimeoutMs(), TimeUnit.MILLISECONDS);
    }
  }

  private boolean isBatchReceive(ConsumerSpecialProperties properties)
      throws PulsarClientException {
    final String consumerBean = properties.getConsumerBeanName();
    final Object bean = SpringHolder.getContext().getBean(consumerBean);
    if (bean instanceof PulsarMessageListConsumer || bean instanceof PulsarStringListConsumer) {
      return true;
    } else if (bean instanceof PulsarMessageConsumer || bean instanceof PulsarStringConsumer) {
      return false;
    } else {
      throw new PulsarClientException("pulsar消费者beanName:[" + consumerBean + "]的类型错误，请实现相关接口");
    }
  }

  public List<Consumer<?>> getConsumers() {
    return consumers;
  }

  /**
   * 构造消费者自定义订阅属性
   *
   * @param consumerBuilder 消费者构造者
   * @param properties 消费者配置
   * @param defaultConfig 消费者默认配置
   */
  private void buildConsumerSubProperties(
      ConsumerBuilder<?> consumerBuilder,
      ConsumerSpecialProperties properties,
      ConsumerDefaultProperties defaultConfig) {
    // 根据用户配置订阅的消息标签
    List<String> tags = PulsarMessageUtils.splitStrByComma(properties.getTags());
    if (!CollectionUtils.isEmpty(tags)) {
      Map<String, String> subProperties = Maps.newHashMap();
      tags.forEach(tag -> subProperties.put(tag, "1"));
      log.info(
          "Topic[{}]-消费者[{}]，设置消息过滤标签：{}",
          properties.getTopic(),
          properties.getConsumerBeanName(),
          tags);
      consumerBuilder.subscriptionProperties(subProperties);
    }
  }
}
