package io.github.manlikang.pulsar.properties.consumer;


import io.github.manlikang.pulsar.reconsume.ReconsumeStrategyEnum;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import java.io.Serializable;

/**
 * @author fuhan
 * @date 2022/9/9 - 10:51
 */
@Getter
@Setter
public class ConsumerSpecialProperties implements Serializable {

  private static final long serialVersionUID = -8713441990445441075L;

  /** 消费Topic， 必填 */
  String topic;

  /** 消费者bean name, 必填 */
  String consumerBeanName;

  /** 消费者名称，不指定时为默认值 默认为 CONSUMER-${spring-application-name}.toUpperCase() (全大写) 建议使用默认值 */
  String consumerName;

  /** 订阅名称，默认为 SUBSCRIPTION-${spring-application-name}-TopicName.toUpperCase() (全大写) 建议自定义 */
  String subscriptionName;
  /**
   * 死信topic, 当消息达到最大重试次数任然没有被成功消费后，会被投递至该Topic 当开启重试时有效，不指定则取默认值 ${subscriptionName} + "-DLQ"
   * 建议使用默认值
   */
  String deadLetterTopic;
  /**
   * 重试topic, 当消息消费失败后，会将消息投递至该topic中进行重试。启动时会自动订阅重试Topic 当开启重试时有效，
   * 不指定则取默认值 ${subscriptionName} + "-RETRY" 建议使用默认值
   */
  String retryLetterTopic;

  /** 订阅类型 */
  SubscriptionType subscriptionType = SubscriptionType.Shared;

  /** 订阅后开始消费位置 */
  SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Latest;

  /** 是否是持久化topic */
  boolean persistent = true;

  /** 消息确认超时时间 单位毫秒 */
  int ackTimeoutMs = 0;

  /** 是否开启消费 */
  boolean enable = true;

  /** 是否开启重试，不开启重试，消息消费失败将会直接丢弃 */
  boolean enableRetry = true;

  /** 最大重试次数，超过这个次数之后将会将消息丢入死信队列 */
  int maxRedeliverCount = -1;

  /** 批量消费消息最大数量 */
  int batchReceiveMaxNumMessages;

  /** 批量消费消息最大字节数 10M */
  int batchReceiveMaxNumBytes;

  /** 批量消息消费超时时间 */
  int batchReceiveTimeoutMs;

  /** 消息重试策略 */
  ReconsumeStrategyEnum reconsumeStrategy = null;

  /** 固定重试时间间隔 单位：秒 当重试策略为固定时间间隔策略(fixedInterval)时，该属性有效 单位毫秒 */
  long fixedReconsumeIntervalMs = 0;

  /** 递进式重试时间间隔 单位：毫秒 */
  long[] progressiveReconsumeIntervalMs = null;

  /** 固定倍率重试策略 */
  double fixedRateReconsume = 0;

  /** 接收队列大小 */
  private int receiveQueueSize = 0;

  /**
   * 订阅消息的标签，多个标签使用英文逗号“,”分隔
   */
  private String tags;
}
