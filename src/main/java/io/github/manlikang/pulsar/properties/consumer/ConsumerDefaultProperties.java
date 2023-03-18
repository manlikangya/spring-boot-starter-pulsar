package io.github.manlikang.pulsar.properties.consumer;


import io.github.manlikang.pulsar.reconsume.ReconsumeStrategyEnum;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.Serializable;

/**
 * @author fuhan
 * @date 2022/9/9 - 10:49
 */
@ConfigurationProperties(prefix = "pulsar.consumer.default")
@Getter
@Setter
public class ConsumerDefaultProperties implements Serializable {

  private static final long serialVersionUID = -9047914502238728857L;
  /** 最大重试次数，超过这个次数之后将会将消息丢入死信队列 */
  int maxRedeliverCount = 16;
  /** 批量消费消息最大数量 */
  int batchReceiveMaxNumMessages = 50;
  /** 批量消费消息最大字节数 10M */
  int batchReceiveMaxNumBytes = 1024 * 1024 * 10;
  /** 批量消息消费超时时间 */
  int batchReceiveTimeoutMs = 100;

  /** 消息重试策略 */
  ReconsumeStrategyEnum reconsumeStrategy = ReconsumeStrategyEnum.progressive;

  /** 固定重试时间间隔 单位：秒 当重试策略为固定时间间隔策略(fixedInterval)时，该属性有效 单位毫秒 */
  long fixedReconsumeIntervalMs = 5000;
  /** 递进式重试时间间隔 单位：毫秒 */
  long[] progressiveReconsumeIntervalMs = {
    1000,
    5 * 1000,
    10 * 1000,
    30 * 1000,
    60 * 1000,
    120 * 1000,
    300 * 1000,
    600 * 1000,
    1800 * 1000,
    3600 * 1000,
    2 * 3600 * 1000
  };
  /** 固定倍率重试策略 */
  double fixedRateReconsume = 1.5d;
  /** 消费者数量 */
  int consumerNum = 1;
  /** 每个消费者处理的线程数 */
  int consumerThreads = 1;
  /** 消息确认超时时间 单位毫秒 */
  private int ackTimeoutMs = 30000;

  /** 接收队列大小 */
  private int receiveQueueSize = 1000;
  /** 是否启用消费者 */
  private boolean enable = true;
}
