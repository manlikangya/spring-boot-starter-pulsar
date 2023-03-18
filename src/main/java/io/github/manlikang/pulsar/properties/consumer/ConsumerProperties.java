package io.github.manlikang.pulsar.properties.consumer;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * pulsar消费者配置
 *
 * @author fuhan
 */
@ConfigurationProperties(prefix = "pulsar.consumer")
@Getter
@Setter
public class ConsumerProperties {
  /** 消费者配置 */
  private List<ConsumerSpecialProperties> consumers;
}
