package io.github.manlikang.pulsar.consumer;


import io.github.manlikang.pulsar.consumer.handler.ConsumerExceptionHandler;
import io.github.manlikang.pulsar.consumer.handler.PulsarMessageConsumer;
import io.github.manlikang.pulsar.consumer.handler.PulsarStringConsumer;
import io.github.manlikang.pulsar.error.FailedMessage;
import io.github.manlikang.pulsar.properties.consumer.ConsumerDefaultProperties;
import io.github.manlikang.pulsar.properties.consumer.ConsumerSpecialProperties;
import io.github.manlikang.pulsar.reconsume.ReconsumeHandler;
import io.github.manlikang.pulsar.utils.PulsarMessageUtils;
import io.github.manlikang.pulsar.utils.SpringHolder;
import io.github.manlikang.pulsar.utils.TraceIdUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;

import static io.github.manlikang.pulsar.utils.PulsarMessageUtils.wrapMessage;


/**
 * @author fuhan
 * @date 2022/12/1 - 16:11
 */
@Slf4j
@Component
public class ConsumerSingleHandler {

  @Resource private ReconsumeHandler reconsumeHandler;

  @SuppressWarnings("all")
  public <T> void receive(
      Consumer<T> consumer,
      ConsumerSpecialProperties properties,
      ConsumerDefaultProperties defaultConfig) {
    for (int i = 0; i < defaultConfig.getConsumerThreads(); i++) {
      // 因为此处需要线程常驻，所以直接创建新线程
      new Thread(
              () -> {
                while (true) {
                  try {
                    final Message<T> message = consumer.receive();
                    if (message != null) {
                      receiveHandle(consumer, message, properties, defaultConfig);
                    }
                  } catch (PulsarClientException e) {
                    if (e instanceof PulsarClientException.AlreadyClosedException) {
                      log.error(
                          "pulsar消费者线程已关闭,线程名：[{}]，原因：接收消息异常,消费者[{}]已经关闭",
                          Thread.currentThread().getName(),
                          consumer.getConsumerName());
                    } else {
                      log.error(
                          "pulsar消费者线程已关闭,线程名：[{}], 接收消息异常:{}",
                          Thread.currentThread().getName(),
                          e.getMessage(),
                          e);
                    }
                    break;
                  } catch (Exception e) {
                    log.error(
                        "pulsar消费者线程已关闭,线程名：[{}],pulsar接收消息异常:{}",
                        Thread.currentThread().getName(),
                        e.getMessage(),
                        e);
                    break;
                  }
                }
              },
              "CONSUMER-" + properties.getTopic() + "-" + i)
          .start();
    }
    }

  private <T> void receiveHandle(
          Consumer<T> consumer,
          Message<T> message,
          ConsumerSpecialProperties properties,
          ConsumerDefaultProperties defaultConfig) {
    final String traceId = TraceIdUtils.get();
    final String consumerBeanName = properties.getConsumerBeanName();
    final Object bean = SpringHolder.getContext().getBean(consumerBeanName);
    try {
      log.info(
              "[{}]Topic[{}]接收到消息 :{}",
              traceId,
              properties.getTopic(),
              PulsarMessageUtils.getMessageLog(message));
      if (bean instanceof PulsarStringConsumer) {
        PulsarStringConsumer pulsarStringConsumer = (PulsarStringConsumer) bean;
        final String msgStr = new String(message.getData(), StandardCharsets.UTF_8);
        pulsarStringConsumer.onMessage(msgStr);
      } else if (bean instanceof PulsarMessageConsumer) {
        PulsarMessageConsumer pulsarMessageConsumer = (PulsarMessageConsumer) bean;
        pulsarMessageConsumer.onMessage(wrapMessage(message));
      }
      log.info("[{}]处理完成,正在推送确认标识", traceId);
      consumer.acknowledge(message);
      log.info("[{}]已推送确认标识", traceId);
    } catch (Exception e) {
      log.error("[{}]消息消费出现异常", traceId, e);
      try{
        if (properties.isEnableRetry()) {
          log.info("[{}]消息消费异常重试处理", traceId);
          reconsumeHandler.reconsume(consumer, message, properties, defaultConfig);
          log.info("[{}]消息消费异常重试处理完成", traceId);
        }
        if (bean instanceof ConsumerExceptionHandler) {
          ConsumerExceptionHandler exceptionHandler = (ConsumerExceptionHandler) bean;
          exceptionHandler.handler(new FailedMessage(e, consumer, message, null));
        }
      } catch (Throwable throwable){
        log.error("[{}]消息消费异常处理失败:{}",traceId,e.getMessage(), throwable);
      }
    }
  }
}
