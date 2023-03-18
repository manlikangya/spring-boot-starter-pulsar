package io.github.manlikang.pulsar.consumer;


import io.github.manlikang.pulsar.consumer.handler.ConsumerExceptionHandler;
import io.github.manlikang.pulsar.consumer.handler.PulsarMessageListConsumer;
import io.github.manlikang.pulsar.consumer.handler.PulsarStringListConsumer;
import io.github.manlikang.pulsar.error.FailedMessage;
import io.github.manlikang.pulsar.properties.consumer.ConsumerDefaultProperties;
import io.github.manlikang.pulsar.properties.consumer.ConsumerSpecialProperties;
import io.github.manlikang.pulsar.reconsume.ReconsumeHandler;
import io.github.manlikang.pulsar.utils.PulsarMessageUtils;
import io.github.manlikang.pulsar.utils.SpringHolder;
import io.github.manlikang.pulsar.utils.TraceIdUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import static io.github.manlikang.pulsar.utils.PulsarMessageUtils.valueToStringList;
import static io.github.manlikang.pulsar.utils.PulsarMessageUtils.wrapMessageList;


/**
 * @author fuhan
 * @date 2022/12/1 - 16:11
 */
@Slf4j
@Component
public class ConsumerBatchHandler {

  @Resource private ReconsumeHandler reconsumeHandler;

  @SuppressWarnings("all")
  public <T> void batchConsume(
      Consumer<T> consumer,
      ConsumerSpecialProperties properties,
      ConsumerDefaultProperties defaultConfig) {

    for (int i = 0; i < defaultConfig.getConsumerThreads(); i++) {
      // 因为此处需要线程常驻，所以直接创建新线程
      new Thread(
              () -> {
                while (true) {
                  try {
                    final Messages<T> messages = consumer.batchReceive();
                    if (messages != null && messages.size() > 0) {
                      receiveHandle(consumer, messages, properties, defaultConfig);
                    }
                  } catch (PulsarClientException e) {
                    if (e instanceof PulsarClientException.AlreadyClosedException) {
                      log.error("pulsar消费者线程已关闭,线程名：[{}]，原因：批量接收消息异常,消费者[{}]已经关闭", Thread.currentThread().getName(),consumer.getConsumerName());
                    } else {
                      log.error("pulsar消费者线程已关闭,线程名：[{}], 批量接收消息异常:{}", Thread.currentThread().getName(), e.getMessage(), e);
                    }
                    break;
                  } catch (Exception e) {
                    log.error("pulsar消费者线程已关闭,线程名：[{}],pulsar批量接收消息异常:{}",Thread.currentThread().getName(), e.getMessage(), e);
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
      Messages<T> messages,
      ConsumerSpecialProperties properties,
      ConsumerDefaultProperties defaultConfig) {
    final String traceId = TraceIdUtils.get();
    final String consumerBeanName = properties.getConsumerBeanName();
    final Object bean = SpringHolder.getContext().getBean(consumerBeanName);
    try {
      log.info(
          "[{}]Topic[{}]批量接收到{}条消息 :{}",
          traceId,
          properties.getTopic(),
          messages.size(),
          PulsarMessageUtils.getMessageLog(messages));
      if (bean instanceof PulsarStringListConsumer) {
        PulsarStringListConsumer stringListConsumer = (PulsarStringListConsumer) bean;
        stringListConsumer.onMessage(valueToStringList(messages));
      } else if (bean instanceof PulsarMessageListConsumer) {
        PulsarMessageListConsumer messageListConsumer = (PulsarMessageListConsumer) bean;
        messageListConsumer.onMessage(wrapMessageList(messages));
      }
      log.info("[{}]处理完成,正在推送确认标识", traceId);
      consumer.acknowledge(messages);
      log.info("[{}]已推送确认标识", traceId);
    } catch (Exception e) {
      log.error("[{}]消息消费出现异常", traceId, e);
      try{
        if (properties.isEnableRetry()) {
          log.info("[{}]消息消费异常重试处理", traceId);
          reconsumeHandler.reconsume(consumer, messages, properties, defaultConfig);
          log.info("[{}]消息消费异常重试处理完成", traceId);
        }
        if (bean instanceof ConsumerExceptionHandler) {
          ConsumerExceptionHandler exceptionHandler = (ConsumerExceptionHandler) bean;
          exceptionHandler.handler(new FailedMessage(e, consumer, null, messages));
        }
      } catch (Throwable throwable){
        log.error("[{}]消息消费异常处理失败:{}",traceId,e.getMessage(), throwable);
      }
    }
  }
}
