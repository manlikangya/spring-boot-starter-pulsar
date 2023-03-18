package io.github.manlikang.pulsar.producer.sender;

import org.apache.pulsar.client.api.MessageId;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author fuhan
 * @date 2022/8/10
 */
public interface PulsarProducer {

  /**
   * 同步发送消息至指定topic
   *
   * @param topic topic名称
   * @param msg 消息内容
   * @return messageId
   */
  String sendRealTimeSync(String topic, String msg);

  /**
   * 同步发送标签消息至指定 topic
   *
   * @param topic topic名称
   * @param tags  消息标签列表
   * @param msg   消息内容
   * @return messageId
   */
  String sendRealTimeSyncTags(String topic, Collection<String> tags, String msg);

  /**
   * 异步发送消息至指定topic
   *
   * @param topic topic名称
   * @param msg 消息内容
   * @return CompletableFuture<MessageId>
   */
  CompletableFuture<MessageId> sendRealTimeAsync(String topic, String msg);

  /**
   * 异步发送消息至指定topic
   *
   * @param topic topic名称
   * @param tags  消息标签列表
   * @param msg   消息内容
   * @return CompletableFuture<MessageId>
   */
  CompletableFuture<MessageId> sendRealTimeAsyncTags(String topic, Collection<String> tags,
      String msg);

  /**
   * 同步发送延时消息至指定topic
   *
   * @param topic topic名称
   * @param msg 消息内容
   * @param delay 延时时间 （多少时间之后再进行消费）
   * @param timeUnit 延时时间单位
   * @return messageId
   */
  String sendDelayedSync(String topic, String msg, long delay, TimeUnit timeUnit);


  /**
   * 同步发送延时消息至指定topic
   *
   * @param topic topic名称
   * @param tags  消息标签列表
   * @param msg 消息内容
   * @param delay 延时时间 （多少时间之后再进行消费）
   * @param timeUnit 延时时间单位
   * @return messageId
   */
  String sendDelayedSyncTags(String topic,Collection<String> tags, String msg, long delay, TimeUnit timeUnit);

  /**
   * 异步发送延时消息至指定topic
   *
   * @param topic topic名称
   * @param msg 消息内容
   * @param delay 延时时间 （多少时间之后再进行消费）
   * @param timeUnit 延时时间单位
   * @return messageId
   */
  CompletableFuture<MessageId> sendDelayedAsync(
      String topic, String msg, long delay, TimeUnit timeUnit);


  /**
   * 异步发送延时消息至指定topic
   *
   * @param topic topic名称
   * @param tags  消息标签列表
   * @param msg 消息内容
   * @param delay 延时时间 （多少时间之后再进行消费）
   * @param timeUnit 延时时间单位
   * @return messageId
   */
  CompletableFuture<MessageId> sendDelayedAsyncTags(
      String topic, Collection<String> tags,String msg, long delay, TimeUnit timeUnit);

  /**
   * 异步发送消息至指定topic(有回调日志打印)
   *
   * @param topic topic名称
   * @param msg 消息内容
   */
  void sendRealTimeAsyncWithCallbackLog(String topic, String msg);


  /**
   * 异步发送消息至指定topic(有回调日志打印)
   *
   * @param topic topic名称
   * @param tags  消息标签列表
   * @param msg 消息内容
   */
  void sendRealTimeAsyncTagsWithCallbackLog(String topic,  Collection<String> tags,String msg);

  /**
   * 异步发送延时消息至指定topic(有回调日志打印)
   *
   * @param topic topic名称
   * @param msg 消息内容
   * @param delay 延时时间 （多少时间之后再进行消费）
   * @param timeUnit 延时时间单位
   */
  void sendDelayedAsyncWithCallbackLog(String topic, String msg, long delay, TimeUnit timeUnit);


  /**
   * 异步发送延时消息至指定topic(有回调日志打印)
   *
   * @param topic    topic名称
   * @param tags     消息标签列表
   * @param msg      消息内容
   * @param delay    延时时间 （多少时间之后再进行消费）
   * @param timeUnit 延时时间单位
   */
  void sendDelayedAsyncTagsWithCallbackLog(String topic, Collection<String> tags, String msg,
      long delay, TimeUnit timeUnit);
}
