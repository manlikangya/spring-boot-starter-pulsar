

<a name="8050b8ac"></a>

# 1. 快速开始

<a name="d8a017b7"></a>

## 1. pom文件中引入依赖

```xml
<dependency>
    <groupId>io.github.manlikang</groupId>
    <artifactId>spring-boot-starter-pulsar</artifactId>
    <version>1.0.4-RELEASE</version>
</dependency>
```


<a name="ed6e9acd"></a>

## 2. 配置

<a name="tyJ8R"></a>

### 2.1 简单配置

```yaml
pulsar:
  # 命名空间
  namespace: '***********'
  # 服务地址
  service-url: '***********'
  # 集群名
  tenant: '***********'
  # 访问密钥
  token-auth-value: '***********'
  producer:
    # topic配置
    topics:
      -
        # Topic 名称
        name: topic1
  # 消费者配置，无需消费可以不用配置
  consumer:
    consumers:
      -
        # Topic 名称
        topic: topic1
        # 消费者beanName
        consumer-bean-name: topic1Consumer
        # 订阅名称
        subscription-name: sub-topic1
```

<a name="WqZbL"></a>

### 2.2 高级配置

```yaml
pulsar:
  # 连接超时时间，单位秒，默认为10秒
  connectionTimeoutSec: 15
  #操作超时时间 Producer-create、subscribe和unsubscribe操作将被重试，直到这个时间间隔，之后 操作将被标记为失败，默认15秒
  operation-timeout-sec: 15
  # 是否开启TCP无延迟，开启后吞吐量会下降，时延会降低。默认false
  enableTcpNoDelay: false
  # 设置用于处理broker连接的线程数,默认为10个
  ioThreads: 10
  # 设置消息监听的线程数 (默认为10个线程)。
  #侦听器线程池在所有消费者和读者之间共享 使用 “侦听器” 模型获取消息。对于给定的消费者，监听器将是 总是从同一线程调用，以确保排序。
  listenerThreads: 10
  # 命名空间
  namespace: '***********'
  # 服务地址
  service-url: '***********'
  # 集群名
  tenant: '***********'
  # 访问密钥
  token-auth-value: '***********'
  # 生产者配置,无生产者可以不用配置
  producer:
    # 是否将配置的生产者和pulsar建立连接
    enabled: true
    # topic配置
    topics:
      -
        # Topic 名称
        name: topic1
        # 是否是持久化topic
        persistent: true
        # 是否开启批量发送;该topic需要发送延时消息时，必须置为false，否则延时效果会失效
        batchingEnabled: false
        # 当发送等待队列堆满了之后，是否阻塞操作。
        # 设置为false，当发送等待队列被填满后，将会直接抛出发送失败异常
        # 设置为true 将会将当前操作阻塞至发送等待队列有空闲后再继续执行
        block-if-queue-full: true
        # 发送等待队列大小 默认1000
        max-pending-messages: 5000
        # 消息发送超时时间 单位ms，默认30000
        send-timeout-ms: 30000
        # 压缩类型，可选值 none, lz4,  zlib,  zstd, snappy。默认为none
        compression-type: none

  # 消费者配置，无需消费可以不用配置
  consumer:
    # 所有消费者共用，默认配置
    default:
      # 是否启用消费，置为false将不会去创建配置好的消费者
      enable: true
      # receive-queue队列大小，默认为1000，如果消息处理很慢，建议调小该值，最小可为1
      receive-queue-size: 1000
      # 消息从receive-queue出去，到回复ack的超时时间。默认为30000
      ack-timeout-ms: 60000
      # 每个Topic需要创建的消费者数量,默认为1
      consumer-num: 1
      # 每个消费者处理线程数,默认为1
      consumer-threads: 4
      # 最大重试次数，默认为16
      max-redeliver-count: 16
      # 批量接收有效，批量接收超时时间(ms),默认100
      batch-receive-timeout-ms: 100
      # 批量接收有效，批量接收最大字节数，单位byte,默认10M
      batch-receive-max-num-bytes: 10485760
      # 批量接收有效，批量接收最大消息数，默认50
      batch-receive-max-num-messages: 50
      # 开启重试有效，重试策略，
      # 可选值:
      # 1. active 立即重试
      # 2. fixedInterval 固定间隔重试时间
      # 3. progressive 递进式重试，间隔时间自己设置，最大支持间隔重试时间为24小时
      #    当设置的最大重试次数 > 配置的 progressive-reconsume-interval-ms 数量时，
      #    剩余的重试将会按照 progressive-reconsume-interval-ms 第一项来设置超时时间
      #    例： 设置max-redeliver-count 为3 ，progressive-reconsume-interval-ms 为 [1000,2000]
      #         那么三次重试时间的间隔为 1000，2000，1000
      # 4. fixedRate 固定倍率，时间基数为60s, 重试间隔为 60, 60*倍率, 60*倍率*倍率, 60*倍率*倍率*倍率 ... 最大为24小时
      # 5. discard 直接丢弃
      # 默认为progressive
      reconsume-strategy: progressive
      # 开启重试，且重试策略为fixedInterval有效，固定重试时间间隔，单位毫秒。默认为5000
      fixed-reconsume-interval-ms: 5000
      # 开启重试, 且重试策略为fixedRate有效，重试间隔倍率，默认为1.5。倍率基数为60s
      fixed-rate-reconsume: 1.5d
      # 开启重试, 且重试策略为progressive有效，递进式重试间隔时间，单位毫秒。默认为如下值
      progressive-reconsume-interval-ms:
        - 1000
        - 5000
        - 10000
        - 30000
        - 60000
        - 120000
        - 300000
        - 600000
        - 1800000
        - 3600000
        - 7200000

    consumers:
      -
        # 队列名
        topic: topic1
        # 消费者beanName
        consumer-bean-name: topic1Consumer
        # 订阅名称，
        #默认为 SUBSCRIPTION-${spring-application-name}-TopicName.toUpperCase() (全大写)
        #建议自定义
        subscription-name: sub-topic1
        # 订阅类型
        subscriptionType: shared
        # 消费者名称，不指定时为默认值
        # 默认为 CONSUMER-${spring-application-name}.toUpperCase() (全大写)
        # 建议使用默认值
        #consumer-name:
        # 是否开启重试，开启重试时需要创建对应的重试队列。重试队列名= ${subscription-name} + "-RETRY"
        enable-retry: true
        # 是否开启对该订阅的消费
        enable: true

        #死信topic,
        #当消息达到最大重试次数任然没有被成功消费后，会被投递至该Topic
        #当开启重试时有效，不指定则取默认值 ${subscriptionName} + "-DLQ"
        #建议使用默认值
        #dead-letter-topic:

        #重试topic,
        #当消息消费失败后，会将消息投递至该topic中进行重试。
        #启动时会自动订阅重试Topic 当
        #开启重试时有效，不指定则取默认值 ${subscriptionName} + "-RETRY"
        #建议使用默认值
        #retry-letter-topic:

        # 订阅后开始消费位置
        subscription-initial-position: latest
        # 是否持久化topic
        persistent: true
        # receive-queue队列大小，默认取default配置，如果消息处理很慢，建议调小该值，最小可为1
        receive-queue-size: 1000
        # 消息从receive-queue出去，到回复ack的超时时间。默认取default配置
        ack-timeout-ms: 60000
        # 最大重试次数。默认取default配置
        max-redeliver-count: 16
        # 批量接收有效，批量接收超时时间(ms)，默认取default配置
        batch-receive-timeout-ms: 100
        # 批量接收有效，批量接收最大字节数，单位byte,默认取default配置
        batch-receive-max-num-bytes: 10485760
        # 批量接收有效，批量接收最大消息数，默认取default配置
        batch-receive-max-num-messages: 50
        # 开启重试有效，重试策略，默认取default配置
        reconsume-strategy: progressive
        # 开启重试，且重试策略为fixedInterval有效，固定重试时间间隔，单位毫秒。默认取default配置
        fixed-reconsume-interval-ms: 5000
        # 开启重试, 且重试策略为fixedRate有效，重试间隔倍率，默认取default配置。倍率基数为60s
        fixed-rate-reconsume: 1.5d
        # 开启重试, 且重试策略为progressive有效，递进式重试间隔时间，单位毫秒。默认取default配置
        progressive-reconsume-interval-ms:
          - 1000
          - 5000
          - 10000
          - 30000
          - 60000
          - 120000
          - 300000
          - 600000
          - 1800000
          - 3600000
          - 7200000
```

<a name="2f113da5"></a>

## 3. 发送消息

直接注入PulsarProducer即可发送消息

```java
@Component
@Slf4j
public class ProducerDemo {

	@Resource
	private PulsarProducer pulsarProducer;

	public void sendMsg(String topic, String msg){
		pulsarProducer.sendRealTimeAsync(topic,msg);
	}
}
```

PulsarProducer还提供了多种发送消息的方法

```java
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
	 * 异步发送消息至指定topic
	 *
	 * @param topic topic名称
	 * @param msg 消息内容
	 * @return CompletableFuture<MessageId>
	 */
	CompletableFuture<MessageId> sendRealTimeAsync(String topic, String msg);

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
	 * 异步发送消息至指定topic(有回调日志打印)
	 *
	 * @param topic topic名称
	 * @param msg 消息内容
	 */
	void sendRealTimeAsyncWithCallbackLog(String topic, String msg);

	/**
	 * 异步发送延时消息至指定topic(有回调日志打印)
	 *
	 * @param topic topic名称
	 * @param msg 消息内容
	 * @param delay 延时时间 （多少时间之后再进行消费）
	 * @param timeUnit 延时时间单位
	 */
	void sendDelayedAsyncWithCallbackLog(String topic, String msg, long delay, TimeUnit timeUnit);
}
```



<a name="e5645078"></a>

## 4.消费消息

消费者需要实现相关接口，通过实现不同的接口，来确认是批量接收还是单条接收模式

1. PulsarStringConsumer ：单条String格式接收

```java
/**
* @author fuhan
* @date 2022/12/1 - 17:41
*/
@Component("topic1Consumer")
@Slf4j
public class Topic1Consumer implements PulsarStringConsumer {
    @Override
    public void onMessage(String msg) {
        log.info("收到消息:{}", msg);
    }
}
```

2. PulsarStringListConsumer: 批量String格式接收。实现该接口，将进入批量接收模式，批量接收配置将会生效

```java
/**
* @author fuhan
* @date 2022/12/1 - 17:41
*/
@Component("topic1Consumer")
@Slf4j
public class Topic1Consumer implements PulsarStringListConsumer {

    @Override
    public void onMessage(List<String> msgList) {
        log.info("收到消息:{}", msgList);
    }
}
```

3. PulsarMessageConsumer: 单条原生Message格式接收

```java
/**
* @author fuhan
* @date 2022/12/1 - 17:41
*/
@Component("topic1Consumer")
@Slf4j
public class Topic1Consumer implements PulsarMessageConsumer {

    @Override
    public void onMessage(PulsarMessage<byte[]> pulsarMessage) {
        final String msg = new String(pulsarMessage.getValue(), StandardCharsets.UTF_8);
        final String msgId = pulsarMessage.getMessageId().toString();
        log.info("收到消息:{}, 消息ID：{}", msg, msgId);
    }
}
```

4. PulsarMessageListConsumer: 批量原生Message格式接收。实现该接口，将进入批量接收模式，批量接收配置将会生效

```java
/**
* @author fuhan
* @date 2022/12/1 - 17:41
*/
@Component("topic1Consumer")
@Slf4j
public class Topic1Consumer implements PulsarMessageListConsumer {

    @Override
    public void onMessage(List<PulsarMessage<byte[]>> pulsarMessages) {
        for (final PulsarMessage<byte[]> pulsarMessage : pulsarMessages) {
            final String msg = new String(pulsarMessage.getValue(), StandardCharsets.UTF_8);
            final String msgId = pulsarMessage.getMessageId().toString();
            log.info("收到消息:{}, 消息ID：{}", msg, msgId);
        }
    }
}
```

<a name="138a6766"></a>

### 注意

消费者中配置的beanName，要确保在spring容器中能找到，否则会启动失败<br />`@Component("topic1Consumer")` 要和配置文件中的 `consumer-bean-name: topic1Consumer` 保持一致
<a name="6cf416d2"></a>

# 2. 原理解析

<a name="8f9c5799"></a>

## 1. 客户端和服务端交互模型

从下图可以看出，Pulsar发送消息是发送到Pulsar服务端，服务端存储之后，再用服务端对消费者进行推送，每个消费者都有一个receive-queue, 服务端会感知到receive-queue是否满了，没满会持续往里面推送消息，消费者拉取消息消费就是从receive-queue中拉取消费<br />![](https://cdn.nlark.com/yuque/0/2022/jpeg/1574769/1672383183158-d122add7-cb50-4632-9b86-5b9a2327d035.jpeg)
<a name="23d60cdb"></a>

## 2. Q&A

**问：生产者可以无限制生产消息吗**？<br />**答：**不可以，每个Topic都有单分区堆积上限。达到上限之后将无法再继续发送消息。

**问：receive-queue是否是越大越好？**<br />**答：**不是，Pulsar推送消息的时候会关注一个unAck（已经出了receive-queue，但是还没有回复ACK）数量指标。如果达到一定值(动态评估)，则会暂停推送。receive-queue越大，其占用的内存也会越大，拉取到的消息如果处理很慢，就会导致触发unAck上限导致服务端停止推送。

**问：消息从什么时候开始算超时时间？**<br />**答：**被消费者从receive-queue中取出之后开始算

