package io.github.manlikang.pulsar.producer;

import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;

/**
 * @author fuhan
 * @date 2022/8/10 - 18:23
 */
@Getter
@Setter
public class ProducerConfiguration {

  /** 是否开启批处理 更多配置项请自行添加: {@link ProducerConfigurationData} */
  private boolean batchingEnabled = true;

  private boolean persistent = true;

  /**
   * 设置包含待处理消息的队列的最大大小，以便从broker接收确认。
   * 当队列已满时，默认的，所有的调用都会失败，除非blockIfQueueFull设置为true。可以使用blockIfQueueFull来改变这个行为。
   */
  private int maxPendingMessages;

  /** 当输出（outgoing）队列已满时，是否停止相应的操作 */
  private boolean blockIfQueueFull;

  /** 发送超时时间 (ms) */
  private int sendTimeoutMs;

  /**
   * Set the compression type for the producer.
   *
   * <p>By default, message payloads are not compressed. Supported compression types are:
   *
   * <ul>
   *   <li>{@link CompressionType#NONE}: No compression (Default)
   *   <li>{@link CompressionType#LZ4}: Compress with LZ4 algorithm. Faster but lower compression
   *       than ZLib
   *   <li>{@link CompressionType#ZLIB}: Standard ZLib compression
   *   <li>{@link CompressionType#ZSTD} Compress with Zstandard codec. Since Pulsar 2.3. Zstd cannot
   *       be used if consumer applications are not in version >= 2.3 as well
   *   <li>{@link CompressionType#SNAPPY} Compress with Snappy codec. Since Pulsar 2.4. Snappy
   *       cannot be used if consumer applications are not in version >= 2.4 as well
   * </ul>
   */
  private CompressionType compressionType = CompressionType.NONE;

  public static ProducerConfigurationBuilder builder() {
    return new ProducerConfigurationBuilder();
  }

  public static class ProducerConfigurationBuilder {

    /** 是否开启批处理 */
    private boolean batchingEnabled = true;

    private boolean persistent = true;

    /**
     * 设置包含待处理消息的队列的最大大小，以便从broker接收确认。
     * 当队列已满时，默认的，所有的调用都会失败，除非blockIfQueueFull设置为true。可以使用blockIfQueueFull来改变这个行为。
     */
    private int maxPendingMessages;

    /** 当输出（outgoing）队列已满时，是否停止相应的操作 */
    private boolean blockIfQueueFull;

    /** 发送超时时间 (ms) */
    private int sendTimeoutMs;

    /**
     * Set the compression type for the producer.
     *
     * <p>By default, message payloads are not compressed. Supported compression types are:
     *
     * <ul>
     *   <li>{@link CompressionType#NONE}: No compression (Default)
     *   <li>{@link CompressionType#LZ4}: Compress with LZ4 algorithm. Faster but lower compression
     *       than ZLib
     *   <li>{@link CompressionType#ZLIB}: Standard ZLib compression
     *   <li>{@link CompressionType#ZSTD} Compress with Zstandard codec. Since Pulsar 2.3. Zstd
     *       cannot be used if consumer applications are not in version >= 2.3 as well
     *   <li>{@link CompressionType#SNAPPY} Compress with Snappy codec. Since Pulsar 2.4. Snappy
     *       cannot be used if consumer applications are not in version >= 2.4 as well
     * </ul>
     */
    private CompressionType compressionType;

    public ProducerConfigurationBuilder compressionType(CompressionType compressionType) {
      this.compressionType = compressionType;
      return this;
    }

    public ProducerConfigurationBuilder blockIfQueueFull(boolean blockIfQueueFull) {
      this.blockIfQueueFull = blockIfQueueFull;
      return this;
    }

    public ProducerConfigurationBuilder batchingEnabled(boolean batchingEnabled) {
      this.batchingEnabled = batchingEnabled;
      return this;
    }

    public ProducerConfigurationBuilder persistent(boolean persistent) {
      this.persistent = persistent;
      return this;
    }

    public ProducerConfigurationBuilder maxPendingMessages(int maxPendingMessages) {
      this.maxPendingMessages = maxPendingMessages;
      return this;
    }

    public ProducerConfigurationBuilder sendTimeoutMs(int sendTimeoutMs) {
      this.sendTimeoutMs = sendTimeoutMs;
      return this;
    }

    public ProducerConfiguration build() {
      ProducerConfiguration configuration = new ProducerConfiguration();
      configuration.setBatchingEnabled(batchingEnabled);
      configuration.setPersistent(persistent);
      configuration.setMaxPendingMessages(maxPendingMessages);
      configuration.setBlockIfQueueFull(blockIfQueueFull);
      configuration.setSendTimeoutMs(sendTimeoutMs);
      configuration.setCompressionType(compressionType);
      return configuration;
    }
  }
}
