package io.github.manlikang.pulsar.exception;

/**
 * @author fuhan
 * @date 2022/11/25 - 16:36
 */
public class TopicProducerNotConfigException extends RuntimeException {

  public TopicProducerNotConfigException(String message) {
    super(message);
  }
}
