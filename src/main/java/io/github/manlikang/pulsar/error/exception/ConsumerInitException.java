package io.github.manlikang.pulsar.error.exception;

public class ConsumerInitException extends RuntimeException {
  public ConsumerInitException(String message, Throwable cause) {
    super(message, cause);
  }
}
