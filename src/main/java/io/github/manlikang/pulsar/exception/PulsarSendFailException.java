package io.github.manlikang.pulsar.exception;

/**
 * @author fuhan
 * @des pulsar 发送失败异常
 * @date 2022/8/5 - 15:03
 */
public class PulsarSendFailException extends RuntimeException {

  public PulsarSendFailException(String message) {
    super(message);
  }

  public PulsarSendFailException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
