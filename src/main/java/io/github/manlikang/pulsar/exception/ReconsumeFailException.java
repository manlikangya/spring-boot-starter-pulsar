package io.github.manlikang.pulsar.exception;

/**
 * @author fuhan
 * @date 2022/8/17 - 18:31
 */
public class ReconsumeFailException extends RuntimeException {
  public ReconsumeFailException(String message) {
    super(message);
  }
}
