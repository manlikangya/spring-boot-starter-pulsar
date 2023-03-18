package io.github.manlikang.pulsar.reconsume;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author fuhan
 * @date 2022/11/25 - 11:22
 */
public class ReconsumeMessagesImpl<T> implements Messages<T> {

  private final List<Message<T>> messageList;

  public ReconsumeMessagesImpl() {
    messageList = new ArrayList<>();
  }

  public void add(Message<T> message) {
    if (message == null) {
      return;
    }
    messageList.add(message);
  }

  @Override
  public int size() {
    return messageList.size();
  }

  @Override
  public Iterator<Message<T>> iterator() {
    return messageList.iterator();
  }
}
