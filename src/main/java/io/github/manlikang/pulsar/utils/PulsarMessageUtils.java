package io.github.manlikang.pulsar.utils;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.github.manlikang.pulsar.message.PulsarMessage;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author fuhan
 * @date 2022/11/25 - 11:35
 */
public class PulsarMessageUtils {

  public static List<String> getMessageTags(PulsarMessage<byte[]> pulsarMessage) {
    final Map<String, String> properties = pulsarMessage.getProperties();
    if (properties == null || properties.size() == 0) {
      return Collections.emptyList();
    }
    List<String> tags = new ArrayList<>();
    properties.forEach(
        (k, v) -> {
          if (v.equals("TAGS")) {
            tags.add(k);
          }
        });
    return tags;
  }

  public static List<PulsarMessage<byte[]>> wrapMessageList(Messages<?> messageList) {
    List<PulsarMessage<byte[]>> pulsarMessageList = new ArrayList<>();
    for (Message<?> message : messageList) {
      pulsarMessageList.add(wrapMessage(message));
    }
    return pulsarMessageList;
  }

  public static <T> List<String> valueToStringList(Messages<T> msgList) {
    List<String> messageList = new ArrayList<>();
    for (final Message<T> msg : msgList) {
      messageList.add(new String(msg.getData(), StandardCharsets.UTF_8));
    }
    return messageList;
  }

  public static PulsarMessage<byte[]> wrapMessage(Message<?> message) {
    final PulsarMessage<byte[]> pulsarMessage = new PulsarMessage<>();
    pulsarMessage.setValue(message.getData());
    pulsarMessage.setMessageId(message.getMessageId());
    pulsarMessage.setSequenceId(message.getSequenceId());
    pulsarMessage.setProperties(message.getProperties());
    pulsarMessage.setTopicName(message.getTopicName());
    pulsarMessage.setKey(message.getKey());
    pulsarMessage.setEventTime(message.getEventTime());
    pulsarMessage.setPublishTime(message.getPublishTime());
    pulsarMessage.setProducerName(message.getProducerName());
    return pulsarMessage;
  }

  public static List<String> getMessageId(Messages<?> messageList) {
    List<String> list = new ArrayList<>(messageList.size());
    for (final Message<?> message : messageList) {
      list.add(message.getMessageId().toString());
    }
    return list;
  }

  public static <T> String getMessageLog(Messages<T> messages) {
    List<JSONObject> list = new ArrayList<>(messages.size());
    for (final Message<T> message : messages) {
      list.add(getMessageJsonObj(message));
    }
    return JSON.toJSONString(list);
  }

  public static <T> String getMessageLog(Message<T> message) {
    return JSON.toJSONString(getMessageJsonObj(message));
  }

  private static <T> JSONObject getMessageJsonObj(Message<T> message) {
    final String s = new String(message.getData(), StandardCharsets.UTF_8);
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("pulsarMessageId", message.getMessageId().toString());
    jsonObject.put("pulsarMessage", s);
    if (isTypeJSONArray(s)) {
      final JSONArray jsonArray = JSONObject.parseArray(s);
      jsonObject.put("pulsarMessage", jsonArray);
    } else if (isTypeJSONObject(s)) {
      final JSONObject object = JSONObject.parseObject(s);
      object.forEach(
          (key, value) -> {
            if (value instanceof String) {
              String content = (String) value;
              if (isTypeJSONObject(content)) {
                final JSONObject contentJsonObj = JSONObject.parseObject(content);
                object.put(key, contentJsonObj);
              } else if (isTypeJSONArray(content)) {
                final JSONArray contentJsonArray = JSONObject.parseArray(content);
                object.put(key, contentJsonArray);
              }
            }
          });
      jsonObject.put("pulsarMessage", object);
    }
    return jsonObject;
  }

  /**
   * 是否为JSONArray类型的字符串，首尾都为中括号判定为JSONArray字符串
   *
   * @param str 字符串
   * @return 是否为JSONArray类型字符串
   * @since 5.7.22
   */
  public static boolean isTypeJSONArray(String str) {
    if (isBlank(str)) {
      return false;
    }
    return isWrap(trim(str), '[', ']');
  }

  /**
   * 是否为JSONObject类型字符串，首尾都为大括号判定为JSONObject字符串
   *
   * @param str 字符串
   * @return 是否为JSON字符串
   * @since 5.7.22
   */
  public static boolean isTypeJSONObject(String str) {
    if (isBlank(str)) {
      return false;
    }
    return isWrap(trim(str), '{', '}');
  }

  public static String trim(String str) {
    if (isBlank(str)) {
      return "";
    }
    return str.trim();
  }

  public static boolean isBlank(String str) {
    return str == null || str.trim().isEmpty();
  }

  /**
   * 指定字符串是否被包装
   *
   * @param str 字符串
   * @param prefixChar 前缀
   * @param suffixChar 后缀
   * @return 是否被包装
   */
  public static boolean isWrap(CharSequence str, char prefixChar, char suffixChar) {
    if (null == str) {
      return false;
    }

    return str.charAt(0) == prefixChar && str.charAt(str.length() - 1) == suffixChar;
  }

  /**
   * 根据英文逗号分割字符串并转换为列表
   *
   * @param value 原字符串
   * @return 分割后的数据列表
   */
  public static List<String> splitStrByComma(String value) {
    if (isBlank(value)) {
      return Collections.emptyList();
    }
    return Stream.of(value.split(",")).filter(str -> !isBlank(str)).collect(Collectors.toList());
  }
}
