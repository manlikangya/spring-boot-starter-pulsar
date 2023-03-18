package io.github.manlikang.pulsar.utils;


import com.google.common.base.Strings;
import io.github.manlikang.pulsar.properties.PulsarProperties;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

/**
 * @author fuhan
 */
@Service
public class UrlBuildService {

  private static final String PERSISTENT_PREFIX = "persistent";
  private static final String NON_PERSISTENT_PREFIX = "non-persistent";
  private static final String DEFAULT_PERSISTENCE = PERSISTENT_PREFIX;
  private static final String CONSUMER_NAME_PREFIX = "consumer";
  private static final String SUBSCRIPTION_NAME_PREFIX = "subscription";


  private final PulsarProperties pulsarProperties;

  private UrlBuildService(PulsarProperties pulsarProperties) {
    this.pulsarProperties = pulsarProperties;
  }

  public String buildTopicUrl(String topic) {
    return DEFAULT_PERSISTENCE
        + "://"
        + pulsarProperties.getTenant()
        + "/"
        + pulsarProperties.getNamespace()
        + "/"
        + topic;
  }

  public String buildTopicUrl(String topic, boolean persistent) {
    String prefix = persistent ? DEFAULT_PERSISTENCE : NON_PERSISTENT_PREFIX;
    return prefix
        + "://"
        + pulsarProperties.getTenant()
        + "/"
        + pulsarProperties.getNamespace()
        + "/"
        + topic;
  }

  public String buildPulsarConsumerName(String customConsumerName) {
    if (Strings.isNullOrEmpty(customConsumerName)) {
      final String applicationName = SpringHolder.getApplicationName();
      final String activeProfile = SpringHolder.getActiveProfile();
      if(StringUtils.isBlank(applicationName) && StringUtils.isBlank(activeProfile)){
        return CONSUMER_NAME_PREFIX.toUpperCase();
      }else if(StringUtils.isBlank(applicationName) && StringUtils.isNotBlank(activeProfile)){
        return (CONSUMER_NAME_PREFIX + "-" + activeProfile).toUpperCase();
      }else if(StringUtils.isNotBlank(applicationName) && StringUtils.isBlank(activeProfile)){
        return applicationName.toUpperCase();
      }
      return (applicationName + "-" + activeProfile).toUpperCase();
    }

    return customConsumerName;
  }

  public String buildPulsarSubscriptionName(String customSubscriptionName, String topicName) {
    if (StringUtils.isBlank(customSubscriptionName)) {
      final String applicationName = SpringHolder.getApplicationName();
      return (SUBSCRIPTION_NAME_PREFIX
              + "-"
              + applicationName
              + "-"
              + topicName)
          .toUpperCase();
    }
    return customSubscriptionName;
  }
}
