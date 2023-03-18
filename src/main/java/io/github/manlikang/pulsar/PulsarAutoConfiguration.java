package io.github.manlikang.pulsar;


import com.google.common.base.Strings;
import io.github.manlikang.pulsar.error.exception.ClientInitException;
import io.github.manlikang.pulsar.producer.PulsarTemplate;
import io.github.manlikang.pulsar.producer.sender.DefaultMessageSendFailHandler;
import io.github.manlikang.pulsar.producer.sender.DefaultPulsarProducer;
import io.github.manlikang.pulsar.producer.sender.MessageSendFailHandler;
import io.github.manlikang.pulsar.producer.sender.PulsarProducer;
import io.github.manlikang.pulsar.properties.PulsarProperties;
import io.github.manlikang.pulsar.properties.consumer.ConsumerDefaultProperties;
import io.github.manlikang.pulsar.properties.consumer.ConsumerProperties;
import io.github.manlikang.pulsar.properties.producer.ProducerProperties;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * @author fuhan
 */
@Configuration
@ComponentScan
@EnableConfigurationProperties({
  PulsarProperties.class,
  ProducerProperties.class,
  ConsumerProperties.class,
  ConsumerDefaultProperties.class
})
public class PulsarAutoConfiguration {

  private final PulsarProperties pulsarProperties;

  public PulsarAutoConfiguration(PulsarProperties pulsarProperties) {
    this.pulsarProperties = pulsarProperties;
  }

  @Bean
  @ConditionalOnMissingBean
  public PulsarProducer defaultPulsarProducer(
          PulsarTemplate<byte[]> pulsarTemplate, MessageSendFailHandler messageSendFailHandler) {
    return new DefaultPulsarProducer(pulsarTemplate, messageSendFailHandler);
  }

  @Bean
  @ConditionalOnMissingBean
  public MessageSendFailHandler messageSendFailHandler() {
    return new DefaultMessageSendFailHandler();
  }

  @Bean
  @ConditionalOnMissingBean
  public PulsarClient pulsarClient() throws PulsarClientException, ClientInitException {
    if (!Strings.isNullOrEmpty(pulsarProperties.getTlsAuthCertFilePath())
        && !Strings.isNullOrEmpty(pulsarProperties.getTlsAuthKeyFilePath())
        && !Strings.isNullOrEmpty(pulsarProperties.getTokenAuthValue())) {
      throw new ClientInitException("You cannot use multiple auth options.");
    }

    final ClientBuilder pulsarClientBuilder =
        PulsarClient.builder()
            .serviceUrl(pulsarProperties.getServiceUrl())
            .ioThreads(pulsarProperties.getIoThreads())
            .listenerThreads(pulsarProperties.getListenerThreads())
            .enableTcpNoDelay(pulsarProperties.isEnableTcpNoDelay())
            .keepAliveInterval(pulsarProperties.getKeepAliveIntervalSec(), TimeUnit.SECONDS)
            .connectionTimeout(pulsarProperties.getConnectionTimeoutSec(), TimeUnit.SECONDS)
            .operationTimeout(pulsarProperties.getOperationTimeoutSec(), TimeUnit.SECONDS)
            .startingBackoffInterval(
                pulsarProperties.getStartingBackoffIntervalMs(), TimeUnit.MILLISECONDS)
            .maxBackoffInterval(pulsarProperties.getMaxBackoffIntervalSec(), TimeUnit.SECONDS)
            .useKeyStoreTls(pulsarProperties.isUseKeyStoreTls())
            .tlsTrustCertsFilePath(pulsarProperties.getTlsTrustCertsFilePath())
            .tlsCiphers(pulsarProperties.getTlsCiphers())
            .tlsProtocols(pulsarProperties.getTlsProtocols())
            .tlsTrustStorePassword(pulsarProperties.getTlsTrustStorePassword())
            .tlsTrustStorePath(pulsarProperties.getTlsTrustStorePath())
            .tlsTrustStoreType(pulsarProperties.getTlsTrustStoreType())
            .allowTlsInsecureConnection(pulsarProperties.isAllowTlsInsecureConnection())
            .enableTlsHostnameVerification(pulsarProperties.isEnableTlsHostnameVerification());

    if (!Strings.isNullOrEmpty(pulsarProperties.getTlsAuthCertFilePath())
        && !Strings.isNullOrEmpty(pulsarProperties.getTlsAuthKeyFilePath())) {
      pulsarClientBuilder.authentication(
          AuthenticationFactory.TLS(
              pulsarProperties.getTlsAuthCertFilePath(), pulsarProperties.getTlsAuthKeyFilePath()));
    }

    if (!Strings.isNullOrEmpty(pulsarProperties.getTokenAuthValue())) {
      pulsarClientBuilder.authentication(
          AuthenticationFactory.token(pulsarProperties.getTokenAuthValue()));
    }

    return pulsarClientBuilder.build();
  }
}
