package io.github.manlikang.pulsar.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashSet;
import java.util.Set;

/**
 * @author fuhan
 */
@ConfigurationProperties(prefix = "pulsar")
@Getter
@Setter
public class PulsarProperties {
  private String serviceUrl = "pulsar://localhost:6650";
  private Integer ioThreads = 10;
  private Integer listenerThreads = 10;
  private boolean enableTcpNoDelay = false;
  private Integer keepAliveIntervalSec = 20;
  private Integer connectionTimeoutSec = 10;
  private Integer operationTimeoutSec = 15;
  private Integer startingBackoffIntervalMs = 100;
  private Integer maxBackoffIntervalSec = 10;
  private String consumerNameDelimiter = "";
  private String namespace = "default";
  private String tenant = "public";
  private String tlsTrustCertsFilePath = null;
  private Set<String> tlsCiphers = new HashSet<>();
  private Set<String> tlsProtocols = new HashSet<>();
  private String tlsTrustStorePassword = null;
  private String tlsTrustStorePath = null;
  private String tlsTrustStoreType = null;
  private boolean useKeyStoreTls = false;
  private boolean allowTlsInsecureConnection = false;
  private boolean enableTlsHostnameVerification = false;
  private String tlsAuthCertFilePath = null;
  private String tlsAuthKeyFilePath = null;
  private String tokenAuthValue = null;
}
