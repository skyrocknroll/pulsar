/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl.conf;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.opentelemetry.api.OpenTelemetry;
import io.swagger.annotations.ApiModelProperty;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.util.Secret;
import org.apache.pulsar.common.util.DefaultPulsarSslFactory;


/**
 * This is a simple holder of the client configuration values.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClientConfigurationData implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    @ApiModelProperty(
            name = "serviceUrl",
            required = true,
            value = "Pulsar cluster HTTP URL to connect to a broker."
    )
    private String serviceUrl;
    @ApiModelProperty(
            name = "serviceUrlProvider",
            value = "The implementation class of ServiceUrlProvider used to generate ServiceUrl."
    )
    @JsonIgnore
    private transient ServiceUrlProvider serviceUrlProvider;

    @ApiModelProperty(
            name = "serviceUrlQuarantineInitDurationMs",
            value = "The initial duration (in milliseconds) to quarantine endpoints that fail to connect."
                    + "A value of 0 means don't quarantine any endpoints even if they fail."
    )
    private long serviceUrlQuarantineInitDurationMs = 60000;

    @ApiModelProperty(
            name = "serviceUrlQuarantineMaxDurationMs",
            value = "The max duration (in milliseconds) to quarantine endpoints that fail to connect."
                    + "A value of 0 means don't quarantine any endpoints even if they fail."
    )
    private long serviceUrlQuarantineMaxDurationMs = TimeUnit.DAYS.toMillis(1);

    @ApiModelProperty(
            name = "authentication",
            value = "Authentication settings of the client."
    )
    @JsonIgnore
    private Authentication authentication;

    @ApiModelProperty(
            name = "authPluginClassName",
            value = "Class name of authentication plugin of the client."
    )
    private String authPluginClassName;

    @ApiModelProperty(
            name = "authParams",
            value = "Authentication parameter of the client."
    )
    @Secret
    private String authParams;

    @ApiModelProperty(
            name = "authParamMap",
            value = "Authentication map of the client."
    )
    @Secret
    private Map<String, String> authParamMap;

    @ApiModelProperty(
            name = "originalPrincipal",
            value = "Original principal for proxy authentication scenarios."
    )
    private String originalPrincipal;

    @ApiModelProperty(
            name = "operationTimeoutMs",
            value = "Client operation timeout (in milliseconds)."
    )
    private long operationTimeoutMs = 30000;

    @ApiModelProperty(
            name = "lookupTimeoutMs",
            value = "Client lookup timeout (in milliseconds)."
    )
    private long lookupTimeoutMs = -1;

    @ApiModelProperty(
            name = "statsIntervalSeconds",
            value = "Interval to print client stats (in seconds)."
    )
    private long statsIntervalSeconds = 60;

    @ApiModelProperty(
            name = "numIoThreads",
            value = "Number of IO threads."
    )
    private int numIoThreads = Runtime.getRuntime().availableProcessors();

    @ApiModelProperty(
            name = "numListenerThreads",
            value = "Number of consumer listener threads."
    )
    private int numListenerThreads = Runtime.getRuntime().availableProcessors();

    @ApiModelProperty(
            name = "connectionsPerBroker",
            value = "Number of connections established between the client and each Broker."
                    + " A value of 0 means to disable connection pooling."
    )
    private int connectionsPerBroker = 1;

    @ApiModelProperty(
            name = "connectionMaxIdleSeconds",
            value = "Release the connection if it is not used for more than [connectionMaxIdleSeconds] seconds. "
                    + "If  [connectionMaxIdleSeconds] < 0, disabled the feature that auto release the idle connections"
    )
    private int connectionMaxIdleSeconds = 60;

    @ApiModelProperty(
            name = "useTcpNoDelay",
            value = "Whether to use TCP NoDelay option."
    )
    private boolean useTcpNoDelay = true;

    @ApiModelProperty(
            name = "useTls",
            value = "Whether to use TLS."
    )
    private boolean useTls = false;

    @ApiModelProperty(
            name = "tlsKeyFilePath",
            value = "Path to the TLS key file."
    )
    private String tlsKeyFilePath = null;

    @ApiModelProperty(
            name = "tlsCertificateFilePath",
            value = "Path to the TLS certificate file."
    )
    private String tlsCertificateFilePath = null;

    @ApiModelProperty(
            name = "tlsTrustCertsFilePath",
            value = "Path to the trusted TLS certificate file."
    )
    private String tlsTrustCertsFilePath = null;

    @ApiModelProperty(
            name = "tlsAllowInsecureConnection",
            value = "Whether the client accepts untrusted TLS certificates from the broker."
    )
    private boolean tlsAllowInsecureConnection = false;

    @ApiModelProperty(
            name = "tlsHostnameVerificationEnable",
            value = "Whether the hostname is validated when the client creates a TLS connection with brokers."
    )
    private boolean tlsHostnameVerificationEnable = false;

    @ApiModelProperty(
            name = "sslFactoryPlugin",
            value = "SSL Factory Plugin class to provide SSLEngine and SSLContext objects. The default "
                    + " class used is DefaultPulsarSslFactory.")
    private String sslFactoryPlugin = DefaultPulsarSslFactory.class.getName();

    @ApiModelProperty(
            name = "sslFactoryPluginParams",
            value = "SSL Factory plugin configuration parameters.")
    private String sslFactoryPluginParams = "";

    @ApiModelProperty(
            name = "concurrentLookupRequest",
            value = "The number of concurrent lookup requests that can be sent on each broker connection. "
                    + "Setting a maximum prevents overloading a broker."
    )
    private int concurrentLookupRequest = 5000;

    @ApiModelProperty(
            name = "maxLookupRequest",
            value = "Maximum number of lookup requests allowed on "
                    + "each broker connection to prevent overloading a broker."
    )
    private int maxLookupRequest = 50000;

    @ApiModelProperty(
            name = "maxLookupRedirects",
            value = "Maximum times of redirected lookup requests."
    )
    private int maxLookupRedirects = 20;

    @ApiModelProperty(
            name = "maxNumberOfRejectedRequestPerConnection",
            value = "Maximum number of rejected requests of a broker in a certain time frame (60 seconds) "
                    + "after the current connection is closed and the client "
                    + "creating a new connection to connect to a different broker."
    )
    private int maxNumberOfRejectedRequestPerConnection = 50;

    @ApiModelProperty(
            name = "keepAliveIntervalSeconds",
            value = "Seconds of keeping alive interval for each client broker connection."
    )
    private int keepAliveIntervalSeconds = 30;

    @ApiModelProperty(
            name = "connectionTimeoutMs",
            value = "Duration of waiting for a connection to a broker to be established."
                    + "If the duration passes without a response from a broker, the connection attempt is dropped."
    )
    private int connectionTimeoutMs = 10000;
    @ApiModelProperty(
            name = "requestTimeoutMs",
            value = "Maximum duration for completing a request."
    )
    private int requestTimeoutMs = 60000;

    @ApiModelProperty(
            name = "readTimeoutMs",
            value = "Maximum read time of a request."
    )
    private int readTimeoutMs = 60000;

    @ApiModelProperty(
            name = "autoCertRefreshSeconds",
            value = "Seconds of auto refreshing certificate."
    )
    private int autoCertRefreshSeconds = 300;

    @ApiModelProperty(
            name = "initialBackoffIntervalNanos",
            value = "Initial backoff interval (in nanosecond)."
    )
    private long initialBackoffIntervalNanos = TimeUnit.MILLISECONDS.toNanos(100);

    @ApiModelProperty(
            name = "maxBackoffIntervalNanos",
            value = "Max backoff interval (in nanosecond)."
    )
    private long maxBackoffIntervalNanos = TimeUnit.SECONDS.toNanos(60);

    @ApiModelProperty(
            name = "enableBusyWait",
            value = "Whether to enable BusyWait for EpollEventLoopGroup."
    )
    private boolean enableBusyWait = false;

    @ApiModelProperty(
            name = "listenerName",
            value = "Listener name for lookup. Clients can use listenerName to choose one of the listeners "
                    + "as the service URL to create a connection to the broker as long as the network is accessible."
                    + "\"advertisedListeners\" must enabled in broker side."
    )
    private String listenerName;

    @ApiModelProperty(
            name = "useKeyStoreTls",
            value = "Set TLS using KeyStore way."
    )
    private boolean useKeyStoreTls = false;
    @ApiModelProperty(
            name = "sslProvider",
            value = "The TLS provider used by an internal client to authenticate with other Pulsar brokers."
    )
    private String sslProvider = null;

    @ApiModelProperty(
            name = "tlsKeyStoreType",
            value = "TLS KeyStore type configuration."
    )
    private String tlsKeyStoreType = "JKS";

    @ApiModelProperty(
            name = "tlsKeyStorePath",
            value = "Path of TLS KeyStore."
    )
    private String tlsKeyStorePath = null;

    @ApiModelProperty(
            name = "tlsKeyStorePassword",
            value = "Password of TLS KeyStore."
    )
    @Secret
    private String tlsKeyStorePassword = null;

    @ApiModelProperty(
            name = "tlsTrustStoreType",
            value = "TLS TrustStore type configuration. You need to set this configuration when client authentication"
                    + " is required."
    )
    private String tlsTrustStoreType = "JKS";

    @ApiModelProperty(
            name = "tlsTrustStorePath",
            value = "Path of TLS TrustStore."
    )
    private String tlsTrustStorePath = null;

    @ApiModelProperty(
            name = "tlsTrustStorePassword",
            value = "Password of TLS TrustStore."
    )
    @Secret
    private String tlsTrustStorePassword = null;

    @ApiModelProperty(
            name = "tlsCiphers",
            value = "Set of TLS Ciphers."
    )
    private Set<String> tlsCiphers = new TreeSet<>();

    @ApiModelProperty(
            name = "tlsProtocols",
            value = "Protocols of TLS."
    )
    private Set<String> tlsProtocols = new TreeSet<>();

    @ApiModelProperty(
            name = "memoryLimitBytes",
            value = "Limit of client memory usage (in byte). The 64M default can guarantee a high producer throughput."
    )
    private long memoryLimitBytes = 64 * 1024 * 1024;

    @ApiModelProperty(
            name = "proxyServiceUrl",
            value = "URL of proxy service. proxyServiceUrl and proxyProtocol must be mutually inclusive."
    )
    private String proxyServiceUrl;

    @ApiModelProperty(
            name = "proxyProtocol",
            value = "Protocol of proxy service. proxyServiceUrl and proxyProtocol must be mutually inclusive."
    )
    private ProxyProtocol proxyProtocol;

    @ApiModelProperty(
            name = "enableTransaction",
            value = "Whether to enable transaction."
    )
    private boolean enableTransaction = false;

    @JsonIgnore
    private Clock clock = Clock.systemDefaultZone();

    @ApiModelProperty(
            name = "dnsLookupBindAddress",
            value = "The Pulsar client dns lookup bind address, default behavior is bind on 0.0.0.0"
    )
    private String dnsLookupBindAddress = null;

    @ApiModelProperty(
            name = "dnsLookupBindPort",
            value = "The Pulsar client dns lookup bind port, takes effect when dnsLookupBindAddress is configured,"
                    + " default value is 0."
    )
    private int dnsLookupBindPort = 0;

    @ApiModelProperty(
            name = "dnsServerAddresses",
            value = "The Pulsar client dns lookup server address"
    )
    @SuppressFBWarnings({"EI_EXPOSE_REP2", "EI_EXPOSE_REP"})
    private List<InetSocketAddress> dnsServerAddresses = new ArrayList<>();

    // socks5
    @ApiModelProperty(
            name = "socks5ProxyAddress",
            value = "Address of SOCKS5 proxy."
    )
    private InetSocketAddress socks5ProxyAddress;

    @ApiModelProperty(
            name = "socks5ProxyUsername",
            value = "User name of SOCKS5 proxy."
    )
    private String socks5ProxyUsername;

    @ApiModelProperty(
            name = "socks5ProxyPassword",
            value = "Password of SOCKS5 proxy."
    )
    @Secret
    private String socks5ProxyPassword;

    @ApiModelProperty(
            name = "description",
            value = "The extra description of the client version. The length cannot exceed 64."
    )
    private String description;

    private Map<String, String> lookupProperties;

    private transient OpenTelemetry openTelemetry;

    /**
     * Gets the authentication settings for the client.
     *
     * @return authentication settings for the client or {@link AuthenticationDisabled} when auth has not been specified
     */
    public Authentication getAuthentication() {
        return this.authentication != null ? this.authentication : AuthenticationDisabled.INSTANCE;
    }

    public void setAuthentication(Authentication authentication) {
        this.authentication = authentication;
    }

    public boolean isUseTls() {
        if (useTls) {
            return true;
        }
        if (getServiceUrl() != null
                && (this.getServiceUrl().startsWith("pulsar+ssl") || this.getServiceUrl().startsWith("https"))) {
            this.useTls = true;
            return true;
        }
        return false;
    }

    public long getLookupTimeoutMs() {
        if (lookupTimeoutMs >= 0) {
            return lookupTimeoutMs;
        } else {
            return operationTimeoutMs;
        }
    }

    public ClientConfigurationData clone() {
        try {
            return (ClientConfigurationData) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone ClientConfigurationData");
        }
    }

    public InetSocketAddress getSocks5ProxyAddress() {
        if (Objects.nonNull(socks5ProxyAddress)) {
            return socks5ProxyAddress;
        }
        String proxyAddress = System.getProperty("socks5Proxy.address");
        return Optional.ofNullable(proxyAddress).map(address -> {
            try {
                URI uri = URI.create(address);
                return new InetSocketAddress(uri.getHost(), uri.getPort());
            } catch (Exception e) {
                throw new RuntimeException("Invalid config [socks5Proxy.address]", e);
            }
        }).orElse(null);
    }

    public String getSocks5ProxyUsername() {
        return Objects.nonNull(socks5ProxyUsername) ? socks5ProxyUsername : System.getProperty("socks5Proxy.username");
    }

    public String getSocks5ProxyPassword() {
        return Objects.nonNull(socks5ProxyPassword) ? socks5ProxyPassword : System.getProperty("socks5Proxy.password");
    }

    public void setLookupProperties(Map<String, String> lookupProperties) {
        this.lookupProperties = Collections.unmodifiableMap(lookupProperties);
    }

    public Map<String, String> getLookupProperties() {
        return (lookupProperties == null) ? Collections.emptyMap() : Collections.unmodifiableMap(lookupProperties);
    }
}
