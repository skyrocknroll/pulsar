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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.net.URL;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ReplicatorTestBase extends TestRetrySupport {
    URL url1;
    URL urlTls1;
    ServiceConfiguration config1 = new ServiceConfiguration();
    PulsarService pulsar1;
    BrokerService ns1;
    protected InMemoryMetricReader metricReader1;

    PulsarAdmin admin1;
    LocalBookkeeperEnsemble bkEnsemble1;

    URL url2;
    URL urlTls2;
    ServiceConfiguration config2 = new ServiceConfiguration();
    PulsarService pulsar2;
    BrokerService ns2;
    PulsarAdmin admin2;
    LocalBookkeeperEnsemble bkEnsemble2;
    protected InMemoryMetricReader metricReader2;

    URL url3;
    URL urlTls3;
    ServiceConfiguration config3 = new ServiceConfiguration();
    PulsarService pulsar3;
    BrokerService ns3;
    PulsarAdmin admin3;
    LocalBookkeeperEnsemble bkEnsemble3;
    protected InMemoryMetricReader metricReader3;

    URL url4;
    URL urlTls4;
    ServiceConfiguration config4 = new ServiceConfiguration();
    PulsarService pulsar4;
    PulsarAdmin admin4;
    LocalBookkeeperEnsemble bkEnsemble4;
    protected InMemoryMetricReader metricReader4;

    ZookeeperServerTest globalZkS;

    ExecutorService executor;

    static final int TIME_TO_CHECK_BACKLOG_QUOTA = 5;

    // PEM
    protected final String brokerCertFilePath =
            Resources.getResource("certificate-authority/server-keys/broker.cert.pem").getPath();
    protected final String brokerFilePath =
            Resources.getResource("certificate-authority/server-keys/broker.key-pk8.pem").getPath();
    protected final String clientCertFilePath =
            Resources.getResource("certificate-authority/client-keys/admin.cert.pem").getPath();
    protected final String clientKeyFilePath =
            Resources.getResource("certificate-authority/client-keys/admin.key-pk8.pem").getPath();
    protected final String caCertFilePath =
            Resources.getResource("certificate-authority/certs/ca.cert.pem").getPath();

    // KEYSTORE
    protected boolean tlsWithKeyStore = false;
    protected final String brokerKeyStorePath =
            Resources.getResource("certificate-authority/jks/broker.keystore.jks").getPath();
    protected final String brokerTrustStorePath =
            Resources.getResource("certificate-authority/jks/broker.truststore.jks").getPath();
    protected final String clientKeyStorePath =
            Resources.getResource("certificate-authority/jks/client.keystore.jks").getPath();
    protected final String clientTrustStorePath =
            Resources.getResource("certificate-authority/jks/client.truststore.jks").getPath();
    protected final String keyStoreType = "JKS";
    protected final String keyStorePassword = "111111";

    protected final String cluster1 = "r1";
    protected final String cluster2 = "r2";
    protected final String cluster3 = "r3";
    protected final String cluster4 = "r4";
    protected String loadManagerClassName;

    protected String getLoadManagerClassName() {
        return loadManagerClassName;
    }

    // Default frequency
    public int getBrokerServicePurgeInactiveFrequency() {
        return 60;
    }

    public boolean isBrokerServicePurgeInactiveTopic() {
        return false;
    }

    @Override
    protected void setup() throws Exception {
        incrementSetupNumber();

        log.info("--- Starting ReplicatorTestBase::setup ---");
        executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                new DefaultThreadFactory("ReplicatorTestBase"));

        globalZkS = new ZookeeperServerTest(0);
        globalZkS.start();

        // Start region 1
        bkEnsemble1 = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble1.start();

        // NOTE: we have to instantiate a new copy of System.getProperties() to make sure pulsar1 and pulsar2 have
        // completely
        // independent config objects instead of referring to the same properties object
        setConfig1DefaultValue();
        metricReader1 = InMemoryMetricReader.create();
        pulsar1 = buildPulsarService(config1, metricReader1);
        pulsar1.start();
        ns1 = pulsar1.getBrokerService();

        url1 = new URL(pulsar1.getWebServiceAddress());
        urlTls1 = new URL(pulsar1.getWebServiceAddressTls());
        admin1 = PulsarAdmin.builder().serviceHttpUrl(url1.toString()).build();

        // Start region 2

        // Start zk & bks
        bkEnsemble2 = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble2.start();

        setConfig2DefaultValue();
        metricReader2 = InMemoryMetricReader.create();
        pulsar2 = buildPulsarService(config2, metricReader2);
        pulsar2.start();
        ns2 = pulsar2.getBrokerService();

        url2 = new URL(pulsar2.getWebServiceAddress());
        urlTls2 = new URL(pulsar2.getWebServiceAddressTls());
        admin2 = PulsarAdmin.builder().serviceHttpUrl(url2.toString()).build();

        // Start region 3

        // Start zk & bks
        bkEnsemble3 = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble3.start();

        setConfig3DefaultValue();
        metricReader3 = InMemoryMetricReader.create();
        pulsar3 = buildPulsarService(config3, metricReader3);
        pulsar3.start();
        ns3 = pulsar3.getBrokerService();

        url3 = new URL(pulsar3.getWebServiceAddress());
        urlTls3 = new URL(pulsar3.getWebServiceAddressTls());
        admin3 = PulsarAdmin.builder().serviceHttpUrl(url3.toString()).build();

        // Start region 4

        // Start zk & bks
        bkEnsemble4 = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble4.start();

        setConfig4DefaultValue();
        metricReader4 = InMemoryMetricReader.create();
        pulsar4 = buildPulsarService(config4, metricReader4);
        pulsar4.start();

        url4 = new URL(pulsar4.getWebServiceAddress());
        urlTls4 = new URL(pulsar4.getWebServiceAddressTls());
        admin4 = PulsarAdmin.builder().serviceHttpUrl(url4.toString()).build();


        // Provision the global namespace
        admin1.clusters().createCluster(cluster1, ClusterData.builder()
                .serviceUrl(url1.toString())
                .serviceUrlTls(urlTls1.toString())
                .brokerServiceUrl(pulsar1.getBrokerServiceUrl())
                .brokerServiceUrlTls(pulsar1.getBrokerServiceUrlTls())
                .brokerClientTlsEnabled(true)
                .brokerClientCertificateFilePath(clientCertFilePath)
                .brokerClientKeyFilePath(clientKeyFilePath)
                .brokerClientTrustCertsFilePath(caCertFilePath)
                .brokerClientTlsEnabledWithKeyStore(tlsWithKeyStore)
                .brokerClientTlsKeyStore(clientKeyStorePath)
                .brokerClientTlsKeyStorePassword(keyStorePassword)
                .brokerClientTlsKeyStoreType(keyStoreType)
                .brokerClientTlsTrustStore(clientTrustStorePath)
                .brokerClientTlsTrustStorePassword(keyStorePassword)
                .brokerClientTlsTrustStoreType(keyStoreType)
                .build());
        admin1.clusters().createCluster(cluster2, ClusterData.builder()
                .serviceUrl(url2.toString())
                .serviceUrlTls(urlTls2.toString())
                .brokerServiceUrl(pulsar2.getBrokerServiceUrl())
                .brokerServiceUrlTls(pulsar2.getBrokerServiceUrlTls())
                .brokerClientTlsEnabled(true)
                .brokerClientCertificateFilePath(clientCertFilePath)
                .brokerClientKeyFilePath(clientKeyFilePath)
                .brokerClientTrustCertsFilePath(caCertFilePath)
                .brokerClientTlsEnabledWithKeyStore(tlsWithKeyStore)
                .brokerClientTlsKeyStore(clientKeyStorePath)
                .brokerClientTlsKeyStorePassword(keyStorePassword)
                .brokerClientTlsKeyStoreType(keyStoreType)
                .brokerClientTlsTrustStore(clientTrustStorePath)
                .brokerClientTlsTrustStorePassword(keyStorePassword)
                .brokerClientTlsTrustStoreType(keyStoreType)
                .build());
        admin1.clusters().createCluster(cluster3, ClusterData.builder()
                .serviceUrl(url3.toString())
                .serviceUrlTls(urlTls3.toString())
                .brokerServiceUrl(pulsar3.getBrokerServiceUrl())
                .brokerServiceUrlTls(pulsar3.getBrokerServiceUrlTls())
                .brokerClientTlsEnabled(true)
                .brokerClientCertificateFilePath(clientCertFilePath)
                .brokerClientKeyFilePath(clientKeyFilePath)
                .brokerClientTrustCertsFilePath(caCertFilePath)
                .brokerClientTlsEnabledWithKeyStore(tlsWithKeyStore)
                .brokerClientTlsKeyStore(clientKeyStorePath)
                .brokerClientTlsKeyStorePassword(keyStorePassword)
                .brokerClientTlsKeyStoreType(keyStoreType)
                .brokerClientTlsTrustStore(clientTrustStorePath)
                .brokerClientTlsTrustStorePassword(keyStorePassword)
                .brokerClientTlsTrustStoreType(keyStoreType)
                .build());
        admin4.clusters().createCluster(cluster4, ClusterData.builder()
                .serviceUrlTls(urlTls4.toString())
                .brokerServiceUrlTls(pulsar4.getBrokerServiceUrlTls())
                .brokerClientTlsEnabled(true)
                .brokerClientCertificateFilePath(clientCertFilePath)
                .brokerClientKeyFilePath(clientKeyFilePath)
                .brokerClientTrustCertsFilePath(caCertFilePath)
                .brokerClientTlsEnabledWithKeyStore(tlsWithKeyStore)
                .brokerClientTlsKeyStore(clientKeyStorePath)
                .brokerClientTlsKeyStorePassword(keyStorePassword)
                .brokerClientTlsKeyStoreType(keyStoreType)
                .brokerClientTlsTrustStore(clientTrustStorePath)
                .brokerClientTlsTrustStorePassword(keyStorePassword)
                .brokerClientTlsTrustStoreType(keyStoreType)
                .build());

        updateTenantInfo("pulsar",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2", "appid3"),
                        Sets.newHashSet("r1", "r2", "r3")));
        admin1.namespaces().createNamespace("pulsar/ns", Sets.newHashSet("r1", "r2", "r3"));
        admin1.namespaces().createNamespace("pulsar/ns1", Sets.newHashSet("r1", "r2"));

        assertEquals(admin2.clusters().getCluster(cluster1).getServiceUrl(), url1.toString());
        assertEquals(admin2.clusters().getCluster(cluster2).getServiceUrl(), url2.toString());
        assertEquals(admin2.clusters().getCluster(cluster3).getServiceUrl(), url3.toString());
        assertNull(admin2.clusters().getCluster(cluster4).getServiceUrl());
        assertEquals(admin2.clusters().getCluster(cluster1).getBrokerServiceUrl(), pulsar1.getBrokerServiceUrl());
        assertEquals(admin2.clusters().getCluster(cluster2).getBrokerServiceUrl(), pulsar2.getBrokerServiceUrl());
        assertEquals(admin2.clusters().getCluster(cluster3).getBrokerServiceUrl(), pulsar3.getBrokerServiceUrl());
        assertNull(admin2.clusters().getCluster(cluster4).getBrokerServiceUrl());

        assertEquals(admin2.clusters().getCluster(cluster1).getServiceUrlTls(), urlTls1.toString());
        assertEquals(admin2.clusters().getCluster(cluster2).getServiceUrlTls(), urlTls2.toString());
        assertEquals(admin2.clusters().getCluster(cluster3).getServiceUrlTls(), urlTls3.toString());
        assertEquals(admin2.clusters().getCluster(cluster4).getServiceUrlTls(), urlTls4.toString());
        assertEquals(admin2.clusters().getCluster(cluster1).getBrokerServiceUrlTls(), pulsar1.getBrokerServiceUrlTls());
        assertEquals(admin2.clusters().getCluster(cluster2).getBrokerServiceUrlTls(), pulsar2.getBrokerServiceUrlTls());
        assertEquals(admin2.clusters().getCluster(cluster3).getBrokerServiceUrlTls(), pulsar3.getBrokerServiceUrlTls());
        assertEquals(admin2.clusters().getCluster(cluster4).getBrokerServiceUrlTls(), pulsar4.getBrokerServiceUrlTls());

        // Also create V1 namespace for compatibility check
        admin1.clusters().createCluster("global", ClusterData.builder()
                .serviceUrl("http://global:8080")
                .serviceUrlTls("https://global:8443")
                .build());
        admin1.namespaces().createNamespace("pulsar/global/ns");
        admin1.namespaces().setNamespaceReplicationClusters("pulsar/global/ns",
                Sets.newHashSet(cluster1, cluster2, cluster3));

        Thread.sleep(100);
        log.info("--- ReplicatorTestBase::setup completed ---");

    }

    private PulsarService buildPulsarService(ServiceConfiguration config, InMemoryMetricReader metricReader) {
        return new PulsarService(config,
                new WorkerConfig(),
                Optional.empty(),
                exitCode -> log.info("Pulsar service finished with exit code {}", exitCode),
                BrokerOpenTelemetryTestUtil.getOpenTelemetrySdkBuilderConsumer(metricReader));
    }

    public void setConfig3DefaultValue() {
        setConfigDefaults(config3, cluster3, bkEnsemble3);
        config3.setTlsEnabled(true);
    }

    public void setConfig1DefaultValue(){
        setConfigDefaults(config1, cluster1, bkEnsemble1);
    }

    public void setConfig2DefaultValue() {
        setConfigDefaults(config2, cluster2, bkEnsemble2);
    }

    public void setConfig4DefaultValue() {
        setConfigDefaults(config4, cluster4, bkEnsemble4);
        config4.setEnableReplicatedSubscriptions(false);
    }

    private void setConfigDefaults(ServiceConfiguration config, String clusterName,
                                   LocalBookkeeperEnsemble bookkeeperEnsemble) {
        config.setClusterName(clusterName);
        config.setAdvertisedAddress("localhost");
        config.setWebServicePort(Optional.of(0));
        config.setWebServicePortTls(Optional.of(0));
        config.setMetadataStoreUrl("zk:127.0.0.1:" + bookkeeperEnsemble.getZookeeperPort());
        config.setConfigurationMetadataStoreUrl("zk:127.0.0.1:" + globalZkS.getZookeeperPort() + "/foo");
        config.setBrokerDeleteInactiveTopicsEnabled(isBrokerServicePurgeInactiveTopic());
        config.setBrokerDeleteInactiveTopicsFrequencySeconds(
                inSec(getBrokerServicePurgeInactiveFrequency(), TimeUnit.SECONDS));
        config.setBrokerShutdownTimeoutMs(0L);
        config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        config.setBrokerServicePort(Optional.of(0));
        config.setBrokerServicePortTls(Optional.of(0));
        config.setTlsCertificateFilePath(brokerCertFilePath);
        config.setTlsKeyFilePath(brokerFilePath);
        config.setTlsTrustCertsFilePath(caCertFilePath);
        config.setTlsEnabledWithKeyStore(tlsWithKeyStore);
        config.setTlsKeyStore(brokerKeyStorePath);
        config.setTlsKeyStoreType(keyStoreType);
        config.setTlsKeyStorePassword(keyStorePassword);
        config.setTlsTrustStore(brokerTrustStorePath);
        config.setTlsTrustStoreType(keyStoreType);
        config.setTlsTrustStorePassword(keyStorePassword);
        config.setBacklogQuotaCheckIntervalInSeconds(TIME_TO_CHECK_BACKLOG_QUOTA);
        config.setDefaultNumberOfNamespaceBundles(1);
        config.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
        config.setEnableReplicatedSubscriptions(true);
        config.setReplicatedSubscriptionsSnapshotFrequencyMillis(1000);
        config.setLoadManagerClassName(getLoadManagerClassName());
    }

    public void resetConfig1() {
        config1 = new ServiceConfiguration();
    }

    public void resetConfig2() {
        config2 = new ServiceConfiguration();
    }

    public void resetConfig3() {
        config3 = new ServiceConfiguration();
    }

    public void resetConfig4() {
        config4 = new ServiceConfiguration();
    }

    private int inSec(int time, TimeUnit unit) {
        return (int) TimeUnit.SECONDS.convert(time, unit);
    }

    @Override
    protected void cleanup() throws Exception {
        markCurrentSetupNumberCleaned();
        log.info("--- Shutting down ---");
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }

        if (admin1 != null) {
            admin1.close();
            admin1 = null;
        }
        if (admin2 != null) {
            admin2.close();
            admin2 = null;
        }
        if (admin3 != null) {
            admin3.close();
            admin3 = null;
        }
        if (admin4 != null) {
            admin4.close();
            admin4 = null;
        }

        if (metricReader4 != null) {
            metricReader4.close();
            metricReader4 = null;
        }
        if (metricReader3 != null) {
            metricReader3.close();
            metricReader3 = null;
        }
        if (metricReader2 != null) {
            metricReader2.close();
            metricReader2 = null;
        }
        if (metricReader1 != null) {
            metricReader1.close();
            metricReader1 = null;
        }

        if (pulsar4 != null) {
            pulsar4.close();
            pulsar4 = null;
        }
        if (pulsar3 != null) {
            pulsar3.close();
            pulsar3 = null;
        }
        if (pulsar2 != null) {
            pulsar2.close();
            pulsar2 = null;
        }
        if (pulsar1 != null) {
            pulsar1.close();
            pulsar1 = null;
        }

        if (bkEnsemble1 != null) {
            bkEnsemble1.stop();
            bkEnsemble1 = null;
        }
        if (bkEnsemble2 != null) {
            bkEnsemble2.stop();
            bkEnsemble2 = null;
        }
        if (bkEnsemble3 != null) {
            bkEnsemble3.stop();
            bkEnsemble3 = null;
        }
        if (bkEnsemble4 != null) {
            bkEnsemble4.stop();
            bkEnsemble4 = null;
        }
        if (globalZkS != null) {
            globalZkS.stop();
            globalZkS = null;
        }

        resetConfig1();
        resetConfig2();
        resetConfig3();
        resetConfig4();
    }

    protected void updateTenantInfo(String tenant, TenantInfoImpl tenantInfo) throws Exception {
        if (!admin1.tenants().getTenants().contains(tenant)) {
            admin1.tenants().createTenant(tenant, tenantInfo);
        } else {
            admin1.tenants().updateTenant(tenant, tenantInfo);
        }
    }

    static class MessageProducer implements AutoCloseable {
        URL url;
        String namespace;
        String topicName;
        PulsarClient client;
        Producer<byte[]> producer;

        MessageProducer(URL url, final TopicName dest) throws Exception {
            this.url = url;
            this.namespace = dest.getNamespace();
            this.topicName = dest.toString();
            client = PulsarClient.builder().serviceUrl(url.toString()).statsInterval(0, TimeUnit.SECONDS).build();
            try {
                producer = client.newProducer()
                        .topic(topicName)
                        .enableBatching(false)
                        .messageRoutingMode(MessageRoutingMode.SinglePartition)
                        .create();
            } catch (Exception e) {
                client.close();
                throw e;
            }
        }

        MessageProducer(URL url, final TopicName dest, boolean batch) throws Exception {
            this.url = url;
            this.namespace = dest.getNamespace();
            this.topicName = dest.toString();
            client = PulsarClient.builder().serviceUrl(url.toString()).statsInterval(0, TimeUnit.SECONDS).build();
            ProducerBuilder<byte[]> producerBuilder = client.newProducer()
                .topic(topicName)
                .enableBatching(batch)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .batchingMaxMessages(5);
            try {
                producer = producerBuilder.create();
            } catch (Exception e) {
                client.close();
                throw e;
            }
        }

        void produceBatch(int messages) throws Exception {
            log.info("Start sending batch messages");

            for (int i = 0; i < messages; i++) {
                producer.sendAsync(("test-" + i).getBytes());
                log.info("queued message {}", ("test-" + i));
            }
            producer.flush();
        }

        void produce(int messages) throws Exception {

            log.info("Start sending messages");
            for (int i = 0; i < messages; i++) {
                producer.send(("test-" + i).getBytes());
                log.info("Sent message {}", ("test-" + i));
            }

        }

        TypedMessageBuilder<byte[]> newMessage() {
            return producer.newMessage();
        }

        void produce(int messages, TypedMessageBuilder<byte[]> messageBuilder) throws Exception {
            log.info("Start sending messages");
            for (int i = 0; i < messages; i++) {
                final String m = "test-" + i;
                messageBuilder.value(m.getBytes()).send();
                log.info("Sent message {}", m);
            }
        }

        public void close() {
            try {
                client.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close client", e);
            }
        }

    }

    static class MessageConsumer implements AutoCloseable {
        final URL url;
        final String namespace;
        final String topicName;
        final PulsarClient client;
        final Consumer<byte[]> consumer;

        MessageConsumer(URL url, final TopicName dest) throws Exception {
            this(url, dest, "sub-id");
        }

        MessageConsumer(URL url, final TopicName dest, String subId) throws Exception {
            this.url = url;
            this.namespace = dest.getNamespace();
            this.topicName = dest.toString();

            client = PulsarClient.builder().serviceUrl(url.toString()).statsInterval(0, TimeUnit.SECONDS).build();

            try {
                consumer = client.newConsumer().topic(topicName).subscriptionName(subId).subscribe();
            } catch (Exception e) {
                client.close();
                throw e;
            }
        }

        void receive(int messages) throws Exception {
            log.info("Start receiving messages");
            Message<byte[]> msg;

            Set<String> receivedMessages = new TreeSet<>();

            int i = 0;
            while (i < messages) {
                msg = consumer.receive(10, TimeUnit.SECONDS);
                assertNotNull(msg);
                consumer.acknowledge(msg);

                String msgData = new String(msg.getData());
                log.info("Received message {}", msgData);

                boolean added = receivedMessages.add(msgData);
                if (added) {
                    assertEquals(msgData, "test-" + i);
                    i++;
                } else {
                    log.info("Ignoring duplicate {}", msgData);
                }
            }
        }

        boolean drained() throws Exception {
            return consumer.receive(0, TimeUnit.MICROSECONDS) == null;
        }

        public void close() {
            try {
                client.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close client", e);
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ReplicatorTestBase.class);
}
