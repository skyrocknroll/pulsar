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
package org.apache.pulsar.common.naming;

import com.google.common.base.Splitter;
import com.google.re2j.Pattern;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.Codec;

/**
 * Encapsulate the parsing of the completeTopicName name.
 */
public class TopicName implements ServiceUnitId {

    public static final String PUBLIC_TENANT = "public";
    public static final String DEFAULT_NAMESPACE = "default";

    public static final String PARTITIONED_TOPIC_SUFFIX = "-partition-";

    private final String completeTopicName;

    private final TopicDomain domain;
    private final String tenant;
    private final String cluster;
    private final String namespacePortion;
    private final String localName;

    private final NamespaceName namespaceName;

    private final int partitionIndex;

    private static final ConcurrentHashMap<String, TopicName> cache = new ConcurrentHashMap<>();

    public static void clearIfReachedMaxCapacity(int maxCapacity) {
        if (maxCapacity < 0) {
            // Unlimited cache.
            return;
        }
        if (cache.size() > maxCapacity) {
            cache.clear();
        }
    }

    public static TopicName get(String domain, NamespaceName namespaceName, String topic) {
        String name = domain + "://" + namespaceName.toString() + '/' + topic;
        return TopicName.get(name);
    }

    public static TopicName get(String domain, String tenant, String namespace, String topic) {
        String name = domain + "://" + tenant + '/' + namespace + '/' + topic;
        return TopicName.get(name);
    }

    public static TopicName get(String domain, String tenant, String cluster, String namespace,
                                String topic) {
        String name = domain + "://" + tenant + '/' + cluster + '/' + namespace + '/' + topic;
        return TopicName.get(name);
    }

    public static TopicName get(String topic) {
        TopicName tp = cache.get(topic);
        if (tp != null) {
            return tp;
        }
        return cache.computeIfAbsent(topic, k -> new TopicName(k));
    }

    public static TopicName getPartitionedTopicName(String topic) {
        TopicName topicName = TopicName.get(topic);
        if (topicName.isPartitioned()) {
            return TopicName.get(topicName.getPartitionedTopicName());
        }
        return topicName;
    }

    public static boolean isValid(String topic) {
        try {
            get(topic);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static String getPartitionPattern(String topic) {
        return "^" + Pattern.quote(get(topic).getPartitionedTopicName().toString()) + "-partition-[0-9]+$";
    }

    public static String getPattern(String topic) {
        return "^" + Pattern.quote(get(topic).getPartitionedTopicName().toString()) + "$";
    }

    @SuppressFBWarnings("DCN_NULLPOINTER_EXCEPTION")
    private TopicName(String completeTopicName) {
        try {
            // The topic name can be in two different forms, one is fully qualified topic name,
            // the other one is short topic name
            if (!completeTopicName.contains("://")) {
                // The short topic name can be:
                // - <topic>
                // - <property>/<namespace>/<topic>
                String[] parts = StringUtils.split(completeTopicName, '/');
                if (parts.length == 3) {
                    completeTopicName = TopicDomain.persistent.name() + "://" + completeTopicName;
                } else if (parts.length == 1) {
                    completeTopicName = TopicDomain.persistent.name() + "://"
                        + PUBLIC_TENANT + "/" + DEFAULT_NAMESPACE + "/" + parts[0];
                } else {
                    throw new IllegalArgumentException(
                        "Invalid short topic name '" + completeTopicName + "', it should be in the format of "
                        + "<tenant>/<namespace>/<topic> or <topic>");
                }
            }

            // The fully qualified topic name can be in two different forms:
            // new:    persistent://tenant/namespace/topic
            // legacy: persistent://tenant/cluster/namespace/topic

            List<String> parts = Splitter.on("://").limit(2).splitToList(completeTopicName);
            this.domain = TopicDomain.getEnum(parts.get(0));

            String rest = parts.get(1);

            // The rest of the name can be in different forms:
            // new:    tenant/namespace/<localName>
            // legacy: tenant/cluster/namespace/<localName>
            // Examples of localName:
            // 1. some, name, xyz
            // 2. xyz-123, feeder-2


            parts = Splitter.on("/").limit(4).splitToList(rest);
            if (parts.size() == 3) {
                // New topic name without cluster name
                this.tenant = parts.get(0);
                this.cluster = null;
                this.namespacePortion = parts.get(1);
                this.localName = parts.get(2);
                this.partitionIndex = getPartitionIndex(completeTopicName);
                this.namespaceName = NamespaceName.get(tenant, namespacePortion);
            } else if (parts.size() == 4) {
                // Legacy topic name that includes cluster name
                this.tenant = parts.get(0);
                this.cluster = parts.get(1);
                this.namespacePortion = parts.get(2);
                this.localName = parts.get(3);
                this.partitionIndex = getPartitionIndex(completeTopicName);
                this.namespaceName = NamespaceName.get(tenant, cluster, namespacePortion);
            } else {
                throw new IllegalArgumentException("Invalid topic name: " + completeTopicName);
            }

            if (StringUtils.isBlank(localName)) {
                throw new IllegalArgumentException(String.format("Invalid topic name: %s. Topic local name must not"
                        + " be blank.", completeTopicName));
            }

        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Invalid topic name: " + completeTopicName, e);
        }
        if (isV2()) {
            this.completeTopicName = String.format("%s://%s/%s/%s",
                                                   domain, tenant, namespacePortion, localName);
        } else {
            this.completeTopicName = String.format("%s://%s/%s/%s/%s",
                                                   domain, tenant, cluster,
                                                   namespacePortion, localName);
        }
    }

    public boolean isPersistent() {
        return TopicDomain.persistent == domain;
    }

    /**
     * Extract the namespace portion out of a completeTopicName name.
     *
     * <p>Works both with old & new convention.
     *
     * @return the namespace
     */
    public String getNamespace() {
        return namespaceName.toString();
    }

    /**
     * Get the namespace object that this completeTopicName belongs to.
     *
     * @return namespace object
     */
    @Override
    public NamespaceName getNamespaceObject() {
        return namespaceName;
    }

    public TopicDomain getDomain() {
        return domain;
    }

    public String getTenant() {
        return tenant;
    }

    @Deprecated
    public String getCluster() {
        return cluster;
    }

    public String getNamespacePortion() {
        return namespacePortion;
    }

    public String getLocalName() {
        return localName;
    }

    public String getEncodedLocalName() {
        return Codec.encode(localName);
    }

    public TopicName getPartition(int index) {
        if (index == -1 || this.toString().endsWith(PARTITIONED_TOPIC_SUFFIX + index)) {
            return this;
        }
        String partitionName = this.toString() + PARTITIONED_TOPIC_SUFFIX + index;
        return get(partitionName);
    }

    /**
     * @return partition index of the completeTopicName.
     * It returns -1 if the completeTopicName (topic) is not partitioned.
     */
    public int getPartitionIndex() {
        return partitionIndex;
    }

    public boolean isPartitioned() {
        return partitionIndex != -1;
    }

    /**
     * For partitions in a topic, return the base partitioned topic name.
     * Eg:
     * <ul>
     *  <li><code>persistent://prop/cluster/ns/my-topic-partition-1</code> -->
     *  <code>persistent://prop/cluster/ns/my-topic</code>
     *  <li><code>persistent://prop/cluster/ns/my-topic</code> --> <code>persistent://prop/cluster/ns/my-topic</code>
     * </ul>
     */
    public String getPartitionedTopicName() {
        if (isPartitioned()) {
            return completeTopicName.substring(0, completeTopicName.lastIndexOf("-partition-"));
        } else {
            return completeTopicName;
        }
    }

    /**
     * @return partition index of the completeTopicName.
     * It returns -1 if the completeTopicName (topic) is not partitioned.
     */
    public static int getPartitionIndex(String topic) {
        int partitionIndex = -1;
        if (topic.contains(PARTITIONED_TOPIC_SUFFIX)) {
            try {
                String idx = StringUtils.substringAfterLast(topic, PARTITIONED_TOPIC_SUFFIX);
                partitionIndex = Integer.parseInt(idx);
                if (partitionIndex < 0) {
                    // for the "topic-partition--1"
                    partitionIndex = -1;
                } else if (StringUtils.length(idx) != String.valueOf(partitionIndex).length()) {
                    // for the "topic-partition-01"
                    partitionIndex = -1;
                }
            } catch (NumberFormatException nfe) {
                // ignore exception
            }
        }

        return partitionIndex;
    }

    /**
     * A helper method to get a partition name of a topic in String.
     * @return topic + "-partition-" + partition.
     */
    public static String getTopicPartitionNameString(String topic, int partitionIndex) {
        return topic + PARTITIONED_TOPIC_SUFFIX + partitionIndex;
    }

    /**
     * Returns the http rest path for use in the admin web service.
     * Eg:
     *   * "persistent/my-tenant/my-namespace/my-topic"
     *   * "non-persistent/my-tenant/my-namespace/my-topic"
     *
     * @return topic rest path
     */
    public String getRestPath() {
        return getRestPath(true);
    }

    public String getRestPath(boolean includeDomain) {
        String domainName = includeDomain ? domain + "/" : "";
        if (isV2()) {
            return String.format("%s%s/%s/%s", domainName, tenant, namespacePortion, getEncodedLocalName());
        } else {
            return String.format("%s%s/%s/%s/%s", domainName, tenant, cluster, namespacePortion, getEncodedLocalName());
        }
    }

    /**
     * Returns the name of the persistence resource associated with the completeTopicName.
     *
     * @return the relative path to be used in persistence
     */
    public String getPersistenceNamingEncoding() {
        // The convention is: domain://tenant/namespace/topic
        // We want to persist in the order: tenant/namespace/domain/topic

        // For legacy naming scheme, the convention is: domain://tenant/cluster/namespace/topic
        // We want to persist in the order: tenant/cluster/namespace/domain/topic
        if (isV2()) {
            return String.format("%s/%s/%s/%s", tenant, namespacePortion, domain, getEncodedLocalName());
        } else {
            return String.format("%s/%s/%s/%s/%s", tenant, cluster, namespacePortion, domain, getEncodedLocalName());
        }
    }

    /**
     * get topic full name from managedLedgerName.
     *
     * @return the topic full name, format -> domain://tenant/namespace/topic
     */
    public static String fromPersistenceNamingEncoding(String mlName) {
        // The managedLedgerName convention is: tenant/namespace/domain/topic
        // We want to transform to topic full name in the order: domain://tenant/namespace/topic
        if (mlName == null || mlName.length() == 0) {
            return mlName;
        }
        List<String> parts = Splitter.on("/").splitToList(mlName);
        String tenant;
        String cluster;
        String namespacePortion;
        String domain;
        String localName;
        if (parts.size() == 4) {
            tenant = parts.get(0);
            namespacePortion = parts.get(1);
            domain = parts.get(2);
            localName = Codec.decode(parts.get(3));
            return String.format("%s://%s/%s/%s", domain, tenant, namespacePortion, localName);
        } else if (parts.size() == 5) {
            tenant = parts.get(0);
            cluster = parts.get(1);
            namespacePortion = parts.get(2);
            domain = parts.get(3);
            localName = Codec.decode(parts.get(4));
            return String.format("%s://%s/%s/%s/%s", domain, tenant, cluster, namespacePortion, localName);
        } else {
            throw new IllegalArgumentException("Invalid managedLedger name: " + mlName);
        }
    }

    /**
     * Get a string suitable for completeTopicName lookup.
     *
     * <p>Example:
     *
     * <p>persistent://tenant/cluster/namespace/completeTopicName ->
     *   persistent/tenant/cluster/namespace/completeTopicName
     *
     * @return
     */
    public String getLookupName() {
        if (isV2()) {
            return String.format("%s/%s/%s/%s", domain, tenant, namespacePortion, getEncodedLocalName());
        } else {
            return String.format("%s/%s/%s/%s/%s", domain, tenant, cluster, namespacePortion, getEncodedLocalName());
        }
    }

    public boolean isGlobal() {
        return cluster == null || Constants.GLOBAL_CLUSTER.equalsIgnoreCase(cluster);
    }

    public String getSchemaName() {
        return getTenant()
            + "/" + getNamespacePortion()
            + "/" + TopicName.get(getPartitionedTopicName()).getEncodedLocalName();
    }

    @Override
    public String toString() {
        return completeTopicName;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TopicName) {
            TopicName other = (TopicName) obj;
            return Objects.equals(completeTopicName, other.completeTopicName);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return completeTopicName.hashCode();
    }

    @Override
    public boolean includes(TopicName otherTopicName) {
        return this.equals(otherTopicName);
    }

    /**
     * Returns true if this a V2 topic name prop/ns/topic-name.
     * @return true if V2
     */
    public boolean isV2() {
        return cluster == null;
    }

    /**
     * Convert a topic name to a full topic name.
     * In Pulsar, a full topic name is "<domain>://<tenant>/<namespace>/<local-topic>" (v2) or
     * "<domain>://<tenant>/<cluster>/<namespace>/<local-topic>" (v1). However, for convenient, it's allowed for clients
     * to pass a short topic name with v2 format:
     * - "<local-topic>", which represents "persistent://public/default/<local-topic>"
     * - "<tenant>/<namespace>/<local-topic>, which represents "persistent://<tenant>/<namespace>/<local-topic>"
     *
     * @param topic the topic name from client
     * @return the full topic name.
     */
    public static String toFullTopicName(String topic) {
        final int index = topic.indexOf("://");
        if (index >= 0) {
            TopicDomain.getEnum(topic.substring(0, index));
            final List<String> parts = splitBySlash(topic.substring(index + "://".length()), 4);
            if (parts.size() != 3 && parts.size() != 4) {
                throw new IllegalArgumentException(topic + " is invalid");
            }
            if (parts.size() == 3) {
                NamespaceName.validateNamespaceName(parts.get(0), parts.get(1));
                if (StringUtils.isBlank(parts.get(2))) {
                    throw new IllegalArgumentException(topic + " has blank local topic");
                }
            } else {
                NamespaceName.validateNamespaceName(parts.get(0), parts.get(1), parts.get(2));
                if (StringUtils.isBlank(parts.get(3))) {
                    throw new IllegalArgumentException(topic + " has blank local topic");
                }
            }
            return topic; // it's a valid full topic name
        } else {
            List<String> parts = splitBySlash(topic, 0);
            if (parts.size() != 1 && parts.size() != 3) {
                throw new IllegalArgumentException(topic + " is invalid");
            }
            if (parts.size() == 1) {
                if (StringUtils.isBlank(parts.get(0))) {
                    throw new IllegalArgumentException(topic + " has blank local topic");
                }
                return "persistent://public/default/" + parts.get(0);
            } else {
                NamespaceName.validateNamespaceName(parts.get(0), parts.get(1));
                if (StringUtils.isBlank(parts.get(2))) {
                    throw new IllegalArgumentException(topic + " has blank local topic");
                }
                return "persistent://" + topic;
            }
        }
    }

    private static List<String> splitBySlash(String topic, int limit) {
        final List<String> tokens = new ArrayList<>(3);
        final int loopCount = (limit <= 0) ? Integer.MAX_VALUE : limit - 1;
        int beginIndex = 0;
        for (int i = 0; i < loopCount; i++) {
            final int endIndex = topic.indexOf('/', beginIndex);
            if (endIndex < 0) {
                tokens.add(topic.substring(beginIndex));
                return tokens;
            } else if (endIndex > beginIndex) {
                tokens.add(topic.substring(beginIndex, endIndex));
            } else {
                throw new IllegalArgumentException("Invalid topic name " + topic);
            }
            beginIndex = endIndex + 1;
        }
        if (beginIndex >= topic.length()) {
            throw new IllegalArgumentException("Invalid topic name " + topic);
        }
        tokens.add(topic.substring(beginIndex));
        return tokens;
    }
}
