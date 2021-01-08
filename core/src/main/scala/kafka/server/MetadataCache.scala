/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util
import java.util.Collections

import kafka.utils.Logging
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.MetadataResponseData.{MetadataResponsePartition, MetadataResponseTopic}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.{Cluster, Node, PartitionInfo, TopicPartition}

import scala.jdk.CollectionConverters._

case class MetadataCacheBuilder(prevCache: MetadataCache) {
  var _partitionsBuilder: MetadataPartitionsBuilder = null
  var _controllerId = prevCache.controllerId
  var _brokersBuilder: MetadataBrokersBuilder = null

  def partitionsBuilder(): MetadataPartitionsBuilder = {
    if (partitionsBuilder == null) {
      _partitionsBuilder = new MetadataPartitionsBuilder(prevCache.partitions)
    }
    _partitionsBuilder
  }

  def setControllerId(controllerId: Option[Int]) = {
    _controllerId = controllerId
  }

  def brokersBuilder(): MetadataBrokersBuilder = {
    if (_brokersBuilder == null) {
      _brokersBuilder = new MetadataBrokersBuilder(prevCache.brokers)
    }
    _brokersBuilder
  }

  def build(): MetadataCache = {
    val nextPartitions = if (_partitionsBuilder == null) {
      prevCache.partitions
    } else {
      _partitionsBuilder.build()
    }
    val nextBrokers = if (_brokersBuilder == null) {
      prevCache.brokers
    } else {
      _brokersBuilder.build()
    }
    MetadataCache(nextPartitions, _controllerId, nextBrokers)
  }
}

case class MetadataCache(partitions: MetadataPartitions,
                         controllerId: Option[Int],
                         brokers: MetadataBrokers,
                         logContext: LogContext) extends Logging {

  this.logIdent = logContext.logPrefix()

  def contains(partition: TopicPartition): Boolean =
    partitions.get(partition.topic(), partition.partition()).isDefined

  def contains(topic: String): Boolean = partitions.topicPartitions(topic).hasNext

  def getAliveBroker(id: Int): Option[MetadataBroker] = brokers.get(id)

  def numAliveBrokers(): Int = {
    brokers.iterator().asScala.count(!_.fenced)
  }

  // if the leader is not known, return None;
  // if the leader is known and corresponding node is available, return Some(node)
  // if the leader is known but corresponding node with the listener name is not available, return Some(NO_NODE)
  def getPartitionLeaderEndpoint(topic: String, partitionId: Int, listenerName: ListenerName): Option[Node] = {
    partitions.get(topic, partitionId) match {
      case None => None
      case Some(partition) =>
        val leaderId = partition.leaderId
        if (leaderId < 0) {
          None
        } else {
          brokers.getAlive(leaderId).flatMap(broker => broker.endPoints.get(listenerName.value()))
        }
    }
  }

  def controller(): Option[MetadataBroker] = controllerId.flatMap(id => brokers.getAlive(id))

  def getClusterMetadata(clusterId: String, listenerName: ListenerName): Cluster = {
    val nodes = new util.HashMap[Integer, Node]
    brokers.iterator().asScala.foreach {
      case node => if (!node.fenced) {
        node.endPoints.get(listenerName.value()).foreach { nodes.put(node.id, _) }
      }
    }

    def node(id: Integer): Node = {
      Some(nodes.get(id)).getOrElse(Node.noNode())
    }

    val partitionInfos = new util.ArrayList[PartitionInfo]
    val internalTopics = new util.HashSet[String]

    partitions.allPartitions().asScala.foreach {
      case partition =>
        partitionInfos.add(new PartitionInfo(partition.topicName,
          partition.partitionIndex, node(partition.leaderId),
          partition.replicas.map(node(_)).toArray,
          partition.isr.map(node(_)).toArray,
          partition.offlineReplicas.map(node(_)).toArray))
        if (Topic.isInternal(partition.topicName)) {
          internalTopics.add(partition.topicName)
        }
    }

    val unauthorizedTopics = Collections.emptySet[String]

    new Cluster(clusterId, nodes.values(),
      partitionInfos, unauthorizedTopics, internalTopics,
      controller().getOrElse(Node.noNode()))
  }

  // This method is the main hotspot when it comes to the performance of metadata requests,
  // we should be careful about adding additional logic here. Relatedly, `brokers` is
  // `List[Integer]` instead of `List[Int]` to avoid a collection copy.
  // filterUnavailableEndpoints exists to support v0 MetadataResponses
  private def maybeFilterAliveReplicas(brokerList: util.List[Integer],
                                       listenerName: ListenerName,
                                       filterUnavailableEndpoints: Boolean): util.List[Integer] = {
    if (!filterUnavailableEndpoints) {
      brokerList
    } else {
      val res = new util.ArrayList[Integer](brokerList.size())
      for (brokerId <- brokerList.asScala) {
        if (hasAliveEndpoint(brokerId, listenerName))
          res.add(brokerId)
      }
      res
    }
  }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  // If errorUnavailableListeners=true, return LISTENER_NOT_FOUND if listener is missing on the broker.
  // Otherwise, return LEADER_NOT_AVAILABLE for broker unavailable and missing listener (Metadata response v5 and below).
  private def getPartitionMetadata(topic: String, listenerName: ListenerName, errorUnavailableEndpoints: Boolean,
                                   errorUnavailableListeners: Boolean): Option[Iterable[MetadataResponsePartition]] = {
    partitions.topicPartitions(topic).asScala.map {
      case partition =>
        val topicPartition = new TopicPartition(topic, partition.partitionIndex)
        val leaderBrokerId = partition.leaderId
        val maybeLeader = getAliveEndpoint(leaderBrokerId, listenerName)

        val filteredReplicas = maybeFilterAliveReplicas(partition.replicas, listenerName, errorUnavailableEndpoints)

        val isr = partitionState.isr
        val filteredIsr = maybeFilterAliveReplicas(isr, listenerName, errorUnavailableEndpoints)

        val offlineReplicas = partitionState.offlineReplicas

        maybeLeader match {
          case None =>
            val error = if (!snapshot.aliveBrokers.contains(leaderBrokerId)) { // we are already holding the read lock
              debug(s"Error while fetching metadata for $topicPartition: leader not available")
              Errors.LEADER_NOT_AVAILABLE
            } else {
              debug(s"Error while fetching metadata for $topicPartition: listener $listenerName " +
                s"not found on leader $leaderBrokerId")
              if (errorUnavailableListeners) Errors.LISTENER_NOT_FOUND else Errors.LEADER_NOT_AVAILABLE
            }

            new MetadataResponsePartition()
              .setErrorCode(error.code)
              .setPartitionIndex(partition.partitionIndex)
              .setLeaderId(MetadataResponse.NO_LEADER_ID)
              .setLeaderEpoch(partition.leaderEpoch)
              .setReplicaNodes(filteredReplicas)
              .setIsrNodes(filteredIsr)
              .setOfflineReplicas(offlineReplicas)

          case Some(_) =>
            val error = if (filteredReplicas.size < replicas.size) {
              debug(s"Error while fetching metadata for $topicPartition: replica information not available for " +
                s"following brokers ${replicas.asScala.filterNot(filteredReplicas.contains).mkString(",")}")
              Errors.REPLICA_NOT_AVAILABLE
            } else if (filteredIsr.size < isr.size) {
              debug(s"Error while fetching metadata for $topicPartition: in sync replica information not available for " +
                s"following brokers ${isr.asScala.filterNot(filteredIsr.contains).mkString(",")}")
              Errors.REPLICA_NOT_AVAILABLE
            } else {
              Errors.NONE
            }

            new MetadataResponsePartition()
              .setErrorCode(error.code)
              .setPartitionIndex(partitionId.toInt)
              .setLeaderId(maybeLeader.map(_.id()).getOrElse(MetadataResponse.NO_LEADER_ID))
              .setLeaderEpoch(partition.leaderEpoch)
              .setReplicaNodes(filteredReplicas)
              .setIsrNodes(filteredIsr)
              .setOfflineReplicas(offlineReplicas)
        }
      }
    }

  /**
   * Check whether a broker is alive and has a registered listener matching the provided name.
   * This method was added to avoid unnecessary allocations in [[maybeFilterAliveReplicas]], which is
   * a hotspot in metadata handling.
   */
  private def hasAliveEndpoint(snapshot: MetadataSnapshot, brokerId: Int, listenerName: ListenerName): Boolean = {
    snapshot.aliveNodes.get(brokerId).exists(_.contains(listenerName))
  }

  /**
   * Get the endpoint matching the provided listener if the broker is alive. Note that listeners can
   * be added dynamically, so a broker with a missing listener could be a transient error.
   *
   * @return None if broker is not alive or if the broker does not have a listener named `listenerName`.
   */
  private def getAliveEndpoint(snapshot: MetadataSnapshot, brokerId: Int, listenerName: ListenerName): Option[Node] = {
    snapshot.aliveNodes.get(brokerId).flatMap(_.get(listenerName))
  }
}

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  def getTopicMetadata(topics: Set[String],
                       listenerName: ListenerName,
                       errorUnavailableEndpoints: Boolean = false,
                       errorUnavailableListeners: Boolean = false): Seq[MetadataResponseTopic] = {
    topics.toSeq.flatMap { topic =>
      getPartitionMetadata(topic, listenerName, errorUnavailableEndpoints, errorUnavailableListeners).map { partitionMetadata =>
        new MetadataResponseTopic()
          .setErrorCode(Errors.NONE.code)
          .setName(topic)
          .setTopicId(snapshot.topicIds.getOrElse(topic, Uuid.ZERO_UUID))
          .setIsInternal(Topic.isInternal(topic))
          .setPartitions(partitionMetadata.toBuffer.asJava)
      }
    }
  }

}

