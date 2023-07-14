package org.apache.flink.connector.rocketmq.sink.writer.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.rocketmq.sink.writer.context.RocketMQSinkContext;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.Serializable;

/**
 * The serialization schema for how to serialize records into RocketMQ. A serialization schema which
 * defines how to convert a value of type {@code T} to {@link MessageExt}.
 *
 * @param <T> the type of values being serialized
 */
@PublicEvolving
public interface RocketMQSerializationSchema<T> extends Serializable {

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #serialize(Object, RocketMQSinkContext, Long)} and thus suitable for one time setup work.
     *
     * <p>The provided {@link SerializationSchema.InitializationContext} can be used to access
     * additional features such as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     * @param sinkContext runtime information i.e. partitions, subtaskId
     */
    default void open(
            SerializationSchema.InitializationContext context, RocketMQSinkContext sinkContext)
            throws Exception {}

    /**
     * Serializes given element and returns it as a {@link MessageExt}.
     *
     * @param element element to be serialized
     * @param context context to possibly determine target partition
     * @param timestamp timestamp
     * @return RocketMQ {@link MessageExt}
     */
    Message serialize(T element, RocketMQSinkContext context, Long timestamp);
}
