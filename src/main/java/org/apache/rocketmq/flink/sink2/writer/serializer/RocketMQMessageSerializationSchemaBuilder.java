/*
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

package org.apache.rocketmq.flink.sink2.writer.serializer;

import org.apache.rocketmq.common.message.Message;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

@PublicEvolving
public class RocketMQMessageSerializationSchemaBuilder<IN> {

    @Nullable private Function<? super IN, String> topicSelector;

    @Nullable private RocketMQTagSelector<? super IN> tagSelector;

    @Nullable private SerializationSchema<? super IN> valueSerializationSchema;

    @SuppressWarnings("unchecked")
    private <T extends IN> RocketMQMessageSerializationSchemaBuilder<T> self() {
        return (RocketMQMessageSerializationSchemaBuilder<T>) this;
    }

    /**
     * Constructs the {@link RocketMQMessageSerializationSchemaBuilder} with the configured
     * properties.
     *
     * @return {@link RocketMQMessageSerializationSchema}
     */
    public RocketMQMessageSerializationSchema<IN> build() {
        checkState(valueSerializationSchema != null, "No value serializer is configured.");
        checkState(topicSelector != null, "No topic selector is configured.");
        return new RocketMQMessageSerializationSchemaWrapper<>(
                topicSelector, tagSelector, valueSerializationSchema);
    }

    /**
     * Sets a custom tag determining the target partition of the target topic.
     *
     * @param tagSelector
     * @return {@code this}
     */
    public <T extends IN> RocketMQMessageSerializationSchemaBuilder<T> setTagSelector(
            RocketMQTagSelector<? super T> tagSelector) {
        checkState(this.topicSelector == null, "Topic selector already set.");
        RocketMQMessageSerializationSchemaBuilder<T> self = self();
        self.tagSelector = checkNotNull(tagSelector);
        return self;
    }

    /**
     * Sets a fixed topic which used as destination for all records.
     *
     * @param topic
     * @return {@code this}
     */
    public RocketMQMessageSerializationSchemaBuilder<IN> setTopic(String topic) {
        checkState(this.topicSelector == null, "Topic selector already set.");
        checkNotNull(topic);
        this.topicSelector = new CachingTopicSelector<>((e) -> topic);
        return this;
    }

    /**
     * Sets a topic selector which computes the target topic for every incoming record.
     *
     * @param topicSelector
     * @return {@code this}
     */
    public <T extends IN> RocketMQMessageSerializationSchemaBuilder<T> setTopicSelector(
            TopicSelector<? super T> topicSelector) {
        checkState(this.topicSelector == null, "Topic selector already set.");
        RocketMQMessageSerializationSchemaBuilder<T> self = self();
        self.topicSelector = new CachingTopicSelector<>(checkNotNull(topicSelector));
        return self;
    }

    /**
     * Sets a {@link SerializationSchema} which is used to serialize the incoming element to the
     * value of the {@link Message}.
     *
     * @param valueSerializationSchema
     * @return {@code this}
     */
    public <T extends IN> RocketMQMessageSerializationSchemaBuilder<T> setValueSerializationSchema(
            SerializationSchema<T> valueSerializationSchema) {
        checkValueSerializerNotSet();
        RocketMQMessageSerializationSchemaBuilder<T> self = self();
        self.valueSerializationSchema = checkNotNull(valueSerializationSchema);
        return self;
    }

    private void checkValueSerializerNotSet() {
        checkState(valueSerializationSchema == null, "Value serializer already set.");
    }

    private static class CachingTopicSelector<IN> implements Function<IN, String>, Serializable {

        private static final int CACHE_RESET_SIZE = 5;
        private final Map<IN, String> cache;
        private final TopicSelector<IN> topicSelector;

        CachingTopicSelector(TopicSelector<IN> topicSelector) {
            this.topicSelector = topicSelector;
            this.cache = new HashMap<>();
        }

        @Override
        public String apply(IN in) {
            final String topic = cache.getOrDefault(in, topicSelector.apply(in));
            cache.put(in, topic);
            if (cache.size() == CACHE_RESET_SIZE) {
                cache.clear();
            }
            return topic;
        }
    }

    private static class RocketMQMessageSerializationSchemaWrapper<IN>
            implements RocketMQMessageSerializationSchema<IN> {
        private Function<? super IN, String> topicSelector;

        @Nullable private RocketMQTagSelector<? super IN> tagSelector;

        private SerializationSchema<? super IN> valueSerializationSchema;

        @Override
        public void open(InitializationContext context, RocketMQSinkContext sinkContext)
                throws Exception {
            valueSerializationSchema.open(context);
            if (valueSerializationSchema != null) {
                valueSerializationSchema.open(context);
            }
            if (tagSelector != null) {
                tagSelector.open(
                        sinkContext.getParallelInstanceId(),
                        sinkContext.getNumberOfParallelInstances());
            }
        }

        public RocketMQMessageSerializationSchemaWrapper(
                @Nullable Function<? super IN, String> topicSelector,
                @Nullable RocketMQTagSelector<? super IN> tagSelector,
                @Nullable SerializationSchema<? super IN> valueSerializationSchema) {
            this.topicSelector = checkNotNull(topicSelector);
            this.tagSelector = tagSelector;
            this.valueSerializationSchema = checkNotNull(valueSerializationSchema);
        }

        @Override
        public Message serialize(IN element, RocketMQSinkContext context, Long timestamp) {

            final String targetTopic = topicSelector.apply(element);

            final Optional<String> tag =
                    tagSelector != null
                            ? Optional.of(tagSelector.tag(element, targetTopic))
                            : Optional.empty();

            return new Message(
                    targetTopic,
                    tag.orElse(null),
                    null,
                    valueSerializationSchema.serialize(element));
        }
    }
}
