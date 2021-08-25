/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.flink.legacy.common.selector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** Uses field name to select topic and tag name from tuple. */
public class SimpleTopicSelector implements TopicSelector<Map> {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleTopicSelector.class);

    private final String topicFieldName;
    private final String defaultTopicName;

    private final String tagFieldName;
    private final String defaultTagName;

    /**
     * SimpleTopicSelector Constructor.
     *
     * @param topicFieldName field name used for selecting topic
     * @param defaultTopicName default field name used for selecting topic
     * @param tagFieldName field name used for selecting tag
     * @param defaultTagName default field name used for selecting tag
     */
    public SimpleTopicSelector(
            String topicFieldName,
            String defaultTopicName,
            String tagFieldName,
            String defaultTagName) {
        this.topicFieldName = topicFieldName;
        this.defaultTopicName = defaultTopicName;
        this.tagFieldName = tagFieldName;
        this.defaultTagName = defaultTagName;
    }

    @Override
    public String getTopic(Map tuple) {
        if (tuple.containsKey(topicFieldName)) {
            Object topic = tuple.get(topicFieldName);
            return topic != null ? topic.toString() : defaultTopicName;
        } else {
            LOG.warn(
                    "Field {} Not Found. Returning default topic {}",
                    topicFieldName,
                    defaultTopicName);
            return defaultTopicName;
        }
    }

    @Override
    public String getTag(Map tuple) {
        if (tuple.containsKey(tagFieldName)) {
            Object tag = tuple.get(tagFieldName);
            return tag != null ? tag.toString() : defaultTagName;
        } else {
            LOG.warn("Field {} Not Found. Returning default tag {}", tagFieldName, defaultTagName);
            return defaultTagName;
        }
    }
}
