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

package org.apache.flink.connector.rocketmq.source.config;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;

/** The enum class for defining the offset verify behavior. */
@PublicEvolving
public enum OffsetVerification implements DescribedEnum {

    /** We would just fail the consuming. */
    FAIL_ON_MISMATCH(
            text("Fail the consuming from RocketMQ when we don't find the related offset.")),

    /** Print one warn message and start consuming from the valid offset. */
    WARN_ON_MISMATCH(text("Print a warn message and start consuming from the valid offset."));

    private final InlineElement desc;

    OffsetVerification(InlineElement desc) {
        this.desc = desc;
    }

    @Internal
    @Override
    public InlineElement getDescription() {
        return desc;
    }
}
