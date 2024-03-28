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

package org.apache.flink.connector.rocketmq.common.config;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A config validator for building {@link RocketMQConfiguration} in {@link RocketMQConfigBuilder}.
 * It's used for source & sink builder.
 *
 * <p>We would validate:
 *
 * <ul>
 *   <li>If the user has provided the required config options.
 *   <li>If the user has provided some conflict options.
 * </ul>
 */
@Internal
public class RocketMQConfigValidator {

    private final List<Set<ConfigOption<?>>> conflictOptions;
    private final Set<ConfigOption<?>> requiredOptions;

    private RocketMQConfigValidator(
            List<Set<ConfigOption<?>>> conflictOptions, Set<ConfigOption<?>> requiredOptions) {
        this.conflictOptions = conflictOptions;
        this.requiredOptions = requiredOptions;
    }

    /** Package private validating for using in {@link RocketMQConfigBuilder}. */
    void validate(Configuration configuration) {
        requiredOptions.forEach(
                option ->
                        checkArgument(
                                configuration.contains(option),
                                "Config option %s is not provided for rocketmq client.",
                                option));

        conflictOptions.forEach(
                options -> {
                    long nums = options.stream().filter(configuration::contains).count();
                    checkArgument(
                            nums <= 1,
                            "Conflict config options %s were provided, "
                                    + "we only support one of them for creating rocketmq client.",
                            options);
                });
    }

    /** Return the builder for building {@link RocketMQConfigValidator}. */
    public static RocketMQConfigValidatorBuilder builder() {
        return new RocketMQConfigValidatorBuilder();
    }

    /** Builder pattern for building {@link RocketMQConfigValidator}. */
    public static class RocketMQConfigValidatorBuilder {

        private final List<Set<ConfigOption<?>>> conflictOptions = new ArrayList<>();
        private final Set<ConfigOption<?>> requiredOptions = new HashSet<>();

        public RocketMQConfigValidatorBuilder conflictOptions(ConfigOption<?>... options) {
            checkArgument(options.length > 1, "You should provide at least two conflict options.");
            conflictOptions.add(ImmutableSet.copyOf(options));
            return this;
        }

        public RocketMQConfigValidatorBuilder requiredOption(ConfigOption<?> option) {
            requiredOptions.add(option);
            return this;
        }

        public RocketMQConfigValidator build() {
            ImmutableList<Set<ConfigOption<?>>> conflict = ImmutableList.copyOf(conflictOptions);
            Set<ConfigOption<?>> required = ImmutableSet.copyOf(requiredOptions);

            return new RocketMQConfigValidator(conflict, required);
        }
    }
}
