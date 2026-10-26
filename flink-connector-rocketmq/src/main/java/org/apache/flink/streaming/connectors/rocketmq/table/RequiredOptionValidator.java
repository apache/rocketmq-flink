/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.rocketmq.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.ValidationException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reports the required table options a DDL does not set, naming the missing keys only.
 *
 * <p>{@link org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper#validate()} repeats the
 * table options in its failure message, and a WITH clause normally carries {@code accessKey} and
 * {@code secretKey}. That message ends up in the JobManager log and in the REST exception history,
 * so a typo would turn the credentials into plaintext in both. Factories therefore run this check
 * first and still call the helper afterwards, which keeps unknown option detection unchanged.
 */
final class RequiredOptionValidator {

    private RequiredOptionValidator() {}

    static void validate(Set<ConfigOption<?>> requiredOptions, Map<String, String> tableOptions) {
        List<String> missing =
                requiredOptions.stream()
                        .map(ConfigOption::key)
                        .filter(key -> !tableOptions.containsKey(key))
                        .sorted()
                        .collect(Collectors.toList());
        if (!missing.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Missing required option%s for the RocketMQ connector: %s",
                            missing.size() == 1 ? "" : "s", missing));
        }
    }
}
