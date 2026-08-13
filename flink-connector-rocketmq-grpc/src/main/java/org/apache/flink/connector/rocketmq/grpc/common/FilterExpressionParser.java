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

package org.apache.flink.connector.rocketmq.grpc.common;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.StringUtils;

import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;

/**
 * Parses a filter definition into an SDK {@link FilterExpression}. A {@code null} or blank
 * expression matches all messages ({@link FilterExpression#SUB_ALL}).
 */
@Internal
public class FilterExpressionParser {

    private FilterExpressionParser() {}

    /**
     * Build a {@link FilterExpression} of the given type.
     *
     * @param expression the tag expression (e.g. {@code "tagA||tagB"}) or SQL92 expression; a blank
     *     value subscribes to all messages.
     * @param type the filter type; a blank value defaults to {@link FilterExpressionType#TAG}.
     */
    public static FilterExpression parse(String expression, String type) {
        if (StringUtils.isNullOrWhitespaceOnly(expression) || "*".equals(expression.trim())) {
            return FilterExpression.SUB_ALL;
        }
        final FilterExpressionType filterType = parseType(type);
        return new FilterExpression(expression.trim(), filterType);
    }

    private static FilterExpressionType parseType(String type) {
        if (StringUtils.isNullOrWhitespaceOnly(type)) {
            return FilterExpressionType.TAG;
        }
        switch (type.trim().toUpperCase()) {
            case "SQL92":
                return FilterExpressionType.SQL92;
            case "TAG":
                return FilterExpressionType.TAG;
            default:
                throw new IllegalArgumentException("Unsupported filter expression type: " + type);
        }
    }
}
