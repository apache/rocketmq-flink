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

package org.apache.flink.connector.rocketmq.common.filter;

import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.filter.FilterFactory;
import org.apache.rocketmq.filter.expression.EvaluationContext;
import org.apache.rocketmq.filter.expression.Expression;
import org.apache.rocketmq.filter.expression.MQFilterException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Evaluates a SQL92 expression against the properties of a single message.
 *
 * <p>{@link org.apache.rocketmq.client.consumer.DefaultLitePullConsumer} can only carry a tag
 * sub-expression while queues are assigned explicitly, and assigning queues explicitly is what
 * keeps split assignment under the control of the Flink enumerator. A SQL92 filter therefore has to
 * be evaluated here rather than on the broker.
 *
 * <p>The expression is compiled once by the reader that owns this filter. Compilation uses the same
 * {@code rocketmq-filter} implementation the broker uses, so predicate semantics, including the
 * coercion between string properties and numeric literals, match server side SQL92 filtering.
 */
public final class Sql92MessageFilter {

    private final String expressionString;
    private final Expression expression;

    public Sql92MessageFilter(String expressionString) {
        this.expressionString = expressionString;
        try {
            this.expression =
                    FilterFactory.INSTANCE.get(ExpressionType.SQL92).compile(expressionString);
        } catch (MQFilterException e) {
            throw new IllegalArgumentException(
                    "Invalid SQL92 filter expression '" + expressionString + "'", e);
        }
    }

    /**
     * Returns whether the message satisfies the expression.
     *
     * <p>A message whose properties cannot be evaluated, for instance because the expression
     * dereferences a missing property in an unsupported way, does not satisfy the predicate. This
     * mirrors the broker, which drops such messages instead of failing the pull request.
     */
    public boolean matches(Map<String, String> properties) {
        try {
            return Boolean.TRUE.equals(expression.evaluate(new PropertiesContext(properties)));
        } catch (Exception e) {
            return false;
        }
    }

    public String getExpressionString() {
        return expressionString;
    }

    /** Adapts the immutable string properties of a message to the filter evaluation context. */
    private static final class PropertiesContext implements EvaluationContext {

        private final Map<String, String> properties;

        private PropertiesContext(Map<String, String> properties) {
            this.properties = properties == null ? Collections.emptyMap() : properties;
        }

        @Override
        public Object get(String name) {
            return properties.get(name);
        }

        @Override
        public Map<String, Object> keyValues() {
            return Collections.unmodifiableMap(new HashMap<String, Object>(properties));
        }
    }
}
