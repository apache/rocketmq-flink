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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link Sql92MessageFilter}. Message properties always arrive as strings, so the cases
 * below also pin down how literals are coerced, which is what decides whether a filter silently
 * accepts every message.
 */
class Sql92MessageFilterTest {

    @Test
    void numericComparisonKeepsOnlyMatchingMessagesTest() {
        Sql92MessageFilter filter = new Sql92MessageFilter("id > 50");

        List<Integer> matched = new ArrayList<>();
        for (int id = 0; id < 100; id++) {
            if (filter.matches(properties("id", String.valueOf(id)))) {
                matched.add(id);
            }
        }

        assertThat(matched).hasSize(49).startsWith(51).endsWith(99);
    }

    @Test
    void stringEqualityAndLogicalOperatorsTest() {
        Sql92MessageFilter filter = new Sql92MessageFilter("region = 'hangzhou' AND retries >= 2");

        assertThat(filter.matches(row("hangzhou", "3"))).isTrue();
        assertThat(filter.matches(row("hangzhou", "1"))).isFalse();
        assertThat(filter.matches(row("beijing", "3"))).isFalse();
    }

    @Test
    void inBetweenAndNotOperatorsTest() {
        Sql92MessageFilter inFilter = new Sql92MessageFilter("level IN ('a', 'b')");
        assertThat(inFilter.matches(properties("level", "a"))).isTrue();
        assertThat(inFilter.matches(properties("level", "c"))).isFalse();

        Sql92MessageFilter betweenFilter = new Sql92MessageFilter("seq BETWEEN 10 AND 20");
        assertThat(betweenFilter.matches(properties("seq", "10"))).isTrue();
        assertThat(betweenFilter.matches(properties("seq", "21"))).isFalse();

        Sql92MessageFilter notFilter = new Sql92MessageFilter("NOT (seq > 5)");
        assertThat(notFilter.matches(properties("seq", "1"))).isTrue();
        assertThat(notFilter.matches(properties("seq", "9"))).isFalse();
    }

    @Test
    void missingPropertyDoesNotMatchTest() {
        Sql92MessageFilter filter = new Sql92MessageFilter("id > 50");

        assertThat(filter.matches(Collections.emptyMap())).isFalse();
        assertThat(filter.matches(properties("other", "99"))).isFalse();
        assertThat(filter.matches(null)).isFalse();
    }

    @Test
    void malformedExpressionFailsAtConstructionTest() {
        assertThatThrownBy(() -> new Sql92MessageFilter("id > > 50"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("id > > 50");
    }

    private static Map<String, String> row(String region, String retries) {
        Map<String, String> properties = new HashMap<>();
        properties.put("region", region);
        properties.put("retries", retries);
        return properties;
    }

    private static Map<String, String> properties(String key, String value) {
        return Collections.singletonMap(key, value);
    }
}
