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

package org.apache.flink.streaming.connectors.rocketmq.common.util;

import org.apache.flink.streaming.connectors.rocketmq.RunningChecker;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Tests for {@link RetryUtil}. */
public class RetryUtilTest extends TestCase {

    private static final Logger log = LoggerFactory.getLogger(RetryUtilTest.class);

    public void testCall() {
        try {
            User user = new User();
            RunningChecker runningChecker = new RunningChecker();
            runningChecker.setRunning(true);
            ExecutorService executorService = Executors.newCachedThreadPool();
            executorService.execute(
                    () ->
                            RetryUtil.call(
                                    () -> {
                                        user.setName("test");
                                        user.setAge(Integer.parseInt("12e"));
                                        return true;
                                    },
                                    "Something is error",
                                    runningChecker));
            Thread.sleep(10000);
            executorService.shutdown();
            log.info("Thread has finished");
            assertEquals(0, user.getAge());
            assertEquals("test", user.getName());
            assertEquals(false, runningChecker.isRunning());
        } catch (Exception e) {
            log.warn("Exception has been caught");
        }
    }

    public class User {
        String name;
        int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
