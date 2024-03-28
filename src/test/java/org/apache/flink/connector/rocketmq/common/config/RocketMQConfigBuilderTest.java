package org.apache.flink.connector.rocketmq.common.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RocketMQConfigBuilderTest {

    @Test
    void canNotSetSameOptionTwiceWithDifferentValue() {
        ConfigOption<String> option = ConfigOptions.key("some.key").stringType().noDefaultValue();
        RocketMQConfigBuilder builder = new RocketMQConfigBuilder();
        builder.set(option, "value1");

        assertDoesNotThrow(() -> builder.set(option, "value1"));
        assertThrows(IllegalArgumentException.class, () -> builder.set(option, "value2"));
    }

    @Test
    void setConfigurationCanNotOverrideExistedKeysWithNewValue() {
        ConfigOption<String> option = ConfigOptions.key("string.k1").stringType().noDefaultValue();
        RocketMQConfigBuilder builder = new RocketMQConfigBuilder();

        Configuration configuration = new Configuration();
        configuration.set(option, "value1");

        builder.set(option, "value1");
        assertDoesNotThrow(() -> builder.set(configuration));

        configuration.set(option, "value2");
        assertThrows(IllegalArgumentException.class, () -> builder.set(configuration));
    }

    @Test
    void setPropertiesCanNotOverrideExistedKeysWithNewValueAndSupportTypeConversion() {
        ConfigOption<Integer> option = ConfigOptions.key("int.type").intType().defaultValue(3);
        RocketMQConfigBuilder builder = new RocketMQConfigBuilder();

        Properties properties = new Properties();
        properties.put("int.type", "6");
        assertDoesNotThrow(() -> builder.set(properties));

        properties.put("int.type", "1");
        assertThrows(IllegalArgumentException.class, () -> builder.set(properties));

        Integer value = builder.get(option);
        assertEquals(value, 6);
    }
}
