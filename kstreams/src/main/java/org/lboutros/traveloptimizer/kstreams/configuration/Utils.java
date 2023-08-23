package org.lboutros.configuration;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.lboutros.configuration.Constants.APPLICATION_ID_PREFIX;

public class Utils {

    public static Map<String, Object> toMap(Properties streamsConfiguration) {
        return streamsConfiguration.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue));
    }

    public static Properties readConfiguration(String propertyFileName) throws ConfigurationException {
        return readConfiguration(null, propertyFileName);
    }

    public static Properties readConfiguration(String systemPropertyFileName, String propertyFileName) throws ConfigurationException {
        if (systemPropertyFileName != null) {
            Configuration systemConfiguration = ((PropertiesConfiguration) new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                    .configure(new Parameters()
                            .properties()
                            .setFileName(systemPropertyFileName))
                    .getConfiguration())
                    .interpolatedConfiguration();

            Properties systemProperties = ConfigurationConverter.getProperties(systemConfiguration);
            systemProperties.forEach((key, value) -> System.setProperty(key.toString(), value.toString()));
        }
        Configuration configuration = ((PropertiesConfiguration) new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                .configure(new Parameters()
                        .properties()
                        .setFileName(propertyFileName))
                .getConfiguration())
                .interpolatedConfiguration();

        return ConfigurationConverter.getProperties(configuration);
    }

    public static void setApplicationId(Properties properties, String applicationId) {
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID_PREFIX + applicationId);
    }
}
