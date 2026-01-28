package io.confluent.ps.netty;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class SimpleConfig extends AbstractConfig {

    public SimpleConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals, false);
    }
    
}
