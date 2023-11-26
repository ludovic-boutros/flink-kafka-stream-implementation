package org.lboutros.traveloptimizer.kstreams.configuration;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Options;

import java.util.Map;

public class RockDBConfig implements RocksDBConfigSetter {
    @Override
    public void setConfig(String s, Options options, Map<String, Object> map) {

        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig)
                options.tableFormatConfig();

        tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
    }

    @Override
    public void close(String s, Options options) {

    }
}
