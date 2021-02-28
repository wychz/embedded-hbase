package com.iiichz.schema.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public interface HTableInterfaceFactory {

    Table create(Configuration conf, String tableName) throws IOException;
}
