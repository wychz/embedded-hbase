package com.iiichz.schema.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public interface HBaseAdminFactory {
    /**
     * Creates a new HBaseAdmin instance.
     *
     * @param conf Configuration.
     * @return a new HBaseAdmin.
     * @throws IOException on I/O error.
     */
    HBaseAdmin create(Configuration conf) throws IOException;
}