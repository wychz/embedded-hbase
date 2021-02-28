package com.iiichz.schema.impl;

public interface HBaseInterface {
    /** @return the factory for HTable interfaces. */
    HTableInterfaceFactory getHTableFactory();

    /** @return a factory for HBaseAdmin. */
    HBaseAdminFactory getAdminFactory();
}