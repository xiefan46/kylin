package org.apache.kylin.storage.jdbc;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HBaseMetadataTestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import parquet.org.slf4j.Logger;
import parquet.org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by xiefan on 17-3-15.
 */
public class ITJDBCResourceStoreTest extends HBaseMetadataTestCase{

    private KylinConfig kylinConfig;

    private static final Logger logger = LoggerFactory.getLogger(ITJDBCResourceStoreTest.class);

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        kylinConfig = KylinConfig.getInstanceFromEnv();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testConnectJDBC() throws Exception{
        Class.forName(kylinConfig.getJdbcDriverClass());
        Connection conn = DriverManager.getConnection(kylinConfig.getJdbcUrl());
    }

}
