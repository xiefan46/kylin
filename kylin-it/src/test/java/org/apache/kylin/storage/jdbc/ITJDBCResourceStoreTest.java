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
