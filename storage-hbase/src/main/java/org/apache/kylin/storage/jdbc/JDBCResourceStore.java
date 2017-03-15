package org.apache.kylin.storage.jdbc;


import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.NavigableSet;

public class JDBCResourceStore extends ResourceStore {

    protected JDBCResourceStore(KylinConfig kylinConfig) {
        super(kylinConfig);
    }

    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath) throws IOException {
        return null;
    }

    @Override
    protected boolean existsImpl(String resPath) throws IOException {
        return false;
    }

    @Override
    protected List<RawResource> getAllResourcesImpl(String folderPath, long timeStart, long timeEndExclusive) throws IOException {
        return null;
    }

    @Override
    protected RawResource getResourceImpl(String resPath) throws IOException {
        return null;
    }

    @Override
    protected long getResourceTimestampImpl(String resPath) throws IOException {
        return 0;
    }

    @Override
    protected void putResourceImpl(String resPath, InputStream content, long ts) throws IOException {

    }

    @Override
    protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS) throws IOException, IllegalStateException {
        return 0;
    }

    @Override
    protected void deleteResourceImpl(String resPath) throws IOException {

    }

    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return null;
    }
}
