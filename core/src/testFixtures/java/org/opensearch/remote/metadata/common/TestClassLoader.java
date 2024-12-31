package org.opensearch.remote.metadata.common;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;

public class TestClassLoader extends ClassLoader {
    private static final String SERVICE_FILE = "META-INF/services/org.opensearch.remote.metadata.client.SdkClientDelegate";

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        if (SERVICE_FILE.equals(name)) {
            URL url = getClass().getClassLoader().getResource(name);
            return Collections.enumeration(Collections.singletonList(url));
        }
        return super.getResources(name);
    }
}
