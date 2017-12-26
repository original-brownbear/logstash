package org.logstash;

import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

public abstract class LsClusterIntegTestCase extends ESIntegTestCase {

    @Before
    public void beforeEach() {
        ensureGreen();
        System.setSecurityManager(null);
    }
}
