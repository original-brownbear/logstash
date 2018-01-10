package org.logstash;

import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;

public abstract class LsClusterIntegTestCase extends ESRestTestCase {

    @Before
    public void beforeEach() {
        System.setSecurityManager(null);
    }
}
