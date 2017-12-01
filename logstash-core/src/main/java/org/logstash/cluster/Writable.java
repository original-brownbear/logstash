package org.logstash.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

interface Writable {

    void readNext(DataInput input) throws IOException;

    void writeTo(DataOutput output) throws IOException;
}
