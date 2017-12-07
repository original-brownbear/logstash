package org.logstash.ext;

public interface EventQueue {

    void push(JrubyEventExtLibrary.RubyEvent event);
}
