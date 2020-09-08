package com.stackroute.productservice;


import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

import java.util.ArrayList;
import java.util.List;

/**
 * Appender that writes all logging events to a single static log. All logging
 * statements written during a test are captured
 */
public class StaticAppender extends AppenderBase<ILoggingEvent> {
    static List<ILoggingEvent> events = new ArrayList<>();

    @Override
    public void append(ILoggingEvent e) {
        events.add(e);
    }

    public static List<ILoggingEvent> getEvents() {
        return events;
    }

    public static void clearEvents() {
        events.clear();
    }
}

