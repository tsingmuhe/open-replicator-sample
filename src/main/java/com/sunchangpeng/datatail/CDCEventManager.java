package com.sunchangpeng.datatail;

import java.util.concurrent.ConcurrentLinkedDeque;

public class CDCEventManager {
    public static final ConcurrentLinkedDeque<CDCEvent> queue = new ConcurrentLinkedDeque<>();
}
