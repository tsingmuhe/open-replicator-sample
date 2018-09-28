package com.sunchangpeng.datatail;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.BinlogEventV4Header;
import lombok.Data;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Data
public class CDCEvent {
    private static AtomicLong uuid = new AtomicLong(0);

    private String databaseName;
    private String tableName;
    //事件唯一标识
    private long eventId = 0;
    //事件类型
    private int eventType = 0;
    private long timestamp = 0;
    private long timestampReceipt = 0;
    private long serverId;
    private long position;
    private long nextPosition;

    private Map<String, String> before;
    private Map<String, String> after;
    private boolean ddl;
    private String sql;

    public CDCEvent(BinlogEventV4 event, String databaseName, String tableName) {
        this.init(event);
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    private void init(BinlogEventV4 event) {
        this.eventId = uuid.getAndAdd(1);
        BinlogEventV4Header header = event.getHeader();
        this.eventType = header.getEventType();
        this.timestamp = header.getTimestamp();
        this.timestampReceipt = header.getTimestampOfReceipt();
        this.serverId = header.getServerId();
        this.position = header.getPosition();
        this.nextPosition = header.getNextPosition();
    }
}
