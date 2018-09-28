package com.sunchangpeng.datatail;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.*;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.util.MySQLConstants;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class InstanceListener implements BinlogEventListener {
    public void onEvents(BinlogEventV4 event) {
        if (event == null) {
            log.error("binlog event is null");
            return;
        }

        int eventType = event.getHeader().getEventType();

        switch (eventType) {
            case MySQLConstants.FORMAT_DESCRIPTION_EVENT: {
                log.info("FORMAT_DESCRIPTION_EVENT");
                break;
            }
            case MySQLConstants.TABLE_MAP_EVENT: {
                TableMapEvent tableMapEvent = (TableMapEvent) event;
                log.info("TABLE_MAP_EVENT.tableId: {}", tableMapEvent.getTableId());
                TableInfoKeeper.saveTablesMap(tableMapEvent);
                break;
            }

            case MySQLConstants.DELETE_ROWS_EVENT: {
                DeleteRowsEvent deleteRowsEvent = (DeleteRowsEvent) event;
                log.info("DELETE_ROWS_EVENT.tableId: {}", deleteRowsEvent.getTableId());
                TableInfo tableInfo = TableInfoKeeper.getTableInfo(deleteRowsEvent.getTableId());
                List<Row> rows = deleteRowsEvent.getRows();
                deleteRowsEvent(deleteRowsEvent, tableInfo, rows);
                break;
            }
            case MySQLConstants.DELETE_ROWS_EVENT_V2: {
                DeleteRowsEventV2 deleteRowsEvent = (DeleteRowsEventV2) event;
                log.info("DELETE_ROWS_EVENT_V2.tableId: {}", deleteRowsEvent.getTableId());
                TableInfo tableInfo = TableInfoKeeper.getTableInfo(deleteRowsEvent.getTableId());
                List<Row> rows = deleteRowsEvent.getRows();
                deleteRowsEvent(deleteRowsEvent, tableInfo, rows);
                break;
            }
            case MySQLConstants.UPDATE_ROWS_EVENT: {
                UpdateRowsEvent updateRowsEvent = (UpdateRowsEvent) event;
                log.info("UPDATE_ROWS_EVENT.tableId: {}", updateRowsEvent.getTableId());
                TableInfo tableInfo = TableInfoKeeper.getTableInfo(updateRowsEvent.getTableId());
                List<Pair<Row>> rows = updateRowsEvent.getRows();
                updateRowsEvent(updateRowsEvent, tableInfo, rows);
                break;
            }
            case MySQLConstants.UPDATE_ROWS_EVENT_V2: {
                UpdateRowsEventV2 updateRowsEvent = (UpdateRowsEventV2) event;
                log.info("UPDATE_ROWS_EVENT_V2.tableId: {}", updateRowsEvent.getTableId());
                TableInfo tableInfo = TableInfoKeeper.getTableInfo(updateRowsEvent.getTableId());
                List<Pair<Row>> rows = updateRowsEvent.getRows();
                updateRowsEvent(updateRowsEvent, tableInfo, rows);
                break;
            }
            case MySQLConstants.WRITE_ROWS_EVENT: {
                WriteRowsEvent writeRowsEvent = (WriteRowsEvent) event;
                log.info("WRITE_ROWS_EVENT.tableId: {}", writeRowsEvent.getTableId());
                TableInfo tableInfo = TableInfoKeeper.getTableInfo(writeRowsEvent.getTableId());
                List<Row> rows = writeRowsEvent.getRows();
                writeRowsEvent(writeRowsEvent, tableInfo, rows);
                break;
            }
            case MySQLConstants.WRITE_ROWS_EVENT_V2: {
                WriteRowsEventV2 writeRowsEvent = (WriteRowsEventV2) event;
                log.info("WRITE_ROWS_EVENT_V2.tableId: {}", writeRowsEvent.getTableId());
                TableInfo tableInfo = TableInfoKeeper.getTableInfo(writeRowsEvent.getTableId());
                List<Row> rows = writeRowsEvent.getRows();
                writeRowsEvent(writeRowsEvent, tableInfo, rows);
                break;
            }
            case MySQLConstants.QUERY_EVENT: {
                QueryEvent queryEvent = (QueryEvent) event;
                String databaseName = queryEvent.getDatabaseName().toString();
                log.info("QUERY_EVENT.databaseName: {}", databaseName);

                CDCEvent cdcEvent = new CDCEvent(queryEvent, databaseName, null);
                cdcEvent.setDdl(true);
                cdcEvent.setSql(queryEvent.getSql().toString());

                CDCEventManager.queue.addLast(cdcEvent);
                break;
            }
            case MySQLConstants.XID_EVENT: {
                XidEvent xidEvent = (XidEvent) event;
                log.info("XID_EVENT.xid: {}", xidEvent.getXid());
                break;
            }
            default: {
                log.info("DEFAULT: {}", eventType);
            }
        }

    }

    private void deleteRowsEvent(BinlogEventV4 event, TableInfo tableInfo, List<Row> rows) {
        for (Row row : rows) {
            List<Column> before = row.getColumns();
            Map<String, String> beforeMap = getMap(before, tableInfo.getFullName());

            if (before != null && !before.isEmpty()) {
                CDCEvent cdcEvent = new CDCEvent(event, tableInfo.getDatabaseName(), tableInfo.getTableName());
                cdcEvent.setBefore(beforeMap);

                CDCEventManager.queue.addLast(cdcEvent);
            }
        }
    }

    private void updateRowsEvent(BinlogEventV4 event, TableInfo tableInfo, List<Pair<Row>> rows) {
        for (Pair<Row> p : rows) {
            List<Column> colsBefore = p.getBefore().getColumns();
            List<Column> colsAfter = p.getAfter().getColumns();

            Map<String, String> beforeMap = getMap(colsBefore, tableInfo.getFullName());
            Map<String, String> afterMap = getMap(colsAfter, tableInfo.getFullName());

            if ((beforeMap != null && !beforeMap.isEmpty()) && (afterMap != null && !afterMap.isEmpty())) {
                CDCEvent cdcEvent = new CDCEvent(event, tableInfo.getDatabaseName(), tableInfo.getTableName());
                cdcEvent.setBefore(beforeMap);
                cdcEvent.setAfter(afterMap);

                CDCEventManager.queue.addLast(cdcEvent);
            }
        }
    }

    private void writeRowsEvent(BinlogEventV4 event, TableInfo tableInfo, List<Row> rows) {
        for (Row row : rows) {
            List<Column> cols = row.getColumns();
            Map<String, String> afterMap = getMap(cols, tableInfo.getFullName());
            if (afterMap != null && !afterMap.isEmpty()) {
                CDCEvent cdcEvent = new CDCEvent(event, tableInfo.getDatabaseName(), tableInfo.getTableName());
                cdcEvent.setAfter(afterMap);

                CDCEventManager.queue.addLast(cdcEvent);
            }
        }
    }

    private Map<String, String> getMap(List<Column> cols, String fullName) {
        if (cols == null || cols.isEmpty()) {
            return null;
        }

        List<ColumnInfo> columnInfoList = TableInfoKeeper.getColumns(fullName);
        if (columnInfoList == null || columnInfoList.isEmpty() || cols.size() != columnInfoList.size()) {
            TableInfoKeeper.refreshColumnsMap();
            columnInfoList = TableInfoKeeper.getColumns(fullName);
        }

        if (columnInfoList == null || columnInfoList.isEmpty() || cols.size() != columnInfoList.size()) {
            log.warn("ColumnInfo size is not equal to Column.fullname: {}", fullName);
            return null;
        }

        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < columnInfoList.size(); i++) {
            if (cols.get(i).getValue() == null) {
                map.put(columnInfoList.get(i).getName(), "");
            } else {
                map.put(columnInfoList.get(i).getName(), String.valueOf(cols.get(i)));
            }
        }
        return map;
    }
}
