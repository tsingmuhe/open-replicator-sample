package com.sunchangpeng.datatail;

import com.google.code.or.binlog.impl.event.TableMapEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class TableInfoKeeper {
    private static Map<Long, TableInfo> tablesMap = new ConcurrentHashMap<>();
    private static Map<String, List<ColumnInfo>> columnsMap = new ConcurrentHashMap<>();

    public static void saveTablesMap(TableMapEvent event) {
        long tableId = event.getTableId();
        String databaseName = String.valueOf(event.getDatabaseName());
        String tableName = String.valueOf(event.getTableName());
        TableInfo tableInfo = TableInfo.builder().databaseName(databaseName).tableName(tableName).fullName(databaseName + "." + tableName).build();
        tablesMap.put(tableId, tableInfo);
    }

    public static TableInfo getTableInfo(long tableId) {
        return tablesMap.get(tableId);
    }

    public static List<ColumnInfo> getColumns(String fullName) {
        return columnsMap.get(fullName);
    }

    public static void refreshColumnsMap() {
        MysqlConnection.INSTANCE.refreshColumnsMap(columnsMap);
    }
}
