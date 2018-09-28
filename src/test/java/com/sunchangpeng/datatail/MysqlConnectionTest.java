package com.sunchangpeng.datatail;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MysqlConnectionTest {
    public static final String USER = "root";
    public static final String PASSWORD = "root";
    public static final String HOST = "localhost";
    public static final int PORT = 3306;

    @Test
    public void getServerId() {
        MysqlConnection.INSTANCE.connection(HOST, PORT, USER, PASSWORD);
        System.out.println(MysqlConnection.INSTANCE.getServerId());
    }

    @Test
    public void getBinlogInfo() {
        MysqlConnection.INSTANCE.connection(HOST, PORT, USER, PASSWORD);
        System.out.println(MysqlConnection.INSTANCE.getBinlogInfo());
    }

    @Test
    public void getBinlogMasterStatus() {
        MysqlConnection.INSTANCE.connection(HOST, PORT, USER, PASSWORD);
        System.out.println(MysqlConnection.INSTANCE.getBinlogMasterStatus());
    }


    @Test
    public void refreshColumnsMap() {
        MysqlConnection.INSTANCE.connection(HOST, PORT, USER, PASSWORD);
        Map<String, List<ColumnInfo>> columnsMap = new HashMap<>();
        MysqlConnection.INSTANCE.refreshColumnsMap(columnsMap);
        System.out.println(columnsMap);
    }
}