package com.sunchangpeng.datatail;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class MysqlConnection {
    public static final MysqlConnection INSTANCE = new MysqlConnection();
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

    private DataSource dataSource;
    private QueryRunner queryRunner;

    public void connection(String host, int port, String user, String password) {
        this.dataSource = new DataSource();
        this.dataSource.setUrl(String.format("jdbc:mysql://%s:%s?useSSL=false", host, port));
        this.dataSource.setUsername(user);
        this.dataSource.setPassword(password);
        this.dataSource.setDriverClassName(JDBC_DRIVER);
        this.queryRunner = new QueryRunner(this.dataSource);
    }

    public void refreshColumnsMap(Map<String, List<ColumnInfo>> columnsMap) {
        try {
            DatabaseMetaData databaseMetaData = this.dataSource.getConnection().getMetaData();
            ResultSet resultSet = databaseMetaData.getCatalogs();
            String tableType[] = {"table"};
            while (resultSet.next()) {
                String databaseName = resultSet.getString("TABLE_CAT");
                ResultSet result = databaseMetaData.getTables(databaseName, null, null, tableType);
                while (result.next()) {
                    String tableName = result.getString("TABLE_NAME");
                    String key = databaseName + "." + tableName;
                    columnsMap.put(key, new ArrayList<>());

                    ResultSet colSet = databaseMetaData.getColumns(databaseName, null, tableName, null);
                    while (colSet.next()) {
                        ColumnInfo columnInfo = new ColumnInfo(colSet.getString("COLUMN_NAME"), colSet.getString("TYPE_NAME"));
                        columnsMap.get(key).add(columnInfo);
                    }
                }
            }
        } catch (SQLException e) {
            log.error("", e);
        }
    }

    public int getServerId() {
        try {
            Map<String, Object> item = queryRunner.query("show variables like 'server_id'", new MapHandler());
            if (item != null && !item.isEmpty()) {
                return Integer.valueOf(String.valueOf(item.get("Value")));
            }
        } catch (SQLException e) {
            log.error("", e);
        }
        return 6789;
    }

    public List<BinlogInfo> getBinlogInfo() {
        try {
            List<Map<String, Object>> items = queryRunner.query("show binary logs", new MapListHandler());
            if (items != null && !items.isEmpty()) {
                List<BinlogInfo> binlogInfos = new ArrayList<>();
                for (Map<String, Object> item : items) {
                    binlogInfos.add(new BinlogInfo(String.valueOf(item.get("Log_name")), Long.valueOf(String.valueOf(item.get("File_size")))));
                }
                return binlogInfos;
            }
        } catch (Exception e) {
            log.error("", e);
        }

        return null;
    }

    public BinlogMasterStatus getBinlogMasterStatus() {
        try {
            Map<String, Object> item = queryRunner.query("show master status", new MapHandler());
            if (item != null && !item.isEmpty()) {
                return new BinlogMasterStatus(String.valueOf(item.get("File")), Long.valueOf(String.valueOf(item.get("Position"))));
            }
        } catch (Exception e) {
            log.error("", e);
        }

        return null;
    }
}
