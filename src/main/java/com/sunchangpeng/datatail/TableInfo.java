package com.sunchangpeng.datatail;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TableInfo {
    private String databaseName;
    private String tableName;
    private String fullName;
}
