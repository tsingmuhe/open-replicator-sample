package com.sunchangpeng.datatail;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BinlogInfo {
    private String binlogName;
    private long fileSize;
}
