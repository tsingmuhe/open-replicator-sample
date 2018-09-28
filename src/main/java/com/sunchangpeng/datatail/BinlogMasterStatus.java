package com.sunchangpeng.datatail;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BinlogMasterStatus {
    private String binlogName;
    private long position;
}
