package com.sunchangpeng;

import com.alibaba.fastjson.JSON;
import com.google.code.or.OpenReplicator;
import com.sunchangpeng.datatail.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

public class App {
    public static final String USER = "root";
    public static final String PASSWORD = "root";
    public static final String HOST = "localhost";
    public static final int PORT = 3306;

    public static void main(String[] args) throws Exception {
        MysqlConnection.INSTANCE.connection(HOST, PORT, USER, PASSWORD);

        OpenReplicator or = new OpenReplicator();
        or.setUser(USER);
        or.setPassword(PASSWORD);
        or.setHost(HOST);
        or.setPort(PORT);
        or.setServerId(MysqlConnection.INSTANCE.getServerId());
        BinlogMasterStatus binlogMasterStatus = MysqlConnection.INSTANCE.getBinlogMasterStatus();
        or.setBinlogFileName(binlogMasterStatus.getBinlogName());
        or.setBinlogPosition(binlogMasterStatus.getPosition());
        or.setBinlogEventListener(new InstanceListener());

        or.start();

        Thread thread = new Thread(new PrintCDCEvent());
        thread.start();

        System.out.println("press 'q' to stop");
        final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        for (String line = br.readLine(); line != null; line = br.readLine()) {
            if (line.equals("q")) {
                or.stop(1, TimeUnit.SECONDS);
                break;
            }
        }
    }

    public static class PrintCDCEvent implements Runnable {
        @Override
        public void run() {
            while (true) {
                if (CDCEventManager.queue.isEmpty() == false) {
                    CDCEvent cdcEvent = CDCEventManager.queue.pollFirst();
                    System.out.println(JSON.toJSONString(cdcEvent));
                } else {
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
