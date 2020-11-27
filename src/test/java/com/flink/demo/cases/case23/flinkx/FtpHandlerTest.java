package com.flink.demo.cases.case23.flinkx;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.OutputStream;

public class FtpHandlerTest{

    private IFtpHandler ftpHandler1;
    private IFtpHandler ftpHandler2;

    @Before
    public void init() {
        FtpConfig ftpConfig = new FtpConfig();
        ftpConfig.setConnectPattern("PORT");
        ftpConfig.setHost("192.168.1.88");
        ftpConfig.setDefaultPort();
        ftpConfig.setUsername("testftp");
        ftpConfig.setPassword("123456");

        ftpHandler1 = new FtpHandler();
        ftpHandler1.loginFtpServer(ftpConfig);

        ftpHandler2 = new FtpHandler();
        ftpHandler2.loginFtpServer(ftpConfig);
    }

    @Test
    public void testIsFileExist() throws Exception {
        boolean fileExist1 = ftpHandler2.isFileExist("/testftp/ftp-sink/2020-11-26-15-49/part-0-0");
        boolean fileExist2 = ftpHandler2.isFileExist("/testftp/ftp-sink/2020-11-26-14-49/part-0-0");
        boolean fileExist3 = ftpHandler2.isFileExist("/testftp/ftp-sink/2020-11-26-14-49/part-0-0");
        System.err.println(fileExist1);
        OutputStream outputStream = ftpHandler1.getOutputStream("/testftp/ftp-sink/2020-11-26-17-23/part-0-0");
//        ftpHandler2.mkDirRecursive("/testftp/ftp-sink/2020-11-26-14-51");
        boolean fileExist4 = ftpHandler2.isFileExist("/testftp/ftp-sink/2020-11-26-14-49/part-0-0");
        System.err.println(fileExist4);

        boolean fileExist5 = ftpHandler1.isFileExist("/testftp/ftp-sink/2020-11-26-14-49/part-0-0");
        System.err.println(fileExist5);
        outputStream.close();
        ftpHandler1.completePendingCommand();

    }

    @After
    public void destroy() {
        ftpHandler1.logoutFtpServer();
        ftpHandler2.logoutFtpServer();
    }

}