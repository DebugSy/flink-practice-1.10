package com.flink.demo.cases.case23.flinkx;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.OutputStream;

public class FtpHandlerTest{

    private FtpHandler ftpHandler1;

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
    }

    @Test
    public void testIsFileExist() throws Exception {
        FTPClient ftpClient = ftpHandler1.getFtpClient();

        OutputStream outputStream = ftpHandler1.getOutputStream("/testftp/part-0-0");
        String data = "abcde";
        byte[] bytes = data.getBytes("UTF-8");
        byte[] rowDelimiterBytes = "\n".getBytes("UTF-8");
        System.err.println(bytes.length + rowDelimiterBytes.length);
        outputStream.write(bytes);
        outputStream.write(rowDelimiterBytes);

        System.err.println(bytes.length + rowDelimiterBytes.length);
        outputStream.write(bytes);
        outputStream.write(rowDelimiterBytes);
        outputStream.close();
        ftpHandler1.completePendingCommand();



    }

    @After
    public void destroy() {
        ftpHandler1.logoutFtpServer();
    }

}