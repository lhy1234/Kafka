package com.nb;

import org.junit.Test;

import java.io.*;

/**
 * @author lihaoyang
 * @date 2021/4/30
 */
public class SeekTest {

    /**
     * RandomAccessFile的特点在于任意访问文件的任意位置，可以说是基于字节访问的，
     * 可通过getFilePointer()获取当前指针所在位置， 
     * 可通过seek()移动指针，这体现了它的任意性，也是其与其他I/O流相比，自成一派的原因 
     * 一句话总结：seek用于设置文件指针位置，设置后ras会从当前指针的下一位读取到或写入到 
     */
    @Test
    public void test() throws Exception {
        File file = new File("D:/Z_lhy/hh.txt");//创建一个txt文件内容是123456789  
        RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
        //默认情况下ras的指针为0，即从第1个字节读写到  
        accessFile.seek(8);//将accessFile的指针设置到8，则读写ras是从第9个字节读写到  

        File file2 = new File("D:/Z_lhy/pp.txt");
        RandomAccessFile accessFile2 = new RandomAccessFile(file2, "rw");
        accessFile2.setLength(10);
        accessFile2.seek(5);

        byte[] buffer = new byte[32];
        int len = 0;
        while ((len = accessFile.read(buffer)) != -1) {
            accessFile2.write(buffer, 0, len);//从ras2的第6个字节被写入，因为前面设置ras2的指针为5  
            //ras2的写入结果是:pp.txt的内容为前5位是空格，第6位是9  
            //待写入的位置如果有内容将会被新写入的内容替换  
        }
        accessFile.close();
        accessFile2.close();
        System.out.println("ok");

    }

}
