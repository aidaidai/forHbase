package com.oracle.test;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Created by 代俊朴 on 2017/6/14.
 */
public class TestData {
    public static void main(String[] args) {
        try {
            FileReader fileReader=new FileReader("E:\\OracleXLH\\digData\\新建文件夹\\【阶段18】Hadoop大数据\\shifenzheng.bak");
            BufferedReader bufferedReader=     new BufferedReader(fileReader);
            String str;
            while((str=bufferedReader.readLine())!=null){
                System.out.println(str);
            }

        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }
}
