package com.test;

import java.io.DataInput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

/**
 * Created by Administrator on 2015/10/19.
 */
public class CommMethod {

   /**
            * 日期转换成Java字符串
    * @param date
    * @return str
    */
    public static String DateToStr(Date date) {

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String str = format.format(date);
        return str;
    }

    /**
     * 字符串转换成日期
     * @param str
     * @return date
     */
    public static Date StrToDate(String str) {

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    public String WaitCommand()
    {
        System.out.print("input 'Q' will quit:\r\n");
        Scanner input=new Scanner(System.in);
        String inputWord = input.nextLine();
//        int x=input.nextInt();
//        switch (x) {
//            case 1:
//                System.out.println("一等奖");
//                break;
//            case 2:
//                System.out.println("二等奖");
//                break;
//            case 3:
//                System.out.println("三等奖");
//                break;
//            default:
//                break;
//        }
        System.out.print("Your input is: " + inputWord);
        return inputWord;
    }
}
