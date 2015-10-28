package com.test;

import java.io.DataInput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.Scanner;

/**
 * Created by Administrator on 2015/10/19.
 */
public class CommMethod {


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
