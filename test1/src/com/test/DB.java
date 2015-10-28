package com.test;

import com.test.Entity.KeyWordEntity;
import com.test.Entity.KeyWordRelWordEntity;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2015/10/19.
 */
public class DB {
    public final String ConnectionString113 = "jdbc:sqlserver://192.168.1.113:1433;DatabaseName=CommentDB;User=DataAnalysis;Password=DataAnalysis01!";
    public final String ConnectionString114 = "jdbc:sqlserver://192.168.1.114:1433;DatabaseName=CommentDB;User=DataAnalysis;Password=DataAnalysis01!";



    public  List<KeyWordEntity>  GetKeyWordsList() throws SQLException {
        DBHelper conn = new DBHelper();
        conn.OpenConnection(ConnectionString114);
        ResultSet rset = conn.ExecuteQuery("select * from keywords  ");

        List<KeyWordEntity> list = new ArrayList<KeyWordEntity>();

         while(rset.next()) {
            KeyWordEntity kw = new KeyWordEntity();

            kw.ID = rset.getInt("ID");
            kw.KeyWord = rset.getString("KeyWord");

            list.add(kw);

        }
        return list;
    }

    public  List<KeyWordRelWordEntity>  GetKeyWordsRelWordList() throws SQLException {
        DBHelper conn = new DBHelper();
        conn.OpenConnection(ConnectionString114);
        ResultSet rset = conn.ExecuteQuery("select * from KeyWordRelWords where State=1");

        List<KeyWordRelWordEntity> list = new ArrayList<KeyWordRelWordEntity>();

        while(rset.next()) {
            KeyWordRelWordEntity kw = new KeyWordRelWordEntity();

            kw.IDX = rset.getInt("IDX");
            kw.KeyWord = rset.getString("KeyWord");
            kw.RelWord = rset.getString("RelWord");
            kw.RelWordPOS = rset.getString("RelWordPOS");

            list.add(kw);
        }
        return list;
    }



    public void InsertRelWordData( String str, int i )
    {
        System.out.println(str);
        String [] sp = str.split(":");
        if(sp.length==3) {
           InsertRelWord(sp[0], sp[1], sp[2], i);
        }
    }

    public void InsertRelWord(String KeyWord,String RelWord, String RelWordPOS,int count)
    {
        DBHelper conn = new DBHelper();
        conn.OpenConnection(ConnectionString114);

        String sql = "INSERT INTO [dbo].[KeyWordRelWords]([KeyWord] ,[RelWord  ,[RelWordPOS]  ,[State]  ,[CreateTime] ,[Total]) VALUES ('" + KeyWord + "' ,'" + RelWord + "' ,'" +RelWordPOS +"',0 ,GetDate() ," +  String.valueOf(count) +" )";

        System.out.println(sql);
        conn.ExecuteUpdate(sql);

    }

    public void InsertRelWordBatch(String  values )
    {
        DBHelper conn = new DBHelper();
        conn.OpenConnection(ConnectionString114);

        values = values.substring(0, values.length()-1);

        String sql = "INSERT INTO [dbo].[KeyWordRelWords]([KeyWord] ,[RelWord]  ,[RelWordPOS]  ,[State]  ,[CreateTime] ,[Total]) VALUES";

        System.out.println(sql + values);

        conn.ExecuteUpdate(sql + values);
    }

    public void InsertHotelKeyWordRelWordBatch(String  values )
    {
        DBHelper conn = new DBHelper();
        conn.OpenConnection(ConnectionString114);

        values = values.substring(0, values.length()-1);

        String sql = "INSERT INTO [dbo].[HotelKeyWordRelWord]([HotelID] ,[KeyWord] ,[RelWord],[RelWordPOS] ,[ADV] ,[ADVPOS],[NO],[NOPOS])  VALUES";

        System.out.println(sql + values);

        conn.ExecuteUpdate(sql + values);
    }
}
