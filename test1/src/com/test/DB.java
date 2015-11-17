package com.test;

import com.test.Entity.KeyWordEntity;
import com.test.Entity.KeyWordRelWordEntity;
import com.test.Entity.SparkCmdEntity;

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

    public  List<SparkCmdEntity>  GetTaskCmdList() throws SQLException {
        DBHelper conn = new DBHelper();
        conn.OpenConnection(ConnectionString114);
        ResultSet rset = conn.ExecuteQuery("select * from SparkCmd where State=0");
        List<SparkCmdEntity> list = new ArrayList<SparkCmdEntity>();
        while(rset.next()) {
            SparkCmdEntity sce = new SparkCmdEntity();
            sce.IDX = rset.getInt("IDX");
            sce.Type = rset.getInt("Type");
            sce.Description= rset.getString("Description");
            sce.State = rset.getInt("Type");
            sce.InputData= rset.getString("InputData");
            sce.Creator= rset.getString("Creator");
            sce. CreateTime= rset.getString("CreateTime");
            sce.StartTime= rset.getString("StartTime");
            sce.EndTime= rset.getString("EndTime");
            list.add(sce);
        }
        return list;
    }

    public void UpdateTaskCmd(List<SparkCmdEntity> list)
    {

        DBHelper conn = new DBHelper();
        conn.OpenConnection(ConnectionString114);


        for(SparkCmdEntity e :list)
        {
            String sql = "Update SparkCmd  Set State=" + e.State + " , StartTime = '" + e.StartTime + "' , EndTime='" + e.EndTime +"' WHERE　IDX=" + e.IDX;

            System.out.println(sql);
            conn.ExecuteUpdate(sql);
        }
    }

    public void UpdateOneTaskCmd(SparkCmdEntity cmd) {

        DBHelper conn = new DBHelper();
        conn.OpenConnection(ConnectionString114);

        String sql = "Update SparkCmd  Set State=" + cmd.State + " , StartTime = '" + cmd.StartTime + "' , EndTime='" + cmd.EndTime + "' WHERE　IDX=" + cmd.IDX;

        System.out.println(sql);
        conn.ExecuteUpdate(sql);
    }

    public void ExecSQL(String sql) {
        DBHelper conn = new DBHelper();
        conn.OpenConnection(ConnectionString114);
        System.out.println(sql);
        conn.ExecuteUpdate(sql);
    }

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
        ResultSet rset = conn.ExecuteQuery("select * from KeyWordRelWords where State=1 and keyWord='原生态'" );

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


    public  void BatchInsert(List<String>  valueList, String sql)
    {
        DBHelper conn = new DBHelper();
        conn.OpenConnection(ConnectionString114);

        for(String values : SplitValuesForBatch(valueList)) {
            System.out.println(sql + values);
            conn.ExecuteUpdate(sql + values);
        }
    }
    public void InsertHotelGroupKeyWordBatch(int TaskID, List<String>  valueList  )
    {
        String delSQL = "delete SparkCmdResult WHERE taskID = " + String.valueOf(TaskID);
        ExecSQL(delSQL);

        String sql = "INSERT INTO [dbo].[SparkCmdResult]([TaskID],[HotelID] ,[Total]) VALUES";
        BatchInsert(valueList, sql);
    }

    public void InsertSparkCmdResultWithWritingBatch(int TaskID, List<String>  valueList  )
    {
        String delSQL = "delete SparkCmdResultWithWriting WHERE taskID = " + String.valueOf(TaskID);
        ExecSQL(delSQL);

        String sql = "INSERT INTO [dbo].[SparkCmdResultWithWriting]([TaskID],[HotelID] ,[Writing]) VALUES";
        BatchInsert(valueList, sql);
    }

    public void DelHotelKeyWordCountByTaskID(int TaskID )
    {
        String delSQL = "delete HotelKeyWordCountWithWriting WHERE taskID = " + String.valueOf(TaskID);
        ExecSQL(delSQL);
    }

    public void InsertHotelKeyWordCountBatchWithWriting(List<String>  valueList)
    {
        String sql = "INSERT INTO [dbo].[HotelKeyWordCountWithWriting] ([hotelid] ,[KeyWord] ,[NO] ,[Writing],[TaskID]) VALUES";
        BatchInsert(valueList,sql);
    }

    public void InsertHotelKeyWordCountBatch(List<String>  valueList  )
    {
        String sql = "INSERT INTO [dbo].[HotelKeyWordCount] ([hotelid] ,[KeyWord] ,[NO] ,[Total]) VALUES";
        BatchInsert(valueList,sql);
    }

    public void InsertRelWordBatch(List<String>  valueList )
    {
        String sql = "INSERT INTO [dbo].[KeyWordRelWords]([KeyWord] ,[RelWord]  ,[RelWordPOS]  ,[State]  ,[CreateTime] ,[Total]) VALUES";
        BatchInsert(valueList,sql);
    }

    public void InsertHotelKeyWordRelWordBatch(List<String> valueList )
    {
        String sql = "INSERT INTO [dbo].[HotelKeyWordRelWord]([HotelID] ,[KeyWord] ,[RelWord],[RelWordPOS] ,[ADV] ,[ADVPOS],[NO],[NOPOS])  VALUES";
        BatchInsert(valueList,sql);
    }

    public  List<String> SplitValuesForBatch(List<String> values )
    {
        List<String> list=new ArrayList<String>();

        int lenght = 900;
        int index = 0;
        StringBuilder sb = new StringBuilder();
        for(String str : values )
        {
            index++;
            sb.append(str);

            if(index > lenght)
            {
                list.add(sb.toString());
                index = 0;
                sb = new StringBuilder();
            }
            else
            {
                sb.append(",");
            }

        }

        if( sb.length() > 0)
        {
            list.add(sb.toString().substring(0, sb.toString().length() -1));
        }

        return list;

    }
}
