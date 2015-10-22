package com.test;
import java.text.*;
import java.util.*;
import java.sql.*;
//import com.jstrd.htgl.webservice.XmlUtility; /*XML可能用到*/


public class DBConn {

  public String ClassString=null;
  public String ConnectionString=null;
  public String UserName=null;
  public String PassWord=null;

  public Connection Conn;
  public Statement Stmt;


  public DBConn() {
	  
    ClassString="com.microsoft.sqlserver.jdbc.SQLServerDriver";
    ConnectionString="jdbc:sqlserver://192.168.1.113:1433;DatabaseName=CommentDB;User=DataAnalysis;Password=DataAnalysis01!";

  }

  //打开连接
  public boolean OpenConnection()
  {
   boolean mResult=true;
   try
   {
     Class.forName(ClassString);
     if ((UserName==null) && (PassWord==null))
     {
       Conn= DriverManager.getConnection(ConnectionString);
     }
     else
     {
       Conn= DriverManager.getConnection(ConnectionString,UserName,PassWord);
     }

     Stmt=Conn.createStatement();
     mResult=true;
   }
   catch(Exception e)
   {
     System.out.println(e.toString());
     mResult=false;
   }
   return (mResult);
  }

  //关闭数据库连接
  public void CloseConnection()
  {
   try
   {
     Stmt.close();
     Conn.close();
   }
   catch(Exception e)
   {
     System.out.println(e.toString());
   }
  }

  //获取当前时间(JAVA)
  public String GetDateTime()
  {
   Calendar cal  = Calendar.getInstance();
   SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
   String mDateTime=formatter.format(cal.getTime());
   return (mDateTime);
  }

  //获取当前时间(T-SQL)
  public  java.sql.Date  GetDate()
  {   
    Calendar cal  = Calendar.getInstance();
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    String mDateTime=formatter.format(cal.getTime());
    return (java.sql.Date.valueOf(mDateTime));
  }

   //生成新的ID
   public int GetMaxID(String vTableName,String vFieldName)
  {
   int mResult=0;
   boolean mConn=true;
   String mSql=new String();
   mSql = "select max("+vFieldName+")+1 as MaxID from " + vTableName;
   try
   {
       if (Conn!=null){
           mConn=Conn.isClosed();
       }
       if (mConn){
         OpenConnection();
       }

       ResultSet result=ExecuteQuery(mSql);
       if (result.next())
       {
         mResult=result.getInt("MaxID");
       }
       result.close();

       if (mConn)
       {
         CloseConnection();
       }

     }
     catch(Exception e)
     {
       System.out.println(e.toString());
   }
   return (mResult);
 }

  //数据检索
  public ResultSet ExecuteQuery(String SqlString)
  {
    ResultSet result=null;
    try
    {
      result=Stmt.executeQuery(SqlString);
    }
    catch(Exception e)
    {
      System.out.println(e.toString());
    }
    return (result);
  }

  //数据更新(增、删、改)
  public int ExecuteUpdate(String SqlString)
  {
    int result=0;
    try
    {
      result=Stmt.executeUpdate(SqlString);
    }
    catch(Exception e)
    {
      System.out.println(e.toString());
    }
    return (result);
  }

}
