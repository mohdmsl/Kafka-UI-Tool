package org.wl.controller;

import org.wl.models.DBConnection;

import java.sql.*;

public class Database {

    /*public static void main(String[] args) {
        Connection conn = DBConnection.getInstance();
        try {
            PreparedStatement stmt = conn.prepareStatement("select * from TEST");
            ResultSet rs = stmt.executeQuery();
            if (rs.next()){
                System.out.println(rs.getString(1));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }*/

    public void createTable(String query){

        Connection conn = DBConnection.getInstance();

    }

    public  ResultSet runQuery(String query){
        ResultSet rs = null;
        Connection conn = DBConnection.getInstance();
        try {
            PreparedStatement stmt = conn.prepareStatement(query);
             rs = stmt.executeQuery();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return rs;
    }

    public int insert(String query){
        int rs = 0;
        Connection conn = DBConnection.getInstance();
        try {
            PreparedStatement stmt = conn.prepareStatement(query);
            rs = stmt.executeUpdate();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return rs;
    }

    public int delete(String query){
        int rs = 0;
        Connection conn = DBConnection.getInstance();
        try {
            Statement stmt = conn.createStatement();
            rs = stmt.executeUpdate(query);

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return rs;
    }
}
