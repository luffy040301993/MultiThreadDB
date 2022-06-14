package timSV;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import model.Book;
import util.MySQLConnectionUtils;

public class timSVtheoID {

    //    sao phai dat static cho conn
    private static Connection conn;
    static Statement stmt;
    //coding convention

    static {
        System.out.println("connect");
        try {
            conn = MySQLConnectionUtils.connect();
            stmt = conn.createStatement();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public timSVtheoID(){
        System.out.println("contructor timSVtheoID");
    }

    public static void timsinhvien(String ID) throws SQLException {
        String sql = "Select * From DSSV WHERE ID='" + ID + "'";
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        System.out.println(rs.getString("ID") + "\t" +
                rs.getString("LastName") + "\t" +
                rs.getString("FirstName") + "\t" +
                rs.getString("Birthday") + "\t" +
                rs.getString("Classa") + "\t" +
                rs.getString("Typea") + "\t" +
                rs.getString("Majors"));
        MySQLConnectionUtils.disconnect(conn);
        System.out.println("disconnect");
    }

    public  void themsinhvien(Book book) throws SQLException {
        String sql = "INSERT INTO DSSV (ID,LastName,FirstName,Birthday,Classa,Typea,Majors) VALUES ('"
                + book.getId() + "','" + book.getLastName() + "','" + book.getFirstName() + "','" + book.getBirthDay() + "','" +
                book.getClassa() + "','" + book.getType() + "','" + book.getMajors() + "')";

        int result = 0;
        result =  stmt.executeUpdate(sql);
        if(result != 0) {
            System.out.println("insert done!");
        }
        MySQLConnectionUtils.disconnect(conn);
        System.out.println("disconnect");
    }

    public static void updatesinhvien(Book book) throws SQLException {
        String sql = "UPDATE DSSV SET LastName='" + book.getLastName() + "',FirstName='" + book.getFirstName() +
                "',Birthday='" + book.getBirthDay() + "',Classa='" + book.getClassa() + "',Typea='" + book.getType() +
                "',Majors='" + book.getMajors() + "'" +
                "WHERE ID='" + book.getId() + "'";
        stmt.executeUpdate(sql);
        String sql1 = "Select * From DSSV WHERE ID='" + book.getId() + "'";
        ResultSet rs = stmt.executeQuery(sql1);
        rs.next();
        System.out.println(rs.getString("ID") + "\t" +
                rs.getString("LastName") + "\t" +
                rs.getString("FirstName") + "\t" +
                rs.getString("Birthday") + "\t" +
                rs.getString("Classa") + "\t" +
                rs.getString("Typea") + "\t" +
                rs.getString("Majors"));
        MySQLConnectionUtils.disconnect(conn);
        System.out.println("disconnect");
    }

    public static void xoasinhvien(String IDxoa) throws SQLException {
        String sql = "DELETE FROM DSSV WHERE ID= '" + IDxoa + "'";
        int result = 0;
        result =  stmt.executeUpdate(sql);
        if(result != 0)
            System.out.println("delete done!");
        MySQLConnectionUtils.disconnect(conn);
        System.out.println("disconnect");
    }


}
