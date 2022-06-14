package dao;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import model.Book;
import util.MySQLConnectionUtils;

public class BookDAO {

    private static Connection conn;
    private PreparedStatement ps;

    public Connection getConnection() throws SQLException {
        if(conn == null){
            return conn = MySQLConnectionUtils.connect();
        }
        return conn;
    }

    public Book insertBook(Book book) {
        try {
            // Sét tự động commit false, để chủ động điều khiển

            Connection con = getConnection();

            con.setAutoCommit(false);

            String sql = "INSERT INTO DSSV1(ID, LastName, FirstName, BirthDay, Classa, Typea, Majors) VALUES (?, ?, ?, ?, ?, ?, ?)";
            ps = con.prepareStatement(sql);

            ps.setString(1, book.getId());
            ps.setString(2, book.getLastName());
            ps.setString(3, book.getFirstName());
            ps.setString(4, book.getBirthDay());
            ps.setString(5, book.getClassa());
            ps.setString(6, book.getType());
            ps.setString(7, book.getMajors());
            ps.addBatch();

            ps.executeBatch();
            con.commit();
        } catch (Exception e) {
            e.printStackTrace();
            MySQLConnectionUtils.rollbackQuietly(conn);
        } finally {
            try {
                if (ps != null)
                    ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            MySQLConnectionUtils.disconnect(conn);
        }
        return book;
    }

    public void insertListBooks(List<Book> listBooks) throws ExecutionException, InterruptedException {

        try {
            conn = MySQLConnectionUtils.connect();
            // Sét tự động commit false, để chủ động điều khiển
            conn.setAutoCommit(false);

            String sql = "INSERT INTO DSSV1(ID, LastName, FirstName, BirthDay, Classa, Typea, Majors) VALUES (?, ?, ?, ?, ?, ?, ?)";
            ps = conn.prepareStatement(sql);

            for (Book book : listBooks) {
                ps.setString(1, book.getId());
                ps.setString(2, book.getLastName());
                ps.setString(3, book.getFirstName());
                ps.setString(4, book.getBirthDay());
                ps.setString(5, book.getClassa());
                ps.setString(6, book.getType());
                ps.setString(7, book.getMajors());
                ps.addBatch();
            }

            ps.executeBatch();

            // Gọi commit() để commit giao dịch với DB
            conn.commit();

            System.out.println("Record is inserted into DSSV table!");

        } catch (Exception e) {

            e.printStackTrace();
            MySQLConnectionUtils.rollbackQuietly(conn);

        } finally {

            try {
                if (ps != null)
                    ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            MySQLConnectionUtils.disconnect(conn);
        }
    }

}