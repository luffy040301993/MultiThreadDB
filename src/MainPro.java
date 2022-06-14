import dao.BookDAO;
import dao.BookThread;
import model.Book;
import timSV.timSVtheoID;
import util.MySQLConnectionUtils;
import util.ReadFromExcelFile;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

public class MainPro {

//    public static void main(String[] args) throws SQLException {
//        ExecutorService executorService = Executors.newFixedThreadPool(4);
////            Future<String> future = executorService.submit();
////            // some operations
////            String result = future.get();
//        int thaotac;
//        System.out.println("Hay nhap so tuong ung thao tac can thuc hien:\n" +
//                "doc file excel ghi vao DB -> 1\n" +
//                "in ra toan bo sinh vien trong DB -> 2\n" +
//                "tim thong tin sinh vien theo ma ID -> 3\n" +
//                "them sinh vien vao DB -> 4\n" +
//                "update thong tin sinh vien -> 5\n" +
//                "xoa sinh vien theo ma sinh vien -> 6\n");
//        Scanner scanner = new Scanner(System.in);
//        thaotac = scanner.nextInt();
//        switch (thaotac) {
//            case 1:
//                try {
//                    String excelFilePath = "DSSV.xlsx";
//                    List<Book> listBooks = new ReadFromExcelFile().readBooksFromExcelFile(excelFilePath);
//                    BookDAO bookDAO = new BookDAO();
//                    Long start = System.currentTimeMillis();
//                   bookDAO.insertListBooks(listBooks);
//                    Long end = System.currentTimeMillis();
//                    System.out.println("time total : "+(end - start));
//
//                        //            Future<String> future = executorService.submit();
////            // some operations
////            String result = future.get();
//
//                } catch (Exception e) {
//                    System.out.println("Error: " + e.getMessage());
//                }
//                break;
//            case 2:
//                Connection conn = MySQLConnectionUtils.connect();
//                Statement stmt = conn.createStatement();
//                String sql = "SELECT * " +
//                        "FROM DSSV";
//                ResultSet rs = stmt.executeQuery(sql);
//                while (rs.next()) {
//                    System.out.println(rs.getString("ID") + "\t" +
//                            rs.getString("LastName") + "\t" +
//                            rs.getString("FirstName") + "\t" +
//                            rs.getString("Birthday") + "\t" +
//                            rs.getString("Classa") + "\t" +
//                            rs.getString("Typea") + "\t" +
//                            rs.getString("Majors"));
//                }
//                MySQLConnectionUtils.disconnect(conn);
//                System.out.println("disconnect");
//                break;
//            case 3:
//                try {
//                    String ID = "1251132520";
//                    timSVtheoID.timsinhvien(ID);
//
//                } catch (Exception e) {
//                    System.out.println("Error: " + e.getMessage());
//                }
//                break;
//            case 4:
//                try {
//                    Book book = new Book("9992", "NGUYENVAN", "THANG", "4/3/1993", "12A2", "TRUNG BINH", "CO DIEN TU");
//                    timSVtheoID db = new timSVtheoID();
//                    db.themsinhvien(book);
//                    Book book2 = new Book("9993", "NGUYENVAN", "THANG", "4/3/1993", "12A2", "TRUNG BINH", "CO DIEN TU");
//                    db.themsinhvien(book2);
//                } catch (Exception e) {
//                    System.out.println("Error: " + e.getMessage());
//                }
//                break;
//            case 5:
//                try {
//                    Book book = new Book("9999", "NGUYENVAN", "THAO", "4/3/1993", "12A2", "TRUNG BINH", "CO DIEN TU");
//                    timSVtheoID.updatesinhvien(book);
//
//                } catch (Exception e) {
//                    System.out.println("Error: " + e.getMessage());
//                }
//                break;
//            case 6:
//                try {
//                    timSVtheoID.xoasinhvien("9999");
//
//                } catch (Exception e) {
//                    System.out.println("Error: " + e.getMessage());
//                }
//                break;
//        }
//    }

    //  By thaonguyen
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        BlockingQueue<Book> bookQueue = readFileExcel();
        insertBooksToDB(bookQueue);
    }

//    public static List<Book> readFileExcel() {
//        String excelFilePath = "DSSV.xlsx";
//        List<Book> listBooks = null;
//        try {
//            listBooks = new ReadFromExcelFile().readBooksFromExcelFile(excelFilePath);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return listBooks;
//    }

    // Multi thread
    public static BlockingQueue<Book> readFileExcel() {
        String excelFilePath = "DSSV.xlsx";
        BlockingQueue<Book> listBooks = null;
        try {
            listBooks = new ReadFromExcelFile().readBooksFromExcelFile(excelFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return listBooks;
    }

    public static void insertBooksToDB(BlockingQueue<Book> bookQueue) throws ExecutionException, InterruptedException {
        List<Future> futures = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        Long start = System.currentTimeMillis();
        for (Book book : bookQueue) {
            BookThread bookThread = new BookThread(book);
            futures.add(executor.submit(bookThread));
        }

        for (Future future : futures) {
            future.get();
        }
        executor.shutdown();
        System.out.println("save done!");
        Long end = System.currentTimeMillis();
        System.out.println("time total : "+(end-start));
    }
}