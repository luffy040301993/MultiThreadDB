package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import dao.BookThread;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import model.Book;

public class ReadFromExcelFile {

    //    public List<Book> readBooksFromExcelFile(String excelFilePath) throws IOException {
//        List<Book> listBooks = new ArrayList<Book>();
//        FileInputStream inputStream = new FileInputStream(new File(excelFilePath));
//
//        Workbook workBook = getWorkbook(inputStream, excelFilePath);
//        Sheet firstSheet = workBook.getSheetAt(0);
//        Iterator<Row> rows = firstSheet.iterator();
//
//        while (rows.hasNext()) {
//            Row row = rows.next();
//            Iterator<Cell> cells = row.cellIterator();
//            Book book = new Book();
//
//            while (cells.hasNext()) {
//                Cell cell = cells.next();
//                int columnIndex = cell.getColumnIndex();
//
//                switch (columnIndex) {
//                    case 0:
//                        book.setId((String) getCellValue(cell));
//                        break;
//                    case 1:
//                        book.setLastName((String) getCellValue(cell));
//                        break;
//                    case 2:
//                        book.setFirstName((String) getCellValue(cell));
//                        break;
//                    case 3:
//                        book.setBirthDay((String) getCellValue(cell));
//                        break;
//                    case 4:
//                        book.setClassa((String) getCellValue(cell));
//                        break;
//                    case 5:
//                        book.setType((String) getCellValue(cell));
//                        break;
//                    case 6:
//                        book.setMajors((String) getCellValue(cell));
//                        break;
//
//                }
//            }
//            listBooks.add(book);
//        }
//
//        workBook.close();
//        inputStream.close();
//
//        return listBooks;
//    }

//    public List<Book> readBooksFromExcelFile(String excelFilePath) throws IOException  {
//        List<Book> listBooks = new ArrayList<>();
//        FileInputStream inputStream = new FileInputStream(new File(excelFilePath));
//
//        Workbook workBook = getWorkbook(inputStream, excelFilePath);
//        Sheet firstSheet = workBook.getSheetAt(0);
//        Iterator<Row> rows = firstSheet.iterator();
//
//        while (rows.hasNext()) {
//            Row row = rows.next();
//            Iterator<Cell> cells = row.cellIterator();
//            Book book = new Book();
//
//            while (cells.hasNext()) {
//                Cell cell = cells.next();
//                int columnIndex = cell.getColumnIndex();
//
//                switch (columnIndex) {
//                    case 0:
//                        book.setId((String) getCellValue(cell));
//                        break;
//                    case 1:
//                        book.setLastName((String) getCellValue(cell));
//                        break;
//                    case 2:
//                        book.setFirstName((String) getCellValue(cell));
//                        break;
//                    case 3:
//                        book.setBirthDay((String) getCellValue(cell));
//                        break;
//                    case 4:
//                        book.setClassa((String) getCellValue(cell));
//                        break;
//                    case 5:
//                        book.setType((String) getCellValue(cell));
//                        break;
//                    case 6:
//                        book.setMajors((String) getCellValue(cell));
//                        break;
//
//                }
//            }
//            listBooks.add(book);
//        }
//
//        workBook.close();
//        inputStream.close();
//
//        return listBooks;
//    }

    //  Multil thread
    public BlockingQueue<Book> readBooksFromExcelFile(String excelFilePath) throws IOException  {
        BlockingQueue<Book> listBooks = new LinkedBlockingQueue<>();
        FileInputStream inputStream = new FileInputStream(new File(excelFilePath));

        Workbook workBook = getWorkbook(inputStream, excelFilePath);
        Sheet firstSheet = workBook.getSheetAt(0);
        Iterator<Row> rows = firstSheet.iterator();

        while (rows.hasNext()) {
            Row row = rows.next();
            Iterator<Cell> cells = row.cellIterator();
            Book book = new Book();

            while (cells.hasNext()) {
                Cell cell = cells.next();
                int columnIndex = cell.getColumnIndex();

                switch (columnIndex) {
                    case 0:
                        book.setId((String) getCellValue(cell));
                        break;
                    case 1:
                        book.setLastName((String) getCellValue(cell));
                        break;
                    case 2:
                        book.setFirstName((String) getCellValue(cell));
                        break;
                    case 3:
                        book.setBirthDay((String) getCellValue(cell));
                        break;
                    case 4:
                        book.setClassa((String) getCellValue(cell));
                        break;
                    case 5:
                        book.setType((String) getCellValue(cell));
                        break;
                    case 6:
                        book.setMajors((String) getCellValue(cell));
                        break;

                }
            }
            listBooks.add(book);
        }

        workBook.close();
        inputStream.close();

        return listBooks;
    }

    private Object getCellValue(Cell cell) {
        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue();

            case BOOLEAN:
                return cell.getBooleanCellValue();

            case NUMERIC:
                return cell.getNumericCellValue();
        }

        return null;
    }

    private Workbook getWorkbook(FileInputStream inputStream, String excelFilePath) throws IOException {
        Workbook workbook = null;

        try{
            if (excelFilePath.endsWith("xlsx")) {
                workbook = new XSSFWorkbook(inputStream);
            } else if (excelFilePath.endsWith("xls")) {
                workbook = new HSSFWorkbook(inputStream);
            } else {
                throw new IllegalArgumentException("The specified file is not Excel file");
            }
        }catch(Exception e){
            e.printStackTrace();
        }

        return workbook;
    }

}