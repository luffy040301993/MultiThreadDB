//package timSV;
//
//import model.Book;
//import org.apache.poi.ss.usermodel.Cell;
//import org.apache.poi.ss.usermodel.Row;
//
//import java.util.Iterator;
//
//public class docdaluong extends Thread {
//    Iterator<Row> rows = firstSheet.iterator();
//    while(rows.hasNext())
//
//    {
//        Row row = rows.next();
//        Iterator<Cell> cells = row.cellIterator();
//        Book book = new Book();
//
//        while (cells.hasNext()) {
//            Cell cell = cells.next();
//            int columnIndex = cell.getColumnIndex();
//
//            switch (columnIndex) {
//                case 0:
//                    book.setId((String) getCellValue(cell));
//                    break;
//                case 1:
//                    book.setLastName((String) getCellValue(cell));
//                    break;
//                case 2:
//                    book.setFirstName((String) getCellValue(cell));
//                    break;
//                case 3:
//                    book.setBirthDay((String) getCellValue(cell));
//                    break;
//                case 4:
//                    book.setClassa((String) getCellValue(cell));
//                    break;
//                case 5:
//                    book.setType((String) getCellValue(cell));
//                    break;
//                case 6:
//                    book.setMajors((String) getCellValue(cell));
//                    break;
//
//            }
//        }
//        listBooks.add(book);
//    }
//}
