package dao;

import model.Book;

public class BookThread implements Runnable {
    Book book;

    public BookThread(Book book){
        this.book = book;
    }

    @Override
    public void run() {
        BookDAO bookDAO = new BookDAO();
        Book book = bookDAO.insertBook(this.book);
    }
}
