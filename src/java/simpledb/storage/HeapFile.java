package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    File f;
    TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageSize = BufferPool.getPageSize();
        int pageNo = pid.getPageNumber();
        long offset = pageNo * pageSize;
        byte[] data = new byte[pageSize];
        RandomAccessFile rfile = null;
        try {
            rfile = new RandomAccessFile(f, "r");
            rfile.seek(offset);
            int readSize = rfile.read(data);
            if (readSize != pageSize) {
                throw new IllegalArgumentException("HeapFile : readPage Fail for Wrong Read Size");
            }
            HeapPageId pgId = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            HeapPage pg = new HeapPage(pgId, data);
            return pg;
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("File Not Found in HeapFile : readPage");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                rfile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (f.length() / BufferPool.getPageSize());
    }

    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to add.  This tuple should be updated to reflect that
     *          it is now stored in this file.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> Pages= new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            PageId pid = new HeapPageId(this.getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() == 0)
                continue;
            else {
                page.insertTuple(t);
                Pages.add(page);
                return Pages;
            }
        }
        //  If no such pages exist in the HeapFile,
        //  create a new page and append it to the physical file on disk
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(this.f, true)); // write to the end
        byte[] newPage = HeapPage.createEmptyPageData();
        bos.write(newPage);
        bos.close(); // When not already closed, the close method of FilterOutputStream calls its flush method
        HeapPageId pid = new HeapPageId(this.getId(), numPages() - 1);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.insertTuple(t);
        Pages.add(page);
        return Pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> Pages = new ArrayList<>();
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        if (page == null)
            throw new DbException("Invalid tuple to delete: no corresponding page");
        page.deleteTuple(t);
        Pages.add(page);
        return Pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

    private class HeapFileIterator implements DbFileIterator {

        final HeapFile f;
        final TransactionId tid;
        Iterator<Tuple> it = null;
        int pageNo;
        int numPages;

        public HeapFileIterator(HeapFile f, TransactionId tid) {
            this.f = f;
            this.tid = tid;
            numPages = f.numPages();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageNo = 0;
            it = getTupleIterator(pageNo);
        }

        private Iterator<Tuple> getTupleIterator(int pageNo) throws DbException, TransactionAbortedException {
            if (pageNo >= 0 && pageNo < numPages) {
                HeapPageId pgId = new HeapPageId(f.getId(), pageNo);
                HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, pgId, Permissions.READ_ONLY);
                if (pg == null)
                    throw new DbException("HeapFile : Get Iterator Fail For PageNumber" + pageNo);
                return pg.iterator();
            }
            else
                throw new DbException("HeapFile : Get Iterator Fail For PageNumber" + pageNo + "For out of Bound");
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (it == null)
                return false;
            if (pageNo >= numPages || pageNo == numPages - 1 && !it.hasNext()) // if end of page && end of tuple
                return false;
            if (pageNo < numPages - 1 && !it.hasNext()) {
                pageNo++;
                it = getTupleIterator(pageNo);
                return it.hasNext();
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null || !it.hasNext())
                throw new NoSuchElementException();
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            it = null;
        }
    }
}

