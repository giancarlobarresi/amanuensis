/**
 * Amanuensis, a distributed Lucene Index Writer for Infinispan
 *
 * Copyright (c) 2011, Tristan Tarrant
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */

package net.dataforte.infinispan.amanuensis;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import net.dataforte.commons.slf4j.LoggerFactory;
import net.dataforte.infinispan.amanuensis.ops.AddDocumentOperation;
import net.dataforte.infinispan.amanuensis.ops.DeleteDocumentsQueriesOperation;
import net.dataforte.infinispan.amanuensis.ops.DeleteDocumentsTermsOperation;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;

public class AmanuensisIndexWriter {
	private static final Logger log = LoggerFactory.make();

	private AmanuensisManager manager;
	private String directoryId;
	private ThreadLocal<IndexOperations> batchOps;
	private GlobalBatch globalBatch;

	// Locks ======
    private final ReadLock gbInsertLock;  // like a read lock: many threads can insert in the queue at the same time 
    private final WriteLock gbExtractionLock; // like a write lock: just one thread can be active while removing from the queue
    // ===========


	private Directory directory;


	public AmanuensisIndexWriter(AmanuensisManager manager, Directory directory) throws IndexerException {
		this.manager = manager;
		this.directoryId = AmanuensisManager.getUniqueDirectoryIdentifier(directory);
		this.directory = directory;
		this.batchOps = new ThreadLocal<IndexOperations>();
		this.globalBatch = new GlobalBatch();
		
        ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
        gbInsertLock = readWriteLock.readLock();
        gbExtractionLock = readWriteLock.writeLock();
	}

	public String getDirectoryId() {
		return directoryId;
	}

	public Directory getDirectory() {
		return directory;
	}

	// IndexWriter methods

	public boolean isBatching() {
		return batchOps.get() != null;
	}

    /**
     * Put this thread in <I>global</I> batch mode: all operations 
     * will be inserted in a queue shared among all threads in global batching mode,
     *  and sent when the {@link AmanuensisIndexWriter#flushGlobalBatch()} method is
     * invoked. Note that every thread can independently choose if join the global batching mode,
     * or to be in the thread-local batching mode (see {@link AmanuensisIndexWriter#startBatch()}), 
     * or not to be in any type of batching mode.
     * 
     * @see AmanuensisIndexWriter#endBatch()
     * @see AmanuensisIndexWriter#cancelBatch()
     */
    public void startAddingGlobalBatch() {
        if ( isBatching() ) {
            throw new IllegalStateException("This thread [ " + Thread.currentThread().getName() + "] is currently in batching mode so it cannot enter in the global batching mode");
        }

        if (isGlobalBatching()) {
            throw new IllegalStateException("Already in global batching mode");
        } else {
            this.globalBatch.batchEnabled.set(Boolean.TRUE);

            if (log.isDebugEnabled()) {
                log.debug("Thread [" + Thread.currentThread().getName() +"] joining the global batching for index " + directoryId);
            }
        }

    }

    /**
     * Return true if and only if at least one thread is in the global batching mode.
     * @return
     */
    public boolean isGlobalBatching() {
        return globalBatch.batchEnabled.get() != null;
    }

    /**
	 * Put this InfinispanIndexWriter in batch mode: all operations will be
	 * queued and sent when the {@link AmanuensisIndexWriter#endBatch} method is
	 * invoked. Note that the batches are local to the current thread, therefore
	 * each thread may start and end a batch independently from the others.
	 * 
	 * @see AmanuensisIndexWriter#endBatch()
	 * @see AmanuensisIndexWriter#cancelBatch()
	 */
	public void startBatch() {
        if ( isGlobalBatching() ) {
            throw new IllegalStateException("This thread [ " + Thread.currentThread().getName() + "] is currently in the global batching mode so it cannot enter in the thread-specific batching mode");
        }

        if (isBatching()) {
			throw new IllegalStateException("Already in batching mode");
		} else {
			batchOps.set(new IndexOperations(this.directoryId));

			if (log.isDebugEnabled()) {
				log.debug("Batching started for index " + directoryId);
			}
		}
	}

    /**
     * Send all changes in the current batch, started by
     * {@link AmanuensisIndexWriter#startBatch()} to the master node for
     * indexing
     * 
     * @throws IllegalStateException if this thread wasn't in global batching mode
     * 
     * @see AmanuensisIndexWriter#startBatch()
     * @see AmanuensisIndexWriter#cancelBatch()
     */
    public void endAddingGlobalBatch() {
        if (!isGlobalBatching()) {
            throw new IllegalStateException("Not in global batching mode");
        } else {
            this.globalBatch.batchEnabled.remove();
            if (log.isDebugEnabled()) {
                log.debug("Finished this thread [" + Thread.currentThread().getName() + "] join to global batching for index " + directoryId);
            }
        }
    }
    
    /**
     * Send all changes in the current global batch to the master node for
     * indexing. The caller of this method doesn't need to be in global batching mode.
     * 
     * @throws IndexerException
     * 
     * @see AmanuensisIndexWriter#startAddingGlobalBatch()
     * @see AmanuensisIndexWriter#cancelGlobalBatch()
     */
    public void flushGlobalBatch() throws IndexerException {
        IndexOperations tempBatchOps;
        gbExtractionLock.lock();
        try {
            tempBatchOps = globalBatch.indexOperations;
            globalBatch.indexOperations = new IndexOperations(this.directoryId);
        } finally {
            gbExtractionLock.unlock();
        }

        manager.dispatchOperations(tempBatchOps);
        if (log.isTraceEnabled()) {
            log.trace("Global batch flushed for index " + directoryId);
        }

    }
    
    /**
	 * Send all changes in the current batch, started by
	 * {@link AmanuensisIndexWriter#startBatch()} to the master node for
	 * indexing
	 * 
	 * @throws IndexerException
	 * 
	 * @see AmanuensisIndexWriter#startBatch()
	 * @see AmanuensisIndexWriter#cancelBatch()
	 */
	public void endBatch() throws IndexerException {
		if (!isBatching()) {
			throw new IllegalStateException("Not in batching mode");
		} else {
			manager.dispatchOperations(batchOps.get());
			batchOps.remove();
			if (log.isDebugEnabled()) {
				log.debug("Batching finished for index " + directoryId);
			}
		}
	}

    /**
     * Cancels the current batch: all queued changes will be reset and not sent
     * to the master
     * 
     * @see AmanuensisIndexWriter#startBatch()
     * @see AmanuensisIndexWriter#endBatch()
     */
    public void cancelGlobalBatch() {
        synchronized (globalBatch.indexOperations) {
            globalBatch.indexOperations = new IndexOperations(directoryId);
            if (log.isDebugEnabled()) {
                log.debug("Global batching cancelled for index " + directoryId);
            }
        }
    }

	/**
	 * Cancels the current batch: all queued changes will be reset and not sent
	 * to the master
	 * 
	 * @see AmanuensisIndexWriter#startBatch()
	 * @see AmanuensisIndexWriter#endBatch()
	 */
	public void cancelBatch() {
		if (!isBatching()) {
			throw new IllegalStateException("Not in batching mode");
		} else {
			batchOps.remove();
			if (log.isDebugEnabled()) {
				log.debug("Batching cancelled for index " + directoryId);
			}
		}
	}

	/**
	 * Adds a single {@link Document} to the index
	 * 
	 * @param doc
	 * @throws IndexerException
	 */
	public void addDocument(Document doc) throws IndexerException {
		dispatch(new AddDocumentOperation(doc));
	}

	/**
	 * Adds multiple {@link Document} to the index
	 * 
	 * @param docs
	 * @throws IndexerException
	 */
	public void addDocuments(Document... docs) throws IndexerException {
		if (docs.length == 0) {
			return;
		}
		IndexOperation ops[] = new IndexOperation[docs.length];
		for (int i = 0; i < ops.length; i++) {
			ops[i] = new AddDocumentOperation(docs[i]);
		}
		dispatch(ops);
	}

	/**
	 * Deletes all documents from the index which match the given array of
	 * {@link Query}
	 * 
	 * @param queries
	 * @throws IndexerException
	 */
	public void deleteDocuments(Query... queries) throws IndexerException {
		if (queries.length == 0) {
			return;
		}
		IndexOperation ops[] = new IndexOperation[queries.length];
		for (int i = 0; i < queries.length; i++) {
			ops[i] = new DeleteDocumentsQueriesOperation(queries[i]);
		}
		dispatch(ops);
	}

	/**
	 * Deletes all documents from the index which match the given array of
	 * {@link Term}
	 * 
	 * @param queries
	 * @throws IndexerException
	 */
	public void deleteDocuments(Term... terms) throws IndexerException {
		if (terms.length == 0) {
			return;
		}
		IndexOperation ops[] = new IndexOperation[terms.length];
		for (int i = 0; i < terms.length; i++) {
			ops[i] = new DeleteDocumentsTermsOperation(terms[i]);
		}
		dispatch(ops);
	}

	// INTERNAL METHODS
	private void dispatch(IndexOperation... ops) throws IndexerException {
        if (isGlobalBatching()) {
            gbInsertLock.lock();
            try {
                globalBatch.indexOperations.addOperations(ops);
                return;
            } finally {
                gbInsertLock.unlock();
            }
        }
        if (isBatching()) {
            batchOps.get().addOperations(ops);
        } else {
			manager.dispatchOperations(new IndexOperations(this.directoryId, ops));
		}
	}


	private class GlobalBatch {
	    private IndexOperations indexOperations;
	    private ThreadLocal<Object> batchEnabled;
	    
	    GlobalBatch() {
	        this.batchEnabled = new ThreadLocal<Object>();
            this.indexOperations = new IndexOperations(directoryId);
	    }

	}

}