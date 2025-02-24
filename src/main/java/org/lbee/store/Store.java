package org.lbee.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lbee.instrumentation.trace.TLATracer;
import org.lbee.instrumentation.trace.VirtualField;

public class Store {

    private static final String NO_VALUE = "a value that cannot be";
    // Maximum number of transactions (better be consistent with conf file)
    // private static final long MAX_NB_TX = 10;  
    private int maxNbTx;

    private final Map<Integer, String> store;
    private final Map<Transaction, Map<Integer, String>> snapshots;
    private final List<Transaction> openTransactions;
    private long nbOpenTransactions;
    private long lastTransactionId = 0;

    private final Map<Transaction, Set<Integer>> written;
    private final Map<Transaction, Set<Integer>> missed;

    // Tracing
    private TLATracer tracer;
    private VirtualField traceTx;
    private VirtualField traceWritten;
    private VirtualField traceSnapshot;

    public Store() {
        this.store = new HashMap<>();
        this.snapshots = new HashMap<>();
        this.openTransactions = new ArrayList<>();
        this.nbOpenTransactions = 0;
        this.written = new HashMap<>();
        this.missed = new HashMap<>();
    }

    public Store(int maxNbTx, TLATracer tracer) {
        this();
        this.maxNbTx = maxNbTx;
        this.tracer = tracer;
        this.traceTx = tracer.getVariableTracer("tx");
        this.traceWritten = tracer.getVariableTracer("written");
        this.traceSnapshot = tracer.getVariableTracer("snapshotStore");
    }

    public synchronized Transaction open() throws IOException, TransactionException {
        if (this.nbOpenTransactions >= this.maxNbTx) {
            throw new TransactionException();
        }
        long newTxId = this.lastTransactionId++ % this.maxNbTx;
        Transaction transaction = new Transaction(newTxId);
        this.nbOpenTransactions++;
        openTransactions.add(transaction);
        snapshots.put(transaction, new HashMap<>());
        written.put(transaction, new HashSet<>());
        missed.put(transaction, new HashSet<>());

        System.out.println("Open " + transaction);

        this.traceTx.add(transaction.getId() + "");
        // either the "OpenTx" or the "CloseTx" should be specified to
        // detect wrong commits
        this.tracer.log("OpenTx", new Object[]{transaction.toString()});
        // this.tracer.log("OpenTx");
        // this.tracer.log();

        return transaction;
    }

    public void add(Transaction transaction, Integer key, String value) throws KeyExistsException, IOException {
        System.out.println("Add (" + transaction + "): " + key + " " + value);

        final Map<Integer, String> snapshot = snapshots.get(transaction);
        // if key already exists because of a previous write operation
        // (not cancelled by a remove operation) in the local snapshot
        // or exists in the global store (and has a value, i.e. not removed) then, throw exception
        if ((snapshot.containsKey(key) && !snapshot.get(key).equals(NO_VALUE))
                || (store.containsKey(key) && !store.get(key).equals(NO_VALUE))) {
            throw new KeyExistsException();
        }
        // if key has been affected by an operation in a different transaction, throw exception
        // -> just to be sure that key->NO_VALUE in the store is not because of a remove operation 
        // from another transaction started the same time and already finished (and that the store 
        // of the beginning of the transaction did not contain key->some_value)
        if (missed.get(transaction).contains(key)) {
            throw new KeyExistsException();
        }

        // Change value in snapshot store
        snapshot.put(key, value);
        written.get(transaction).add(key);

        // trace
        synchronized (this) {
            this.traceWritten.getField(transaction.getId() + "").add(key);
            this.traceSnapshot.getField(transaction.getId() + "").setKey(key, value);
            this.tracer.log("Add", new Object[]{transaction.toString(), key.intValue(), value});
            // this.tracer.log();
        }
    }

    public void update(Transaction transaction, Integer key, String value)
            throws KeyNotExistsException, ValueExistsException, IOException {
        System.out.println("Update (" + transaction + "): " + key + " " + value);

        final Map<Integer, String> snapshot = snapshots.get(transaction);
        if (!snapshot.containsKey(key)) { // key doesn't exist in the snapshot
            if (!store.containsKey(key)) { // and it doesn't exist in the store
                throw new KeyNotExistsException();
            } else { // key exists in the store 
                if (store.get(key).equals(NO_VALUE)) { // but it is a NO_VALUE (because of a remove operation)
                    throw new KeyNotExistsException();
                } else {
                    if (store.get(key).equals(value)) { // key exists in the store but already has the value
                        throw new ValueExistsException();
                    }
                }
            }
        } else { // key exists in the snapshot 
            if (snapshot.get(key).equals(NO_VALUE)) { // but it is a NO_VALUE (because of a remove operation)
                throw new KeyNotExistsException();
            } else {
                if (snapshot.get(key).equals(value)) { // key exists in the snapshot but already has the value
                    throw new ValueExistsException();
                }
            }
        }
        // if key has been affected by an operation in a different transaction, throw exception
        // -> just to be sure that key->some_value in the store is not because of an operation 
        // from another transaction started the same time and already finished (and that the store 
        // of the beginning of the transaction did not contain key->value or key->NO_VALUE)
        if (missed.get(transaction).contains(key)) {
            throw new KeyNotExistsException();
        }

        // Change value in snapshot store
        snapshot.put(key, value);
        written.get(transaction).add(key);

        // trace
        synchronized (this) {
            this.traceWritten.getField(transaction.getId() + "").add(key);
            this.traceSnapshot.getField(transaction.getId() + "").setKey(key, value);
            this.tracer.log("Update", new Object[]{transaction.toString(), key.intValue(), value});
            // this.tracer.log();
        }
    }

    public void remove(Transaction transaction, Integer key) throws KeyNotExistsException, IOException {
        System.out.println("Remove (" + transaction + "): " + key);

        final Map<Integer, String> snapshot = snapshots.get(transaction);
        if (!snapshot.containsKey(key)) { // key doesn't exist in the snapshot
            if (!store.containsKey(key)) { // and it doesn't exist in the store
                throw new KeyNotExistsException();
            } else { // key exists in the store 
                if (store.get(key).equals(NO_VALUE)) { // but it is a NO_VALUE (because of a remove operation)
                    throw new KeyNotExistsException();
                }
            }
        } else { // key exists in the snapshot 
            if (snapshot.get(key).equals(NO_VALUE)) { // but it is a NO_VALUE (because of a remove operation)
                throw new KeyNotExistsException();
            }
        }

        // if key has been affected by an operation in a different transaction, throw exception
        // -> just to be sure that key->some_value in the store is not because of an add operation 
        // from another transaction started the same time and already finished (and that the store 
        // of the beginning of the transaction did not contain the key or contained key->NO_VALUE)
        if (missed.get(transaction).contains(key)) {
            throw new KeyNotExistsException();
        }
        
        // Change value to NO_VALUE in snapshot in order
        // to remove the key at commit time
        snapshot.put(key, NO_VALUE);
        written.get(transaction).add(key);

        // trace
        synchronized (this) {
            this.traceWritten.getField(transaction.getId() + "").add(key);
            this.tracer.log("Remove", new Object[]{transaction.toString(), key.intValue()});
            // this.tracer.log();
        }
    }

    public String read(Integer key) {
        System.out.println("Read " + key);

        return store.get(key);
    }

    public synchronized boolean close(Transaction transaction) throws IOException {
        // compute the intersection between written and missed
        Set<Integer> intersection = new HashSet<>(written.get(transaction));
        // if we forget to make a defensive copy, the intersection computation
        // modifies the original set and the rollback will not work
        // Set<String> intersection = written.get(transaction);
        intersection.retainAll(missed.get(transaction));
        // System.out.println("Close: ("+transaction+"): written:
        // "+written.get(transaction)+", missed: "+missed.get(transaction)+",
        // intersection: "+intersection);
        // check if the the intersection of written and missed is empty; rollback if not
        if (!intersection.isEmpty()) {
            // remove the transaction from the pool, snapshots, written and missed
            openTransactions.remove(transaction);
            this.nbOpenTransactions--;
            snapshots.remove(transaction);
            written.remove(transaction);
            missed.remove(transaction);
            System.out.println("Rollback (" + transaction + "): " + intersection);

            // trace
            this.traceTx.remove(transaction.getId() + "");
            this.traceWritten.getField(transaction.getId() + "").clear();
            this.tracer.log("RollbackTx", new Object[]{transaction.toString()});
            // this.tracer.log();
            return false;
        }
        // add the operation from snapshot to store
        final Map<Integer, String> snapshot = snapshots.get(transaction);
        for (Integer key : snapshot.keySet()) {
            if (snapshot.get(key).equals(NO_VALUE)) {
                store.remove(key);
            } else {
                store.put(key, snapshot.get(key));
            }
        }
        // Add written log as missed for other open transactions
        for (Transaction tx : openTransactions) {
            missed.get(tx).addAll(written.get(transaction));
        }
        // remove the transaction from the pool, snapshots, written and missed
        openTransactions.remove(transaction);
        snapshots.remove(transaction);
        this.nbOpenTransactions--;
        written.remove(transaction);
        missed.remove(transaction);
        System.out.println("Commit (" + transaction + "): " + snapshot);

        // trace
        this.traceTx.remove(transaction.getId() + "");
        this.traceWritten.getField(transaction.getId() + "").clear();
        this.tracer.log("CloseTx", new Object[]{transaction.toString()});
        // this.tracer.log();
        return true;
    }

    @Override
    public String toString() {
        return store.toString() + " - " + openTransactions.toString();
    }
}
