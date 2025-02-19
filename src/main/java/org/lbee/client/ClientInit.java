package org.lbee.client;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import org.lbee.store.KeyExistsException;
import org.lbee.store.Store;
import org.lbee.store.Transaction;
import org.lbee.store.TransactionException;

/**
 * Key value store consumer.
 * Initialize the store with a value for each key.
 */
public class ClientInit implements Callable<Boolean> {
    // Store used by client
    private final Store store;
    // potential keys and values
    List<Integer> keys;
    List<String> values;

    public ClientInit(Store store, List<Integer> keys, List<String> values) throws IOException {
        this.store = store;
        this.keys = keys;
        this.values = values;
    }

    @Override
    public Boolean call() throws InterruptedException {
        boolean commitSucceed = false;

        // open a new transaction
        Transaction tx = null;
        try {
            tx = store.open();
        } catch (IOException e) {
            System.out.println("xxx IO problem when opening transaction");
            return false;
        } catch (TransactionException e) {
            System.out.printf("--- No more transaction for client Init.\n");
            return false;
        }
        System.out.printf("-- Open a new transaction %s from client init.\n", tx);

        // Initialise all keys
        Random random = new Random();
        for (Integer key : keys) {
            String val = values.get(random.nextInt(0, values.size()));
            try {
                store.add(tx, key, val);
            } catch (KeyExistsException e) {
                System.out.println("*** Key " + key + " already exists");
                return false;
            } catch (IOException e) {
                System.out.println("Tracing error");
                return false;
            }
            System.out.printf("Added %d-%s.\n", key, val);
        }

        // Try to commit
        try {
            commitSucceed = store.close(tx);
        } catch (IOException e) {
            System.out.println("xxx IO problem when committing");
        }
        return commitSucceed;
    }


}
