package org.lbee.start;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.lbee.client.ClientSimple;
import org.lbee.instrumentation.clock.ClockException;
import org.lbee.instrumentation.clock.ClockFactory;
import org.lbee.instrumentation.trace.TLATracer;
import org.lbee.store.KeyExistsException;
import org.lbee.store.Store;

public class MainSimple {
    public static void main(String[] args) throws InterruptedException, IOException, KeyExistsException, ClockException {
        // Store store = new Store();
        TLATracer tracer = TLATracer.getTracer("store.ndjson",
                ClockFactory.getClock(ClockFactory.MEMORY));
        // Maximum number of transactions (better be consistent with conf file)
        int maxNbTx = 10;

        Store store = new Store(maxNbTx, tracer);

        final Collection<Callable<Boolean>> tasks = new HashSet<>();

        final ClientSimple c1 = new ClientSimple(store);
        final ClientSimple c2 = new ClientSimple(store);
        final ClientSimple c3 = new ClientSimple(store);

        // Run all tasks concurrently.
        final ExecutorService pool = Executors.newCachedThreadPool();
        tasks.add(c1);
        Collection<Future<Boolean>> future = pool.invokeAll(tasks);
        for (Future<Boolean> f : future) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                System.out.println("Error for task: " + e.getMessage());
            }
        }

        tasks.remove(c1);
        tasks.add(c2);
        tasks.add(c3);

        future = pool.invokeAll(tasks);
        for (Future<Boolean> f : future) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        pool.shutdown();
        // pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

        System.out.println(store);
    }

}