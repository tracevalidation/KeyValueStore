package org.lbee.start;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.lbee.client.Client;
import org.lbee.client.ClientInit;
import org.lbee.instrumentation.clock.ClockException;
import org.lbee.instrumentation.clock.ClockFactory;
import org.lbee.instrumentation.trace.TLATracer;
import org.lbee.store.KeyExistsException;
import org.lbee.store.Store;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

public class Main {
    private static final int NB_CLIENTS = 5;
    
    public static void main(String[] args) throws InterruptedException, IOException, KeyExistsException, ClockException {
        List<Integer> keys = new ArrayList<>();
        List<String> vals = new ArrayList<>();
        List<String> txIds = new ArrayList<>();
        readConfig("conf.ndjson", keys, vals, txIds);
        int maxNbTx = txIds.size();

        TLATracer tracer = TLATracer.getTracer("store.ndjson",
                ClockFactory.getClock(ClockFactory.MEMORY));
        Store store = new Store(maxNbTx, tracer);

        final Collection<Callable<Boolean>> tasks = new HashSet<>();

        // use ClientInit to initialize the store and then ClientDet to run the tests
        // ClientDet is the similar to Client but with less exceptions (doesn't perform add operations)
        final ClientInit ci = new ClientInit(store, keys, vals);
        final ExecutorService poolInit = Executors.newCachedThreadPool();
        tasks.add(ci);
        Collection<Future<Boolean>> future = poolInit.invokeAll(tasks);
        for (Future<Boolean> f : future) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        tasks.remove(ci);

        for (int i = 0; i < NB_CLIENTS; i++) {
            // final Client c = new Client(store, keys, vals);
            // 2 trnasactions, 10 requests per transaction
            final Client c = new Client(store, keys, vals,5,5);
            System.out.printf("Create new client.\n");
            tasks.add(c);
        }

        // Run all tasks concurrently.
        final ExecutorService pool = Executors.newCachedThreadPool();
        future = pool.invokeAll(tasks);
        for (Future<Boolean> f : future) {
            // Boolean result = null;
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

    private static void readConfig(String path, List<Integer> keys, List<String> values, List<String> txIds)
            throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line = reader.readLine();
            if (line == null) {
                throw new IOException("Configuration file must contain one json object. Invalid configuration file.");
            }
            JsonElement jsonLine = JsonParser.parseString(line);
            if (!jsonLine.isJsonObject()) {
                throw new IOException("Configuration file must contain one json object. Invalid configuration file.");
            }
            JsonObject config = jsonLine.getAsJsonObject();

            Gson gson = new Gson();
            Type stringListType = new TypeToken<List<String>>() {}.getType();
            Type integerListType = new TypeToken<List<Integer>>() {}.getType();
            List<Integer> ks = gson.fromJson(config.get("Key").getAsJsonArray(), integerListType);
            for (Integer k : ks) {
                keys.add(k);
            }
            List<String> vs = gson.fromJson(config.get("Val").getAsJsonArray(), stringListType);
            for (String v : vs) {
                values.add(v);
            }
            List<String> txs = gson.fromJson(config.get("TxId").getAsJsonArray(), stringListType);
            for (String t : txs) {
                txIds.add(t);
            }
        } catch (JsonSyntaxException e) {
            System.out.println("Invalid json syntax in configuration file.");
        }
    }
}