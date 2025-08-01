/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.lang.reflect.Field;
import java.nio.ReadOnlyBufferException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import lombok.Cleanup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.client.PulsarMockLedgerHandle;
import org.apache.bookkeeper.client.PulsarMockReadHandleInterceptor;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.BoundedScheduledExecutorService;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFencedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerNotFoundException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryMXBean;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionBound;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.VoidCallback;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.impl.cache.EntryCache;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class ManagedLedgerTest extends MockedBookKeeperTestCase {
    private static final Charset Encoding = StandardCharsets.UTF_8;

    @DataProvider(name = "checkOwnershipFlag")
    public Object[][] checkOwnershipFlagProvider() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    private void makeAddEntryTimeout(ManagedLedgerImpl ml, AtomicBoolean addEntryFinished) throws Exception {
        LedgerHandle currentLedger = ml.currentLedger;
        final LedgerHandle spyLedgerHandle = spy(currentLedger);
        doAnswer(invocation -> {
            ByteBuf bs = (ByteBuf) invocation.getArguments()[0];
            AddCallback addCallback = (AddCallback) invocation.getArguments()[1];
            Object originalContext = invocation.getArguments()[2];
            currentLedger.asyncAddEntry(bs, (rc, lh, entryId, ctx) -> {
                addEntryFinished.set(true);
                addCallback.addComplete(BKException.Code.TimeoutException, spyLedgerHandle,  -1, ctx);
            }, originalContext);
            return null;
        }).when(spyLedgerHandle).asyncAddEntry(any(ByteBuf.class), any(AddCallback.class), any());
        ml.currentLedger = spyLedgerHandle;
    }

    @Data
    private static class DeleteLedgerInfo{
        volatile boolean hasCalled;
        volatile CompletableFuture<Void> future = new CompletableFuture<>();
    }

    private DeleteLedgerInfo makeDelayIfDoLedgerDelete(LedgerHandle ledger, final AtomicBoolean signal,
                                                              BookKeeper spyBookKeeper) {
        DeleteLedgerInfo deleteLedgerInfo = new DeleteLedgerInfo();
        doAnswer(invocation -> {
            long ledgerId = (long) invocation.getArguments()[0];
            AsyncCallback.DeleteCallback originalCb = (AsyncCallback.DeleteCallback) invocation.getArguments()[1];
            AsyncCallback.DeleteCallback cb = (rc, ctx) -> {
                if (deleteLedgerInfo.hasCalled) {
                    deleteLedgerInfo.future.complete(null);
                }
                originalCb.deleteComplete(rc, ctx);
            };
            Object ctx = invocation.getArguments()[2];
            if (ledgerId != ledger.getId()){
                bkc.asyncDeleteLedger(ledgerId, originalCb, ctx);
            } else {
                deleteLedgerInfo.hasCalled = true;
                cachedExecutor.submit(() -> {
                    Awaitility.await().atMost(Duration.ofSeconds(60)).until(signal::get);
                    bkc.asyncDeleteLedger(ledgerId, cb, ctx);
                });
            }
            return null;
        }).when(spyBookKeeper).asyncDeleteLedger(any(long.class), any(AsyncCallback.DeleteCallback.class), any());
        return deleteLedgerInfo;
    }

    /***
     * This test simulates the following problems that can occur when ZK connections are unstable:
     *  - add entry timeout
     *  - write ZK fail when update ledger info of ML
     * and verifies that ledger info of ML is still correct when the above problems occur.
     */
    @Test
    public void testLedgerInfoMetaCorrectIfAddEntryTimeOut() throws Exception {
        String mlName = "testLedgerInfoMetaCorrectIfAddEntryTimeOut";
        BookKeeper spyBookKeeper = spy(bkc);
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, spyBookKeeper);
        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open(mlName);

        // Make add entry timeout(The data write was actually successful).
        AtomicBoolean addEntryFinished = new AtomicBoolean(false);
        makeAddEntryTimeout(ml, addEntryFinished);

        // Make the update operation of ledger info failure when switch ledger.
        metadataStore.failConditional(new MetadataStoreException.BadVersionException(""), (opType, path) -> {
            if (opType == FaultInjectionMetadataStore.OperationType.PUT && addEntryFinished.get()
                    && "/managed-ledgers/testLedgerInfoMetaCorrectIfAddEntryTimeOut".equals(path)) {
                return true;
            }
            return false;
        });

        // Make delete ledger is delayed if delete is called.
        AtomicBoolean deleteLedgerDelaySignal = new AtomicBoolean(false);
        DeleteLedgerInfo deleteLedgerInfo =
                makeDelayIfDoLedgerDelete(ml.currentLedger, deleteLedgerDelaySignal, spyBookKeeper);

        // Add one entry.
        // - it will fail and trigger ledger switch(we mocked the error).
        // - ledger switch will also fail(we mocked the error).
        try {
            ml.addEntry("1".getBytes(Charset.defaultCharset()));
            fail("Expected the operation of add entry will fail by timeout or ledger fenced.");
        } catch (Exception e){
            // expected ex.
        }

        // Reopen ML.
        try {
            ml.close();
            fail("Expected the operation of ml close will fail by fenced state.");
        } catch (Exception e){
            // expected ex.
        }
        ManagedLedgerImpl mlReopened = (ManagedLedgerImpl) factory.open(mlName);
        deleteLedgerDelaySignal.set(true);
        if (deleteLedgerInfo.hasCalled){
            deleteLedgerInfo.future.join();
        }
        mlReopened.close();

        // verify: all ledgers in ledger info is worked.
        for (long ledgerId : mlReopened.getLedgersInfo().keySet()){
            LedgerHandle lh = bkc.openLedger(ledgerId, ml.digestType, ml.getConfig().getPassword());
            lh.close();
        }
    }

    @Test
    public void managedLedgerApi() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursor cursor = ledger.openCursor("c1");

        for (int i = 0; i < 100; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }

        // Reads all the entries in batches of 20
        while (cursor.hasMoreEntries()) {

            List<Entry> entries = cursor.readEntries(20);
            log.debug("Read {} entries", entries.size());

            // Acknowledge only on last entry
            Entry lastEntry = entries.get(entries.size() - 1);
            cursor.markDelete(lastEntry.getPosition());

            for (Entry entry : entries) {
                log.info("Read entry. Position={} Content='{}'", entry.getPosition(), new String(entry.getData()));
                entry.release();
            }

            log.info("-----------------------");
        }

        log.info("Finished reading entries");

        ledger.close();
    }

    @Test(timeOut = 20000)
    public void simple() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        assertEquals(ledger.getNumberOfEntries(), 0);
        assertEquals(ledger.getNumberOfActiveEntries(), 0);
        assertEquals(ledger.getTotalSize(), 0);

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        assertEquals(ledger.getNumberOfEntries(), 1);
        assertEquals(ledger.getNumberOfActiveEntries(), 0);
        assertEquals(ledger.getTotalSize(), "dummy-entry-1".getBytes(Encoding).length);

        ManagedCursor cursor = ledger.openCursor("c1");

        assertFalse(cursor.hasMoreEntries());
        assertEquals(cursor.getNumberOfEntries(), 0);
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 0);
        assertEquals(cursor.readEntries(100), new ArrayList<Entry>());

        ledger.addEntry("dummy-entry-2".getBytes(Encoding));

        assertTrue(cursor.hasMoreEntries());
        assertEquals(cursor.getNumberOfEntries(), 1);
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 1);
        assertEquals(ledger.getNumberOfActiveEntries(), 1);

        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 1);
        entries.forEach(Entry::release);

        entries = cursor.readEntries(100);
        assertEquals(entries.size(), 0);

        ledger.close();
        factory.shutdown();
    }

    @Test(timeOut = 20000)
    public void closeAndReopen() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-2".getBytes(Encoding));

        ledger.close();

        log.info("Closing ledger and reopening");

        // / Reopen the same managed-ledger
        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_test_ledger");

        cursor = ledger.openCursor("c1");

        assertEquals(ledger.getNumberOfEntries(), 2);
        assertEquals(ledger.getTotalSize(), "dummy-entry-1".getBytes(Encoding).length * 2);

        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 1);
        entries.forEach(Entry::release);

        ledger.close();
    }

    @Test(timeOut = 20000)
    public void acknowledge1() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));

        assertTrue(cursor.hasMoreEntries());

        List<Entry> entries = cursor.readEntries(2);
        assertEquals(entries.size(), 2);

        assertEquals(cursor.getNumberOfEntries(), 0);
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 2);
        assertFalse(cursor.hasMoreEntries());

        assertEquals(ledger.getNumberOfEntries(), 2);
        assertEquals(ledger.getNumberOfActiveEntries(), 2);
        cursor.markDelete(entries.get(0).getPosition());
        entries.forEach(Entry::release);

        assertEquals(cursor.getNumberOfEntries(), 0);
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 1);
        assertFalse(cursor.hasMoreEntries());
        assertEquals(ledger.getNumberOfActiveEntries(), 1);

        ledger.close();

        // / Reopen the same managed-ledger

        ledger = factory.open("my_test_ledger");
        cursor = ledger.openCursor("c1");

        assertEquals(ledger.getNumberOfEntries(), 2);
        assertEquals(ledger.getTotalSize(), "dummy-entry-1".getBytes(Encoding).length * 2);

        assertEquals(cursor.getNumberOfEntries(), 1);
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 1);
        assertTrue(cursor.hasMoreEntries());

        entries = cursor.readEntries(100);
        assertEquals(entries.size(), 1);
        entries.forEach(Entry::release);

        ledger.close();
    }

    @Test
    public void shouldKeepEntriesInCacheByEarliestReadPosition() throws ManagedLedgerException, InterruptedException {
        // This test case reproduces issue #16054

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        factory.updateCacheEvictionTimeThreshold(TimeUnit.MILLISECONDS
                .toNanos(30000));

        // GIVEN an opened ledger with 10 opened cursors

        ManagedLedger ledger = factory.open("test_ledger_for_shouldKeepEntriesInCacheByEarliestReadPosition",
                config);
        List<ManagedCursor> cursors = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ManagedCursor cursor = ledger.openCursor("c" + i);
            cursors.add(cursor);
        }

        ManagedLedgerFactoryMXBean cacheStats = factory.getCacheStats();
        int insertedEntriesCountBefore = (int) cacheStats.getCacheInsertedEntriesCount();

        // AND 100 added entries

        for (int i = 0; i < 100; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }

        int insertedEntriesCount =
                (int) cacheStats.getCacheInsertedEntriesCount() - insertedEntriesCountBefore;
        // EXPECT that 100 entries should have been inserted to the cache
        assertEquals(insertedEntriesCount, 100);

        int evictedEntriesCountBefore = (int) cacheStats.getCacheEvictedEntriesCount();

        // WHEN entries are read for the cursors so that the farthest cursor has most entries read
        for (int i = 0; i < 10; i++) {
            ManagedCursor cursor = cursors.get(i);
            // read entries farther of the  earliest cursor
            List<Entry> entries = cursor.readEntries(20 - i);
            // mark delete the least for the earliest cursor
            cursor.markDelete(entries.get(i).getPosition());
            entries.forEach(Entry::release);
        }

        // THEN it is expected that the cache evicts entries to the earliest read position
        Thread.sleep(2 * factory.getConfig().getCacheEvictionIntervalMs());
        int evictedEntriesCount =
                (int) cacheStats.getCacheEvictedEntriesCount() - evictedEntriesCountBefore;
        assertEquals(evictedEntriesCount, 11,
                "It is expected that the cache evicts entries to the earliest read position");

        ledger.close();
    }

    @Test
    public void shouldKeepEntriesInCacheByEarliestMarkDeletePosition()
            throws ManagedLedgerException, InterruptedException {
        // This test case reproduces issue #16054

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setCacheEvictionByMarkDeletedPosition(true);
        factory.updateCacheEvictionTimeThreshold(TimeUnit.MILLISECONDS
                .toNanos(30000));

        // GIVEN an opened ledger with 10 opened cursors

        ManagedLedger ledger = factory.open("test_ledger_for_shouldKeepEntriesInCacheByEarliestMarkDeletePosition",
                config);
        List<ManagedCursor> cursors = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ManagedCursor cursor = ledger.openCursor("c" + i);
            cursors.add(cursor);
        }

        ManagedLedgerFactoryMXBean cacheStats = factory.getCacheStats();
        int insertedEntriesCountBefore = (int) cacheStats.getCacheInsertedEntriesCount();

        // AND 100 added entries

        for (int i = 0; i < 100; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }

        int insertedEntriesCount =
                (int) cacheStats.getCacheInsertedEntriesCount() - insertedEntriesCountBefore;
        // EXPECT that 100 entries should have been inserted to the cache
        assertEquals(insertedEntriesCount, 100);

        int evictedEntriesCountBefore = (int) cacheStats.getCacheEvictedEntriesCount();

        // WHEN entries are read for the cursors so that the farthest cursor has most entries read
        Position lastMarkDeletePos = null;
        for (int i = 0; i < 10; i++) {
            ManagedCursor cursor = cursors.get(i);
            // read 50 (+ index) entries for each cursor
            List<Entry> entries = cursor.readEntries(50 + (5 * i));
            // mark delete the most for the earliest cursor
            lastMarkDeletePos = entries.get(20 - i).getPosition();
            cursor.markDelete(lastMarkDeletePos);
            entries.forEach(Entry::release);
        }

        Thread.sleep(1000 + 2 * factory.getConfig().getCacheEvictionIntervalMs());

        ManagedCursorContainer activeCursors = (ManagedCursorContainer) ledger.getActiveCursors();
        assertEquals(activeCursors.getSlowestReaderPosition(), lastMarkDeletePos);

        // THEN it is expected that the cache evicts entries to the earliest read position
        int evictedEntriesCount =
                (int) cacheStats.getCacheEvictedEntriesCount() - evictedEntriesCountBefore;
        assertEquals(evictedEntriesCount, 11,
                "It is expected that the cache evicts entries to the earliest read position");

        ledger.close();
    }

    @Test(timeOut = 20000)
    public void asyncAPI() throws Throwable {
        final CountDownLatch counter = new CountDownLatch(1);

        factory.asyncOpen("my_test_ledger", initManagedLedgerConfig(new ManagedLedgerConfig()),
                new OpenLedgerCallback() {
            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                ledger.asyncOpenCursor("test-cursor", new OpenCursorCallback() {
                    @Override
                    public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                        ManagedLedger ledger = (ManagedLedger) ctx;

                        ledger.asyncAddEntry("test".getBytes(Encoding), new AddEntryCallback() {
                            @Override
                            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                                @SuppressWarnings("unchecked")
                                Pair<ManagedLedger, ManagedCursor> pair = (Pair<ManagedLedger, ManagedCursor>) ctx;
                                ManagedLedger ledger = pair.getLeft();
                                ManagedCursor cursor = pair.getRight();

                                assertEquals(ledger.getNumberOfEntries(), 1);
                                assertEquals(ledger.getTotalSize(), "test".getBytes(Encoding).length);

                                cursor.asyncReadEntries(2, new ReadEntriesCallback() {
                                    @Override
                                    public void readEntriesComplete(List<Entry> entries, Object ctx) {
                                        ManagedCursor cursor = (ManagedCursor) ctx;

                                        assertEquals(entries.size(), 1);
                                        Entry entry = entries.get(0);
                                        final Position position = entry.getPosition();
                                        assertEquals(new String(entry.getDataAndRelease(), Encoding), "test");

                                        log.debug("Mark-Deleting to position {}", position);
                                        cursor.asyncMarkDelete(position, new MarkDeleteCallback() {
                                            @Override
                                            public void markDeleteComplete(Object ctx) {
                                                log.debug("Mark delete complete");
                                                ManagedCursor cursor = (ManagedCursor) ctx;
                                                assertFalse(cursor.hasMoreEntries());

                                                counter.countDown();
                                            }

                                            @Override
                                            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                                                fail(exception.getMessage());
                                            }

                                        }, cursor);
                                    }

                                    @Override
                                    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                                        fail(exception.getMessage());
                                    }
                                }, cursor, PositionFactory.LATEST);
                            }

                            @Override
                            public void addFailed(ManagedLedgerException exception, Object ctx) {
                                fail(exception.getMessage());
                            }
                        }, Pair.of(ledger, cursor));
                    }

                    @Override
                    public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                        fail(exception.getMessage());
                    }

                }, ledger);
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                fail(exception.getMessage());
            }
        }, null, null);

        counter.await();

        log.info("Test completed");
    }

    @Test(timeOut = 20000)
    public void spanningMultipleLedgers() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(10);
        ManagedLedger ledger = factory.open("my_test_ledger", config);

        assertEquals(ledger.getNumberOfEntries(), 0);
        assertEquals(ledger.getTotalSize(), 0);

        ManagedCursor cursor = ledger.openCursor("c1");

        for (int i = 0; i < 11; i++) {
            ledger.addEntry(("dummy-entry-" + i).getBytes(Encoding));
        }

        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 11);
        assertFalse(cursor.hasMoreEntries());

        Position first = entries.get(0).getPosition();
        Position last = entries.get(entries.size() - 1).getPosition();
        entries.forEach(Entry::release);

        log.info("First={} Last={}", first, last);
        assertTrue(first.getLedgerId() < last.getLedgerId());
        assertEquals(first.getEntryId(), 0);
        assertEquals(last.getEntryId(), 0);

        // Read again, from next ledger id
        entries = cursor.readEntries(100);
        assertEquals(entries.size(), 0);
        assertFalse(cursor.hasMoreEntries());

        ledger.close();
    }

    @Test
    public void testStartReadOperationOnLedgerWithEmptyLedgers() throws ManagedLedgerException, InterruptedException {
        ManagedLedger ledger = factory.open("my_test_ledger_1");
        ManagedLedgerImpl ledgerImpl = (ManagedLedgerImpl) ledger;
        NavigableMap<Long, LedgerInfo> ledgers = ledgerImpl.getLedgersInfo();
        LedgerInfo ledgerInfo = ledgers.firstEntry().getValue();
        ledgers.clear();
        ManagedCursor c1 = ledger.openCursor("c1");
        Position position = PositionFactory.create(ledgerInfo.getLedgerId(), 0);
        Position maxPosition = PositionFactory.create(ledgerInfo.getLedgerId(), 99);
        OpReadEntry opReadEntry = OpReadEntry.create((ManagedCursorImpl) c1, position, 20,
                new ReadEntriesCallback() {

                    @Override
                    public void readEntriesComplete(List<Entry> entries, Object ctx) {

                    }

                    @Override
                    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {

                    }
                }, null, maxPosition, null);
        Assert.assertEquals(opReadEntry.readPosition, position);
    }


    @Test(timeOut = 20000)
    public void spanningMultipleLedgersWithSize() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(1000000);
        config.setMaxSizePerLedgerMb(1);
        config.setEnsembleSize(1);
        config.setWriteQuorumSize(1).setAckQuorumSize(1);
        config.setMetadataWriteQuorumSize(1).setMetadataAckQuorumSize(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);

        assertEquals(ledger.getNumberOfEntries(), 0);
        assertEquals(ledger.getTotalSize(), 0);

        ManagedCursor cursor = ledger.openCursor("c1");

        byte[] content = new byte[1023 * 1024];

        for (int i = 0; i < 3; i++) {
            ledger.addEntry(content);
        }

        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 3);
        assertFalse(cursor.hasMoreEntries());

        Position first = entries.get(0).getPosition();
        Position last = entries.get(entries.size() - 1).getPosition();
        entries.forEach(Entry::release);

        // Read again, from next ledger id
        entries = cursor.readEntries(100);
        assertEquals(entries.size(), 0);
        assertFalse(cursor.hasMoreEntries());
        entries.forEach(Entry::release);

        log.info("First={} Last={}", first, last);
        assertTrue(first.getLedgerId() < last.getLedgerId());
        assertEquals(first.getEntryId(), 0);
        assertEquals(last.getEntryId(), 0);
        ledger.close();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void invalidReadEntriesArg1() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("entry".getBytes());
        cursor.readEntries(-1);

        fail("Should have thrown an exception in the above line");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void invalidReadEntriesArg2() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("entry".getBytes());
        cursor.readEntries(0);

        fail("Should have thrown an exception in the above line");
    }

    @Test(timeOut = 20000)
    public void deleteAndReopen() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);
        ledger.close();

        // Reopen
        ledger = factory.open("my_test_ledger");
        assertEquals(ledger.getNumberOfEntries(), 1);

        // Delete and reopen
        ledger.delete();
        ledger = factory.open("my_test_ledger");
        assertEquals(ledger.getNumberOfEntries(), 0);
        ledger.close();
    }

    @Test(timeOut = 20000)
    public void deleteAndReopenWithCursors() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test-cursor");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);
        ledger.close();

        // Reopen
        ledger = factory.open("my_test_ledger");
        assertEquals(ledger.getNumberOfEntries(), 1);

        // Delete and reopen
        ledger.delete();
        ledger = factory.open("my_test_ledger");
        assertEquals(ledger.getNumberOfEntries(), 0);
        ManagedCursor cursor = ledger.openCursor("test-cursor");
        assertFalse(cursor.hasMoreEntries());
        ledger.close();
    }

    @Test(timeOut = 20000)
    public void asyncDeleteWithError() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test-cursor");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);
        ledger.close();

        // Reopen
        ledger = factory.open("my_test_ledger");
        assertEquals(ledger.getNumberOfEntries(), 1);

        final CountDownLatch counter = new CountDownLatch(1);
        stopBookKeeper();
        stopMetadataStore();

        // Delete and reopen
        factory.open("my_test_ledger", initManagedLedgerConfig(new ManagedLedgerConfig()))
                .asyncDelete(new DeleteLedgerCallback() {

            @Override
            public void deleteLedgerComplete(Object ctx) {
                assertNull(ctx);
                fail("The async-call should have failed");
            }

            @Override
            public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                counter.countDown();
            }

        }, null);

        counter.await();
    }

    private byte[] copyBytesFromByteBuf(final ByteBuf buf) {
        final int index = buf.readerIndex();
        final byte[] bytes = new byte[buf.readableBytes()];
        buf.getBytes(index, bytes);
        buf.readerIndex(index);
        return bytes;
    }

    @Test(timeOut = 20000)
    public void asyncAddEntryWithoutError() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger",
                initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(2));
        ledger.openCursor("test-cursor");

        final int count = 4;
        final CountDownLatch counter = new CountDownLatch(count);

        final byte[] bytes = "dummy-entry-1".getBytes(Encoding);
        AddEntryCallback callback = new AddEntryCallback() {
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                assertNull(ctx);
                assertEquals(copyBytesFromByteBuf(entryData), bytes);

                // `entryData` is read-only so that write related methods will throw ReadOnlyBufferException
                try {
                    entryData.array();
                } catch (Exception e) {
                    assertTrue(e instanceof ReadOnlyBufferException);
                }

                counter.countDown();
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                fail(exception.getMessage());
            }

        };
        for (int i = 0; i < count; i++) {
            ledger.asyncAddEntry(bytes, callback, null);
        }

        counter.await();
        assertEquals(ledger.getNumberOfEntries(), count);
        assertEquals(ledger.getTotalSize(), "dummy-entry-1".getBytes(Encoding).length * count);
    }

    @Test(timeOut = 20000)
    public void doubleAsyncAddEntryWithoutError() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test-cursor");

        final CountDownLatch done = new CountDownLatch(10);

        for (int i = 0; i < 10; i++) {
            final String content = "dummy-entry-" + i;
            ledger.asyncAddEntry(content.getBytes(Encoding), new AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    assertNotNull(ctx);
                    assertEquals(copyBytesFromByteBuf(entryData), content.getBytes(Encoding));

                    log.info("Successfully added {}", content);
                    done.countDown();
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    fail(exception.getMessage());
                }

            }, this);
        }

        done.await();
        assertEquals(ledger.getNumberOfEntries(), 10);
    }

    @Test(timeOut = 20000)
    public void asyncAddEntryWithError() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test-cursor");

        final CountDownLatch counter = new CountDownLatch(1);
        stopBookKeeper();
        stopMetadataStore();

        ledger.asyncAddEntry("dummy-entry-1".getBytes(Encoding), new AddEntryCallback() {
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                fail("Should have failed");
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                counter.countDown();
            }

        }, null);

        counter.await();
    }

    @Test(timeOut = 20000)
    public void asyncCloseWithoutError() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test-cursor");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        final CountDownLatch counter = new CountDownLatch(1);

        ledger.asyncClose(new CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                assertNull(ctx);
                counter.countDown();
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                fail(exception.getMessage());
            }

        }, null);

        counter.await();
    }

    @Test(timeOut = 20000)
    public void asyncOpenCursorWithoutError() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        final CountDownLatch counter = new CountDownLatch(1);

        ledger.asyncOpenCursor("test-cursor", new OpenCursorCallback() {
            @Override
            public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                assertNull(ctx);
                assertNotNull(cursor);

                counter.countDown();
            }

            @Override
            public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                fail(exception.getMessage());
            }

        }, null);

        counter.await();
    }

    @Test(timeOut = 20000)
    public void asyncOpenCursorWithError() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        final CountDownLatch counter = new CountDownLatch(1);

        stopBookKeeper();
        stopMetadataStore();

        ledger.asyncOpenCursor("test-cursor", new OpenCursorCallback() {
            @Override
            public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                fail("The async-call should have failed");
            }

            @Override
            public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                counter.countDown();
            }
        }, null);

        counter.await();
    }

    @Test(timeOut = 20000)
    public void readFromOlderLedger() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("test");

        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));

        assertTrue(cursor.hasMoreEntries());
    }

    @Test(timeOut = 20000)
    public void readFromOlderLedgers() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("test");

        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));
        ledger.addEntry("entry-3".getBytes(Encoding));

        assertTrue(cursor.hasMoreEntries());
        cursor.readEntries(1).forEach(Entry::release);

        assertTrue(cursor.hasMoreEntries());
        cursor.readEntries(1).forEach(Entry::release);
        assertTrue(cursor.hasMoreEntries());
        cursor.readEntries(1).forEach(Entry::release);
        assertFalse(cursor.hasMoreEntries());
    }

    @Test(timeOut = 20000)
    public void triggerLedgerDeletion() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("test");

        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));
        ledger.addEntry("entry-3".getBytes(Encoding));

        assertTrue(cursor.hasMoreEntries());
        List<Entry> entries = cursor.readEntries(1);
        assertEquals(entries.size(), 1);
        assertEquals(ledger.getNumberOfEntries(), 3);
        entries.forEach(Entry::release);

        assertTrue(cursor.hasMoreEntries());
        entries = cursor.readEntries(1);
        assertTrue(cursor.hasMoreEntries());

        cursor.markDelete(entries.get(0).getPosition());
        entries.forEach(Entry::release);
    }

    @Test(timeOut = 20000)
    public void testEmptyManagedLedgerContent() throws Exception {
        metadataStore.put("/managed-ledger/my_test_ledger", " ".getBytes(), Optional.empty()).join();

        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("test");

        ledger.addEntry("entry-1".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);
    }

    @Test(timeOut = 20000)
    public void testProducerAndNoConsumer() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);

        assertEquals(ledger.getNumberOfEntries(), 0);

        ledger.addEntry("entry-1".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);

        // Since there are no consumers, older ledger will be deleted
        // in a short time (in a background thread)
        ledger.addEntry("entry-2".getBytes(Encoding));
        while (ledger.getNumberOfEntries() > 1) {
            log.debug("entries={}", ledger.getNumberOfEntries());
            Thread.sleep(100);
        }

        ledger.addEntry("entry-3".getBytes(Encoding));
        while (ledger.getNumberOfEntries() > 1) {
            log.debug("entries={}", ledger.getNumberOfEntries());
            Thread.sleep(100);
        }
    }

    @Test(timeOut = 20000)
    public void testTrimmer() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("c1");

        assertEquals(ledger.getNumberOfEntries(), 0);

        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));
        ledger.addEntry("entry-3".getBytes(Encoding));
        ledger.addEntry("entry-4".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 4);

        cursor.readEntries(1).forEach(Entry::release);
        cursor.readEntries(1).forEach(Entry::release);
        List<Entry> entries = cursor.readEntries(1);
        Position lastPosition = entries.get(0).getPosition();
        entries.forEach(Entry::release);

        assertEquals(ledger.getNumberOfEntries(), 4);

        cursor.markDelete(lastPosition);

        Awaitility.await().untilAsserted(() -> {
            // The number of entries in the ledger should not contain the entry in the mark delete position.
            // last position is the position of entry-3.
            // cursor.markDelete(lastPosition);
            // only entry-4 is left in the ledger.
            assertEquals(ledger.getNumberOfEntries(), 1);
        });
    }

    @Test(timeOut = 20000)
    public void testAsyncAddEntryAndSyncClose() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(10);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ledger.openCursor("c1");

        assertEquals(ledger.getNumberOfEntries(), 0);

        final CountDownLatch counter = new CountDownLatch(100);

        for (int i = 0; i < 100; i++) {
            String content = "entry-" + i;
            ledger.asyncAddEntry(content.getBytes(Encoding), new AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    counter.countDown();
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    fail(exception.getMessage());
                }

            }, null);
        }

        counter.await();

        assertEquals(ledger.getNumberOfEntries(), 100);
    }

    @Test(timeOut = 20000)
    public void moveCursorToNextLedger() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("test");

        ledger.addEntry("entry-1".getBytes(Encoding));
        log.debug("Added 1st message");
        List<Entry> entries = cursor.readEntries(1);
        log.debug("read message ok");
        assertEquals(entries.size(), 1);
        entries.forEach(Entry::release);

        ledger.addEntry("entry-2".getBytes(Encoding));
        log.debug("Added 2nd message");
        ledger.addEntry("entry-3".getBytes(Encoding));
        log.debug("Added 3nd message");

        assertTrue(cursor.hasMoreEntries());
        assertEquals(cursor.getNumberOfEntries(), 2);

        entries = cursor.readEntries(2);
        assertEquals(entries.size(), 2);
        entries.forEach(Entry::release);

        entries = cursor.readEntries(2);
        assertEquals(entries.size(), 0);

        entries = cursor.readEntries(2);
        assertEquals(entries.size(), 0);
    }

    @Test(timeOut = 20000)
    public void differentSessions() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        assertEquals(ledger.getNumberOfEntries(), 0);
        assertEquals(ledger.getTotalSize(), 0);

        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        assertEquals(ledger.getNumberOfEntries(), 1);
        assertEquals(ledger.getTotalSize(), "dummy-entry-1".getBytes(Encoding).length);

        assertTrue(cursor.hasMoreEntries());
        assertEquals(cursor.getNumberOfEntries(), 1);

        ledger.close();

        // Create a new factory and re-open the same managed ledger
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);

        ledger = factory2.open("my_test_ledger");

        assertEquals(ledger.getNumberOfEntries(), 1);
        assertEquals(ledger.getTotalSize(), "dummy-entry-1".getBytes(Encoding).length);

        cursor = ledger.openCursor("c1");

        assertTrue(cursor.hasMoreEntries());
        assertEquals(cursor.getNumberOfEntries(), 1);

        ledger.addEntry("dummy-entry-2".getBytes(Encoding));

        assertEquals(ledger.getNumberOfEntries(), 2);
        assertEquals(ledger.getTotalSize(), "dummy-entry-1".getBytes(Encoding).length * 2);

        assertTrue(cursor.hasMoreEntries());
        assertEquals(cursor.getNumberOfEntries(), 2);

        ledger.close();
    }

    @Test(enabled = false)
    public void fenceManagedLedger() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory1 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedger ledger1 = factory1.open("my_test_ledger");
        ManagedCursor cursor1 = ledger1.openCursor("c1");
        ledger1.addEntry("entry-1".getBytes(Encoding));

        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedger ledger2 = factory2.open("my_test_ledger");
        ManagedCursor cursor2 = ledger2.openCursor("c1");

        // At this point ledger1 must have been fenced
        try {
            ledger1.addEntry("entry-1".getBytes(Encoding));
            fail("Expecting exception");
        } catch (ManagedLedgerFencedException e) {
        }

        try {
            ledger1.addEntry("entry-2".getBytes(Encoding));
            fail("Expecting exception");
        } catch (ManagedLedgerFencedException e) {
        }

        try {
            cursor1.readEntries(10);
            fail("Expecting exception");
        } catch (ManagedLedgerFencedException e) {
        }

        try {
            ledger1.openCursor("new cursor");
            fail("Expecting exception");
        } catch (ManagedLedgerFencedException e) {
        }

        ledger2.addEntry("entry-2".getBytes(Encoding));

        assertEquals(cursor2.getNumberOfEntries(), 2);
    }

    @Test
    public void forceCloseLedgers() throws Exception {
        ManagedLedger ledger1 = factory.open("my_test_ledger",
                initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(1));
        ledger1.openCursor("c1");
        ManagedCursor c2 = ledger1.openCursor("c2");
        ledger1.addEntry("entry-1".getBytes(Encoding));
        ledger1.addEntry("entry-2".getBytes(Encoding));
        ledger1.addEntry("entry-3".getBytes(Encoding));

        c2.readEntries(1).forEach(Entry::release);
        c2.readEntries(1).forEach(Entry::release);
        c2.readEntries(1).forEach(Entry::release);

        ledger1.close();

        try {
            ledger1.addEntry("entry-3".getBytes(Encoding));
            fail("should not have reached this point");
        } catch (ManagedLedgerException e) {
            // ok
        }

        try {
            ledger1.openCursor("new-cursor");
            fail("should not have reached this point");
        } catch (ManagedLedgerException e) {
            // ok
        }
    }

    @Test
    public void closeLedgerWithError() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.addEntry("entry-1".getBytes(Encoding));

        stopMetadataStore();
        stopBookKeeper();

        try {
            ledger.close();
            // fail("should have thrown exception");
        } catch (ManagedLedgerException e) {
            // Ok
        }
    }

    @Test(timeOut = 20000)
    public void deleteWithErrors1() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        Position position = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);

        // Force delete a ledger and test that deleting the ML still happens
        // without errors
        bkc.deleteLedger(position.getLedgerId());
        ledger.delete();
    }

    @Test(timeOut = 20000)
    public void deleteWithErrors2() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        stopMetadataStore();

        try {
            ledger.delete();
            fail("should have failed");
        } catch (ManagedLedgerException e) {
            // ok
        } catch (RejectedExecutionException e) {
            // ok
        }
    }

    @Test(timeOut = 20000)
    public void readWithErrors1() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger",
                initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(1));
        ManagedCursor cursor = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));

        stopMetadataStore();
        stopBookKeeper();

        try {
            cursor.readEntries(10);
            fail("should have failed");
        } catch (ManagedLedgerException e) {
            // ok
        }

        try {
            ledger.addEntry("dummy-entry-3".getBytes(Encoding));
            fail("should have failed");
        } catch (ManagedLedgerException e) {
            // ok
        }
    }

    @Test(timeOut = 20000, enabled = false)
    void concurrentAsyncOpen() throws Exception {
        final CountDownLatch counter = new CountDownLatch(2);

        class Result {
            ManagedLedger instance1 = null;
            ManagedLedger instance2 = null;
        }

        final Result result = new Result();
        factory.asyncOpen("my-test-ledger", new OpenLedgerCallback() {

            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                result.instance1 = ledger;
                counter.countDown();
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
            }
        }, null);

        factory.asyncOpen("my-test-ledger", new OpenLedgerCallback() {

            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                result.instance2 = ledger;
                counter.countDown();
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
            }
        }, null);

        counter.await();
        assertEquals(result.instance1, result.instance2);
        assertNotNull(result.instance1);
    }

    @Test // (timeOut = 20000)
    public void asyncOpenClosedLedger() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my-closed-ledger");

        ManagedCursor c1 = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        c1.close();

        assertEquals(ledger.getNumberOfEntries(), 1);

        ledger.setFenced();

        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedger instance1 = null;
        }

        final Result result = new Result();
        factory.asyncOpen("my-closed-ledger", new OpenLedgerCallback() {

            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                result.instance1 = ledger;
                counter.countDown();
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
            }
        }, null);
        counter.await();
        assertNotNull(result.instance1);

        ManagedCursor c2 = result.instance1.openCursor("c1");
        List<Entry> entries = c2.readEntries(1);
        assertEquals(entries.size(), 1);
        entries.forEach(Entry::release);

    }

    @Test
    public void getCursors() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");
        ManagedCursor c2 = ledger.openCursor("c2");

        assertEquals(Sets.newHashSet(ledger.getCursors()), Sets.newHashSet(c1, c2));

        c1.close();
        ledger.deleteCursor("c1");
        assertEquals(Sets.newHashSet(ledger.getCursors()), Sets.newHashSet(c2));

        c2.close();
        ledger.deleteCursor("c2");
        assertEquals(Sets.newHashSet(ledger.getCursors()), new HashSet());
    }

    @Test
    public void testUpdateProperties() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");
        properties.put("key3", "value3");
        ledger.setProperties(properties);
        assertEquals(ledger.getProperties(), properties);

        properties.put("key4", "value4");
        ledger.setProperty("key4", "value4");
        assertEquals(ledger.getProperties(), properties);

        ledger.deleteProperty("key4");
        properties.remove("key4");
        assertEquals(ledger.getProperties(), properties);

        Map<String, String> newProperties = new HashMap<>();
        newProperties.put("key5", "value5");
        newProperties.put("key1", "value6");
        newProperties.putAll(properties);
        ledger.setProperties(newProperties);
        assertEquals(ledger.getProperties(), newProperties);
    }

    @Test
    public void testAsyncUpdateProperties() throws Exception {

        ManagedLedger ledger = factory.open("my_test_ledger");
        Map<String, String> prop = new HashMap<>();
        prop.put("key1", "value1");
        prop.put("key2", "value2");
        prop.put("key3", "value3");

        final CountDownLatch latch1 = new CountDownLatch(1);
        ledger.asyncSetProperties(prop, new AsyncCallbacks.UpdatePropertiesCallback() {
            @Override
            public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                assertEquals(prop, properties);
                latch1.countDown();
            }

            @Override
            public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
            }
        }, null);
        assertTrue(latch1.await(5, TimeUnit.SECONDS));

        final CountDownLatch latch2 = new CountDownLatch(1);
        ledger.asyncSetProperty("key4", "value4", new AsyncCallbacks.UpdatePropertiesCallback() {
            @Override
            public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                assertNotNull(properties.get("key4"));
                assertEquals("value4", properties.get("key4"));
                latch2.countDown();
            }

            @Override
            public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
            }
        }, null);
        assertTrue(latch2.await(5, TimeUnit.SECONDS));

        prop.remove("key1");

        final CountDownLatch latch3 = new CountDownLatch(1);
        ledger.asyncDeleteProperty("key1", new AsyncCallbacks.UpdatePropertiesCallback() {
            @Override
            public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                assertNull(properties.get("key1"));
                latch3.countDown();
            }

            @Override
            public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
            }
        }, null);
        assertTrue(latch3.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testConcurrentAsyncSetProperties() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1000);
        ManagedLedger ledger = factory.open("my_test_ledger",
                initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(1));
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i = 0; i < 1000; i++) {
            final int finalI = i;
            executor.execute(() -> {
                Map<String, String> newProperties = new HashMap<>();
                newProperties.put("key0", String.valueOf(finalI));
                newProperties.put("key1", "value1");
                newProperties.put("key2", "value2");
                newProperties.put("key3", "value3");
                ledger.asyncSetProperties(newProperties, new AsyncCallbacks.UpdatePropertiesCallback() {
                    @Override
                    public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                        assertEquals(properties, newProperties);
                        latch.countDown();
                    }

                    @Override
                    public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
                    }
                }, null);
            });
        }
        try {
            for (int i = 0; i < 100; i++) {
                ledger.addEntry("data".getBytes(Encoding));
                Thread.sleep(300);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
        assertTrue(latch.await(300, TimeUnit.SECONDS));
        factory.shutdown();
    }

    @Test
    public void ledgersList() throws Exception {
        MetaStore store = factory.getMetaStore();

        assertEquals(Sets.newHashSet(store.getManagedLedgers()), new HashSet());
        ManagedLedger ledger1 = factory.open("ledger1");
        assertEquals(Sets.newHashSet(store.getManagedLedgers()), Sets.newHashSet("ledger1"));
        ManagedLedger ledger2 = factory.open("ledger2");
        assertEquals(Sets.newHashSet(store.getManagedLedgers()), Sets.newHashSet("ledger1", "ledger2"));
        ledger1.delete();
        assertEquals(Sets.newHashSet(store.getManagedLedgers()), Sets.newHashSet("ledger2"));
        ledger2.delete();
        assertEquals(Sets.newHashSet(store.getManagedLedgers()), new HashSet());
    }

    @Test
    public void testCleanup() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("c1");

        ledger.addEntry("data".getBytes(Encoding));
        assertEquals(bkc.getLedgers().size(), 1);

        ledger.delete();
        assertEquals(bkc.getLedgers().size(), 0);
    }

    @Test(timeOut = 20000)
    public void testAsyncCleanup() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("c1");

        ledger.addEntry("data".getBytes(Encoding));
        assertEquals(bkc.getLedgers().size(), 1);

        final CountDownLatch latch = new CountDownLatch(1);

        ledger.asyncDelete(new DeleteLedgerCallback() {
            @Override
            public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                fail("should have succeeded");
            }

            @Override
            public void deleteLedgerComplete(Object ctx) {
                latch.countDown();
            }
        }, null);

        latch.await();
        assertEquals(bkc.getLedgers().size(), 0);
    }

    @Test(timeOut = 20000)
    public void testReopenAndCleanup() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("c1");

        ledger.addEntry("data".getBytes(Encoding));
        ledger.close();
        Thread.sleep(100);
        assertEquals(bkc.getLedgers().size(), 1);

        factory.shutdown();

        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory.open("my_test_ledger");
        ledger.openCursor("c1");
        Thread.sleep(100);
        assertEquals(bkc.getLedgers().size(), 2);

        ledger.close();
        factory.open("my_test_ledger", initManagedLedgerConfig(new ManagedLedgerConfig())).delete();
        Thread.sleep(100);
        assertEquals(bkc.getLedgers().size(), 0);

        factory.shutdown();
    }

    @Test(timeOut = 20000)
    public void doubleOpen() throws Exception {
        ManagedLedger ledger1 = factory.open("my_test_ledger");
        ManagedLedger ledger2 = factory.open("my_test_ledger");

        assertSame(ledger1, ledger2);
    }

    @Test
    public void compositeNames() throws Exception {
        // Should not throw exception
        factory.open("my/test/ledger");
    }

    @Test
    public void previousPosition() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger",
                initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(2));
        ManagedCursor cursor = ledger.openCursor("my_cursor");

        Position p0 = cursor.getMarkDeletedPosition();
        // This is expected because p0 is already an "invalid" position (since no entry has been mark-deleted yet)
        assertEquals(ledger.getPreviousPosition(p0), p0);

        // Force to close an empty ledger
        ledger.close();

        ledger = (ManagedLedgerImpl) factory.open("my_test_ledger",
                initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(2));
        // again
        ledger.close();

        ledger = (ManagedLedgerImpl) factory.open("my_test_ledger",
                initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(2));
        Position pBeforeWriting = ledger.getLastPosition();
        Position p1 = ledger.addEntry("entry".getBytes());
        ledger.close();

        ledger = (ManagedLedgerImpl) factory.open("my_test_ledger",
                initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(2));
        Position p2 = ledger.addEntry("entry".getBytes());
        Position p3 = ledger.addEntry("entry".getBytes());
        Position p4 = ledger.addEntry("entry".getBytes());

        assertEquals(ledger.getPreviousPosition(p1), pBeforeWriting);
        assertEquals(ledger.getPreviousPosition(p2), p1);
        assertEquals(ledger.getPreviousPosition(p3), p2);
        assertEquals(ledger.getPreviousPosition(p4), p3);
    }

    /**
     * Reproduce a race condition between opening cursors and concurrent mark delete operations.
     */
    @Test(timeOut = 20000)
    public void testOpenRaceCondition() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setEnsembleSize(2).setAckQuorumSize(2).setMetadataEnsembleSize(2);
        final ManagedLedger ledger = factory.open("my-ledger", config);
        final ManagedCursor c1 = ledger.openCursor("c1");

        final int num = 1000;
        final Position position = ledger.addEntry("entry-0".getBytes());
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();
        final CountDownLatch counter = new CountDownLatch(2);
        executor.execute(() -> {
            try {
                for (int i = 0; i < num; i++) {
                    c1.markDelete(position);
                }
                counter.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        executor.execute(() -> {
            try {
                for (int i = 0; i < num; i++) {
                    ledger.openCursor("cursor-" + i);
                }
                counter.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // If there is the race condition, this method will not complete triggering the test timeout
        counter.await();
    }

    @Test
    public void invalidateConsumedEntriesFromCache() throws Exception {
        ManagedLedgerImpl ledger =
                (ManagedLedgerImpl) factory.open("my_test_ledger_for_invalidateConsumedEntriesFromCache");

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        EntryCache entryCache = ledger.entryCache;
        entryCache.clear();

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        ManagedCursorImpl c2 = (ManagedCursorImpl) ledger.openCursor("c2");

        Position p1 = ledger.addEntry("entry-1".getBytes());
        Position p2 = ledger.addEntry("entry-2".getBytes());
        Position p3 = ledger.addEntry("entry-3".getBytes());
        Position p4 = ledger.addEntry("entry-4".getBytes());

        assertEquals(entryCache.getSize(), 7 * 4);
        assertEquals(cacheManager.getSize(), entryCache.getSize());


        c2.setReadPosition(p3);


        ledger.waitForPendingCacheEvictions();
        assertEquals(entryCache.getSize(), 7 * 4);
        assertEquals(cacheManager.getSize(), entryCache.getSize());

        c1.setReadPosition(p2);

        ledger.waitForPendingCacheEvictions();
        assertEquals(entryCache.getSize(), 7 * 3);
        assertEquals(cacheManager.getSize(), entryCache.getSize());

        c1.setReadPosition(p3);

        ledger.waitForPendingCacheEvictions();
        assertEquals(entryCache.getSize(), 7 * 2);
        assertEquals(cacheManager.getSize(), entryCache.getSize());


        ledger.deactivateCursor(c1);

        ledger.waitForPendingCacheEvictions();
        assertEquals(entryCache.getSize(), 7 * 2); // as c2.readPosition=p3 => Cache contains p3,p4
        assertEquals(cacheManager.getSize(), entryCache.getSize());


        c2.setReadPosition(p4);

        ledger.waitForPendingCacheEvictions();
        assertEquals(entryCache.getSize(), 7);
        assertEquals(cacheManager.getSize(), entryCache.getSize());


        ledger.deactivateCursor(c2);

        ledger.waitForPendingCacheEvictions();
        assertEquals(entryCache.getSize(), 0);
        assertEquals(cacheManager.getSize(), entryCache.getSize());
    }

    @Test
    public void invalidateEntriesFromCacheByMarkDeletePosition() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setCacheEvictionByMarkDeletedPosition(true);
        ManagedLedgerImpl ledger =
                (ManagedLedgerImpl) factory.open("my_test_ledger_for_invalidateEntriesFromCacheByMarkDeletePosition",
                        config);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        EntryCache entryCache = ledger.entryCache;
        entryCache.clear();

        ManagedCursorImpl c1 = (ManagedCursorImpl) ledger.openCursor("c1");
        ManagedCursorImpl c2 = (ManagedCursorImpl) ledger.openCursor("c2");

        Position p1 = ledger.addEntry("entry-1".getBytes());
        Position p2 = ledger.addEntry("entry-2".getBytes());
        Position p3 = ledger.addEntry("entry-3".getBytes());
        Position p4 = ledger.addEntry("entry-4".getBytes());

        assertEquals(entryCache.getSize(), 7 * 4);
        assertEquals(cacheManager.getSize(), entryCache.getSize());


        c2.setReadPosition(p4);
        c2.markDelete(p3);

        ledger.waitForPendingCacheEvictions();
        assertEquals(entryCache.getSize(), 7 * 4);
        assertEquals(cacheManager.getSize(), entryCache.getSize());


        c1.setReadPosition(p3);
        c1.markDelete(p2);

        ledger.waitForPendingCacheEvictions();
        assertEquals(entryCache.getSize(), 7 * 3);
        assertEquals(cacheManager.getSize(), entryCache.getSize());


        c1.setReadPosition(p4);
        c1.markDelete(p3);

        ledger.waitForPendingCacheEvictions();
        assertEquals(entryCache.getSize(), 7 * 2);
        assertEquals(cacheManager.getSize(), entryCache.getSize());


        ledger.deactivateCursor(c1);

        ledger.waitForPendingCacheEvictions();
        assertEquals(entryCache.getSize(), 7 * 2);
        assertEquals(cacheManager.getSize(), entryCache.getSize());


        c2.markDelete(p4);
        ledger.waitForPendingCacheEvictions();
        assertEquals(entryCache.getSize(), 7);
        assertEquals(cacheManager.getSize(), entryCache.getSize());


        ledger.deactivateCursor(c2);
        ledger.waitForPendingCacheEvictions();
        assertEquals(entryCache.getSize(), 0);
        assertEquals(cacheManager.getSize(), entryCache.getSize());
    }

    @Test
    public void discardEmptyLedgersOnClose() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry("entry".getBytes());

        assertEquals(ledger.getLedgersInfoAsList().size(), 1);

        c1.close();
        ledger.close();

        // re-open
        ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        assertEquals(ledger.getLedgersInfoAsList().size(), 2); // 1 ledger with 1 entry and the current writing ledger

        c1.close();
        ledger.close();

        // re-open, now the previous empty ledger should have been discarded
        ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        assertEquals(ledger.getLedgersInfoAsList().size(), 2); // 1 ledger with 1 entry, and the current
        // writing ledger
    }

    @Test
    public void discardEmptyLedgersOnError() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");

        assertEquals(ledger.getLedgersInfoAsList().size(), 1);

        bkc.failNow(BKException.Code.NoBookieAvailableException);
        metadataStore.failConditional(new MetadataStoreException("error"), (op, path) ->
                path.equals("/managed-ledgers/my_test_ledger")
                        && op == FaultInjectionMetadataStore.OperationType.PUT
        );

        try {
            ledger.addEntry("entry".getBytes());
            fail("Should have received exception");
        } catch (ManagedLedgerException e) {
            // Ok
        }

        assertEquals(ledger.getLedgersInfoAsList().size(), 0);

        // Next write should fail as well
        bkc.failNow(BKException.Code.NoBookieAvailableException);
        try {
            ledger.addEntry("entry".getBytes());
            fail("Should have received exception");
        } catch (ManagedLedgerException e) {
            // Ok
        }

        assertEquals(ledger.getLedgersInfoAsList().size(), 0);
        assertEquals(ledger.getNumberOfEntries(), 0);
    }

    @Test
    public void cursorReadsWithDiscardedEmptyLedgers() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");

        Position p1 = c1.getReadPosition();

        c1.close();
        ledger.close();

        // re-open
        ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        c1 = ledger.openCursor("c1");

        assertEquals(c1.getNumberOfEntries(), 0);
        assertFalse(c1.hasMoreEntries());

        ledger.addEntry("entry".getBytes());

        assertEquals(c1.getNumberOfEntries(), 1);
        assertTrue(c1.hasMoreEntries());

        assertEquals(ledger.getLedgersInfoAsList().size(), 1);

        List<Entry> entries = c1.readEntries(1);
        assertEquals(entries.size(), 1);
        entries.forEach(Entry::release);

        assertFalse(c1.hasMoreEntries());
        assertEquals(c1.readEntries(1).size(), 0);

        c1.seek(p1);
        assertTrue(c1.hasMoreEntries());
        assertEquals(c1.getNumberOfEntries(), 1);

        entries = c1.readEntries(1);
        assertEquals(entries.size(), 1);
        entries.forEach(Entry::release);
        assertEquals(c1.readEntries(1).size(), 0);
    }

    @Test
    public void testSetTopicMetadata() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");
        final MetaStore store = factory.getMetaStore();
        final CountDownLatch latch = new CountDownLatch(1);
        final ManagedLedgerInfo[] storedMLInfo = new ManagedLedgerInfo[1];
        store.getManagedLedgerInfo("my_test_ledger", true, properties, new MetaStoreCallback<ManagedLedgerInfo>() {
            @Override
            public void operationComplete(ManagedLedgerInfo result, Stat version) {
                storedMLInfo[0] = result;
                latch.countDown();
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                latch.countDown();
                fail("Should have failed here");
            }
        });
        latch.await();

        assertEquals(storedMLInfo[0].getPropertiesCount(), 2);
        assertEquals(storedMLInfo[0].getPropertiesList().get(0).getKey(), "key1");
        assertEquals(storedMLInfo[0].getPropertiesList().get(0).getValue(), "value1");
        assertEquals(storedMLInfo[0].getPropertiesList().get(1).getKey(), "key2");
        assertEquals(storedMLInfo[0].getPropertiesList().get(1).getValue(), "value2");
    }

    @Test
    public void cursorReadsWithDiscardedEmptyLedgersStillListed() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry("entry-1".getBytes());
        ledger.close();

        ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        c1 = ledger.openCursor("c1");
        ledger.addEntry("entry-2".getBytes());

        final LedgerInfo l1info = ledger.getLedgersInfoAsList().get(0);
        final LedgerInfo l2info = ledger.getLedgersInfoAsList().get(1);

        ledger.close();

        // Add the deleted ledger back in the meta-data to simulate an empty ledger that was deleted but not removed
        // from the list of ledgers
        final CountDownLatch counter = new CountDownLatch(1);
        final MetaStore store = factory.getMetaStore();
        store.getManagedLedgerInfo("my_test_ledger", false, new MetaStoreCallback<ManagedLedgerInfo>() {
            @Override
            public void operationComplete(ManagedLedgerInfo result, Stat version) {
                // Update the list
                ManagedLedgerInfo.Builder info = ManagedLedgerInfo.newBuilder(result);
                info.clearLedgerInfo();
                info.addLedgerInfo(LedgerInfo.newBuilder().setLedgerId(l1info.getLedgerId()).build());
                info.addLedgerInfo(l2info);

                store.asyncUpdateLedgerIds("my_test_ledger", info.build(), version, new MetaStoreCallback<Void>() {
                    @Override
                    public void operationComplete(Void result, Stat version) {
                        counter.countDown();
                    }

                    @Override
                    public void operationFailed(MetaStoreException e) {
                        counter.countDown();
                    }
                });
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                counter.countDown();
            }
        });

        // Wait for the change to be effective
        counter.await();

        // Delete the ledger and mantain it in the ledgers list
        bkc.deleteLedger(l1info.getLedgerId());

        // re-open
        ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        c1 = ledger.openCursor("c1");

        assertEquals(c1.getNumberOfEntries(), 1);
        assertTrue(c1.hasMoreEntries());
        assertEquals(ledger.getLedgersInfoAsList().size(), 2);

        List<Entry> entries = c1.readEntries(10);
        assertEquals(entries.size(), 1);
        entries.forEach(Entry::release);

        assertFalse(c1.hasMoreEntries());
        entries = c1.readEntries(1);
        assertEquals(entries.size(), 0);
        entries.forEach(Entry::release);
    }

    @Test
    public void addEntryWithOffset() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry("012345678".getBytes(), 2, 3);

        List<Entry> entries = c1.readEntries(1);
        assertEquals(entries.get(0).getLength(), 3);
        Entry entry = entries.get(0);
        assertEquals(new String(entry.getData()), "234");
        entry.release();
    }

    @Test
    public void totalSizeTest() throws Exception {
        ManagedLedgerConfig conf = new ManagedLedgerConfig();
        initManagedLedgerConfig(conf);
        conf.setMaxEntriesPerLedger(1);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", conf);
        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry(new byte[10], 1, 8);

        assertEquals(ledger.getTotalSize(), 8);

        Position p2 = ledger.addEntry(new byte[12], 2, 5);

        assertEquals(ledger.getTotalSize(), 13);
        c1.markDelete(PositionFactory.create(p2.getLedgerId(), -1));

        // Wait for background trimming
        Thread.sleep(400);
        assertEquals(ledger.getTotalSize(), 5);
    }

    @Test
    public void testMinimumRolloverTime() throws Exception {
        ManagedLedgerConfig conf = new ManagedLedgerConfig();
        initManagedLedgerConfig(conf);
        conf.setMaxEntriesPerLedger(1);
        conf.setMinimumRolloverTime(1, TimeUnit.SECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", conf);
        ledger.openCursor("c1");

        ledger.addEntry("data".getBytes());
        ledger.addEntry("data".getBytes());

        assertEquals(ledger.getLedgersInfoAsList().size(), 1);

        Thread.sleep(1000);

        ledger.addEntry("data".getBytes());
        ledger.addEntry("data".getBytes());

        assertEquals(ledger.getLedgersInfoAsList().size(), 2);
    }

    @Test
    public void testMaximumRolloverTime() throws Exception {
        ManagedLedgerConfig conf = new ManagedLedgerConfig();
        initManagedLedgerConfig(conf);
        conf.setMaxEntriesPerLedger(5);
        conf.setMinimumRolloverTime(1, TimeUnit.SECONDS);
        conf.setMaximumRolloverTime(1, TimeUnit.SECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_maxtime_ledger", conf);
        ledger.openCursor("c1");

        ledger.addEntry("data".getBytes());
        ledger.addEntry("data".getBytes());

        assertEquals(ledger.getLedgersInfoAsList().size(), 1);

        ledger.addEntry("data".getBytes());
        ledger.addEntry("data".getBytes());

        Awaitility.await().untilAsserted(() -> {
            assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        });
    }

    @Test
    public void testNoRolloverIfNoMetadataSession() throws Exception {
        ManagedLedgerConfig conf = new ManagedLedgerConfig();
        initManagedLedgerConfig(conf);
        conf.setMaxEntriesPerLedger(1);
        conf.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testNoRolloverIfNoMetadataSession", conf);
        ledger.openCursor("c1");

        metadataStore.triggerSessionEvent(SessionEvent.SessionLost);

        for (int i = 1; i < 10; i++) {
            ledger.addEntry("data".getBytes());
        }

        // This should not have changed
        assertEquals(ledger.getLedgersInfoAsList().size(), 1);

        metadataStore.triggerSessionEvent(SessionEvent.SessionReestablished);
        ledger.addEntry("data".getBytes());
        ledger.addEntry("data".getBytes());
        ledger.addEntry("data".getBytes());

        // After the re-establishment, we'll be creating new ledgers
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(ledger.getLedgersInfoAsList().size(), 4);
        });
    }

    @Test
    public void testNoRolloverIfNoMetadataSessionWithExistingData() throws Exception {
        ManagedLedgerConfig conf = new ManagedLedgerConfig();
        initManagedLedgerConfig(conf);
        conf.setMaxEntriesPerLedger(2);
        conf.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testNoRolloverIfNoMetadataSession", conf);
        ledger.openCursor("c1");

        ledger.addEntry("data".getBytes());

        assertEquals(ledger.getLedgersInfoAsList().size(), 1);

        metadataStore.triggerSessionEvent(SessionEvent.SessionLost);

        for (int i = 1; i < 10; i++) {
            ledger.addEntry("data".getBytes());
        }

        // This should not have changed
        assertEquals(ledger.getLedgersInfoAsList().size(), 1);

        metadataStore.triggerSessionEvent(SessionEvent.SessionReestablished);
        ledger.addEntry("data".getBytes());
        ledger.addEntry("data".getBytes());

        // After the re-establishment, we'll be creating new ledgers
        assertEquals(ledger.getLedgersInfoAsList().size(), 2);
    }

    @Test
    public void testRetention() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setRetentionSizeInMB(10);
        config.setMaxEntriesPerLedger(1);
        config.setRetentionTime(1, TimeUnit.HOURS);

        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("retention_test_ledger", config);
        ManagedCursor c1 = ml.openCursor("c1");
        ml.addEntry("iamaverylongmessagethatshouldberetained".getBytes());
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        ml.close();

        // reopen ml
        ml = (ManagedLedgerImpl) factory.open("retention_test_ledger", config);
        c1 = ml.openCursor("c1");
        ml.addEntry("shortmessage".getBytes());
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        ml.close();
        assertTrue(ml.getLedgersInfoAsList().size() > 1);
        assertTrue(ml.getTotalSize() > "shortmessage".getBytes().length);
    }

    @Test(enabled = true)
    public void testNoRetention() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setRetentionSizeInMB(0);
        config.setMaxEntriesPerLedger(1);
        // Default is no-retention

        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("noretention_test_ledger", config);
        ManagedCursor c1 = ml.openCursor("c1noretention");
        ml.addEntry("iamaverylongmessagethatshouldnotberetained".getBytes());
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        ml.close();

        // reopen ml
        ml = (ManagedLedgerImpl) factory.open("noretention_test_ledger", config);
        c1 = ml.openCursor("c1noretention");
        ml.addEntry("shortmessage".getBytes());
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        // sleep for trim
        Thread.sleep(1000);
        ml.close();

        assertTrue(ml.getLedgersInfoAsList().size() <= 1);
        assertTrue(ml.getTotalSize() <= "shortmessage".getBytes().length);
    }

    @Test
    public void testDeletionAfterRetention() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setRetentionSizeInMB(0);
        config.setMaxEntriesPerLedger(1);
        config.setRetentionTime(1, TimeUnit.SECONDS);

        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("deletion_after_retention_test_ledger", config);
        ManagedCursor c1 = ml.openCursor("c1noretention");
        ml.addEntry("iamaverylongmessagethatshouldnotberetained".getBytes());
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        ml.close();

        // reopen ml
        ml = (ManagedLedgerImpl) factory.open("deletion_after_retention_test_ledger", config);
        c1 = ml.openCursor("c1noretention");
        ml.addEntry("shortmessage".getBytes());
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        // let retention expire
        Thread.sleep(1000);
        ml.internalTrimConsumedLedgers(CompletableFuture.completedFuture(null));

        assertTrue(ml.getLedgersInfoAsList().size() <= 1);
        assertTrue(ml.getTotalSize() <= "shortmessage".getBytes().length);
        ml.close();
    }

    @Test
    public void testDeletionAfterLedgerClosedAndRetention() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setRetentionSizeInMB(0);
        config.setMaxEntriesPerLedger(1);
        config.setRetentionTime(1, TimeUnit.SECONDS);
        config.setMaximumRolloverTime(1, TimeUnit.SECONDS);

        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("deletion_after_retention_test_ledger", config);
        ManagedCursor c1 = ml.openCursor("testCursor1");
        ManagedCursor c2 = ml.openCursor("testCursor2");
        ml.addEntry("iamaverylongmessagethatshouldnotberetained".getBytes());
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        c2.skipEntries(1, IndividualDeletedEntries.Exclude);
        // let current ledger close
        Field stateUpdater = ManagedLedgerImpl.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(ml, ManagedLedgerImpl.State.LedgerOpened);
        ml.rollCurrentLedgerIfFull();
        // let retention expire
        Thread.sleep(1500);
        // delete the expired ledger
        ml.internalTrimConsumedLedgers(CompletableFuture.completedFuture(null));

        // the closed and expired ledger should be deleted
        assertTrue(ml.getLedgersInfoAsList().size() <= 1);
        assertEquals(ml.getTotalSize(), 0);
        ml.close();
    }

    /**
     * Set retention time = 0 and create a empty ledger,
     * first position can't higher than last after trim ledgers.
     */
    @Test
    public void testRetention0WithEmptyLedger() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setRetentionTime(0, TimeUnit.MINUTES);
        config.setMaxEntriesPerLedger(1);

        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("deletion_after_retention_test_ledger", config);
        ManagedCursor c1 = ml.openCursor("c1noretention");
        ml.addEntry("message1".getBytes());
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        ml.close();

        // reopen ml
        ml = (ManagedLedgerImpl) factory.open("deletion_after_retention_test_ledger", config);
        c1 = ml.openCursor("c1noretention");
        ml.deleteCursor(c1.getName());
        ml.internalTrimConsumedLedgers(CompletableFuture.completedFuture(null));

        assertTrue(ml.getFirstPosition().getLedgerId() <= ml.lastConfirmedEntry.getLedgerId());
        ml.close();
    }

    /**
     * Set retention time = 0 and create a empty ledger,
     * first position can't higher than last after trim ledgers.
     * Even if we do not have subscriptions the ledger
     * that contains the lastConfirmedEntry will be deleted anyway.
     */
    @Test
    public void testRetention0WithEmptyLedgerWithoutCursors() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setRetentionTime(0, TimeUnit.MINUTES);
        config.setMaxEntriesPerLedger(1);

        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("deletion_after_retention_test_ledger", config);
        ml.addEntry("message1".getBytes());
        ml.close();

        // reopen ml
        ml = (ManagedLedgerImpl) factory.open("deletion_after_retention_test_ledger", config);
        ml.internalTrimConsumedLedgers(CompletableFuture.completedFuture(null));

        assertTrue(ml.getFirstPosition().getLedgerId() <= ml.lastConfirmedEntry.getLedgerId());
        assertFalse(ml.getLedgersInfo().containsKey(ml.lastConfirmedEntry.getLedgerId()),
                "the ledger at lastConfirmedEntry has not been trimmed!");
        ml.close();
    }

    @Test
    public void testInfiniteRetention() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setRetentionSizeInMB(-1);
        config.setRetentionTime(-1, TimeUnit.HOURS);
        config.setMaxEntriesPerLedger(1);

        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("retention_test_ledger", config);
        ManagedCursor c1 = ml.openCursor("c1");
        ml.addEntry("iamaverylongmessagethatshouldberetained".getBytes());
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        ml.close();

        // reopen ml
        ml = (ManagedLedgerImpl) factory.open("retention_test_ledger", config);
        c1 = ml.openCursor("c1");
        ml.addEntry("shortmessage".getBytes());
        c1.skipEntries(1, IndividualDeletedEntries.Exclude);
        ml.close();
        assertTrue(ml.getLedgersInfoAsList().size() > 1);
        assertTrue(ml.getTotalSize() > "shortmessage".getBytes().length);
    }

    @Test
    public void testRetentionSize() throws Exception {
        final int retentionSizeInMB = 5;
        final int totalMessage = 10;

        // message size is 1MB
        final int messageSize = 1048576;
        char[] data = new char[messageSize];
        Arrays.fill(data, 'a');
        byte [] message = new String(data).getBytes(Encoding);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setRetentionSizeInMB(retentionSizeInMB);
        config.setMaxEntriesPerLedger(1);
        config.setRetentionTime(1, TimeUnit.HOURS);


        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("retention_size_ledger", config);
        ManagedCursor c1 = ml.openCursor("c1");
        Position position = null;
        for (int i = 0; i < totalMessage; i++) {
            position = ml.addEntry(message);
        }
        // all ledgers are not delete yet since no entry has been acked for c1
        assertEquals(ml.getLedgersInfoAsList().size(), totalMessage);

        List<Entry> entryList = c1.readEntries(totalMessage);
        if (null != position) {
            c1.markDelete(position);
        }
        entryList.forEach(entry -> {
            log.info("Read entry position {}:{}", entry.getLedgerId(), entry.getEntryId());
            entry.release();
        });

        Awaitility.await().untilAsserted(() -> {
            assertTrue(ml.getTotalSize() <= retentionSizeInMB * 1024 * 1024);
            assertEquals(ml.getLedgersInfoAsList().size(), 6);
        });
    }

    @Test
    public void testTimestampOnWorkingLedger() throws Exception {
        ManagedLedgerConfig conf = new ManagedLedgerConfig();
        initManagedLedgerConfig(conf);
        conf.setMaxEntriesPerLedger(1);
        conf.setRetentionSizeInMB(10);
        conf.setRetentionTime(1, TimeUnit.HOURS);

        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("my_test_ledger", conf);
        ml.openCursor("c1");
        ml.addEntry("msg1".getBytes());
        Iterator<LedgerInfo> iter = ml.getLedgersInfoAsList().iterator();
        long ts = -1;
        while (iter.hasNext()) {
            LedgerInfo i = iter.next();
            if (iter.hasNext()) {
                assertTrue(ts <= i.getTimestamp(), i.toString());
                ts = i.getTimestamp();
            } else {
                // the last timestamp can be
                // 0 if it is still opened
                // >0 if it is closed after the addEntry see OpAddEntry#addComplete()
                assertTrue(i.getTimestamp() == 0 || ts <= i.getTimestamp(), i.toString());
            }
        }

        ml.addEntry("msg02".getBytes());

        // reopen a new ml2
        ml.close();
        ManagedLedgerImpl ml2 = (ManagedLedgerImpl) factory.open("my_test_ledger", conf);

        List<LedgerInfo> ledgers = ml2.getLedgersInfoAsList();
        // after reopen ledgers will be 2 + 1(new open, not contain any entries)
        assertEquals(ledgers.size(), 3);

        // the last closed ledger should be the penultimate one.
        LedgerInfo lastClosedLeger = ledgers.get(ledgers.size() - 2);
        assertTrue(lastClosedLeger.getTimestamp() > 0, "well closed LedgerInfo should set a timestamp > 0");
        ml2.close();
    }

    @Test
    public void testBackwardCompatiblityForMeta() throws Exception {
        final ManagedLedgerInfo[] storedMLInfo = new ManagedLedgerInfo[3];
        final Stat[] versions = new Stat[1];

        ManagedLedgerConfig conf = new ManagedLedgerConfig();
        initManagedLedgerConfig(conf);
        conf.setMaxEntriesPerLedger(1);
        conf.setRetentionSizeInMB(10);
        conf.setRetentionTime(1, TimeUnit.HOURS);

        ManagedLedger ml = factory.open("backward_test_ledger", conf);
        ml.openCursor("c1");
        ml.addEntry("msg1".getBytes());
        ml.addEntry("msg2".getBytes());
        ml.close();

        MetaStore store = new MetaStoreImpl(metadataStore, executor);
        CountDownLatch l1 = new CountDownLatch(1);

        // obtain the ledger info
        store.getManagedLedgerInfo("backward_test_ledger", false, new MetaStoreCallback<ManagedLedgerInfo>() {
            @Override
            public void operationComplete(ManagedLedgerInfo result, Stat version) {
                storedMLInfo[0] = result;
                versions[0] = version;
                l1.countDown();
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                fail("on get ManagedLedgerInfo backward_test_ledger");
            }
        });

        l1.await();
        ManagedLedgerInfo.Builder builder1 = ManagedLedgerInfo.newBuilder();

        // simulate test for old ledger with no timestampl
        for (LedgerInfo info : storedMLInfo[0].getLedgerInfoList()) {
            LedgerInfo noTimestamp = ManagedLedgerInfo.LedgerInfo.newBuilder().mergeFrom(info).clearTimestamp().build();
            assertFalse(noTimestamp.hasTimestamp(), "expected old version info with no timestamp");
            builder1.addLedgerInfo(noTimestamp);

        }
        storedMLInfo[1] = builder1.build();

        // test timestamp on new ledger

        CountDownLatch l2 = new CountDownLatch(1);
        store.asyncUpdateLedgerIds("backward_test_ledger", storedMLInfo[1], versions[0], new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat version) {
                l2.countDown();
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                fail("on asyncUpdateLedgerIds");
            }
        });

        // verify that after update ledgers have timestamp

        ManagedLedgerImpl newVersionLedger = (ManagedLedgerImpl) factory.open("backward_test_ledger", conf);
        List<LedgerInfo> mlInfo = newVersionLedger.getLedgersInfoAsList();

        assertTrue(mlInfo.stream().allMatch(ledgerInfo -> ledgerInfo.hasTimestamp()));
    }



    @Test
    public void testGetPositionAfterN() throws Exception {
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        initManagedLedgerConfig(managedLedgerConfig);
        managedLedgerConfig.setMaxEntriesPerLedger(5);
        ManagedLedgerImpl managedLedger =
                (ManagedLedgerImpl) factory.open("testGetPositionAfterN", managedLedgerConfig);

        // open cursor to prevent ledger to be deleted when ledger rollover
        ManagedCursorImpl managedCursor = (ManagedCursorImpl) managedLedger.openCursor("cursor");
        Position positionMarkDelete = null;
        for (int i = 0; i < 10; i++) {
            if (i == 3) {
                positionMarkDelete = managedLedger.addEntry(("entry-" + i).getBytes(Encoding));
                continue;
            }
            managedLedger.addEntry(("entry-" + i).getBytes(Encoding));
        }

        managedCursor.markDelete(positionMarkDelete);

        //trigger ledger rollover and wait for the new ledger created
        Field stateUpdater = ManagedLedgerImpl.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(managedLedger, ManagedLedgerImpl.State.LedgerOpened);
        managedLedger.rollCurrentLedgerIfFull();
        Awaitility.await().untilAsserted(() -> assertEquals(managedLedger.getLedgersInfo().size(), 3));
        assertEquals(5, managedLedger.getLedgersInfoAsList().get(0).getEntries());
        assertEquals(5, managedLedger.getLedgersInfoAsList().get(1).getEntries());
        assertEquals(0, managedLedger.getLedgersInfoAsList().get(2).getEntries());
        log.info("### ledgers {}", managedLedger.getLedgersInfo());

        long firstLedger = managedLedger.getLedgersInfo().firstKey();
        long secondLedger = managedLedger.getLedgersInfoAsList().get(1).getLedgerId();

        Position startPosition = PositionFactory.create(firstLedger, 0);

        Position targetPosition = managedLedger.getPositionAfterN(startPosition, 1, PositionBound.startExcluded);
        assertEquals(targetPosition.getLedgerId(), firstLedger);
        assertEquals(targetPosition.getEntryId(), 1);

        targetPosition = managedLedger.getPositionAfterN(startPosition, 4, PositionBound.startExcluded);
        assertEquals(targetPosition.getLedgerId(), firstLedger);
        assertEquals(targetPosition.getEntryId(), 4);

        // test for expiry situation
        Position searchPosition = managedLedger.getNextValidPosition(managedCursor.getMarkDeletedPosition());
        long length = managedCursor.getNumberOfEntriesInStorage();
        // return the last confirm entry position if searchPosition is exceed the last confirm entry
        targetPosition = managedLedger.getPositionAfterN(searchPosition, length, PositionBound.startExcluded);
        log.info("Target position is {}", targetPosition);
        assertEquals(targetPosition.getLedgerId(), secondLedger);
        assertEquals(targetPosition.getEntryId(), 4);

        // test for n > NumberOfEntriesInStorage
        searchPosition = PositionFactory.create(secondLedger, 0);
        targetPosition = managedLedger.getPositionAfterN(searchPosition, 100, PositionBound.startIncluded);
        assertEquals(targetPosition.getLedgerId(), secondLedger);
        assertEquals(targetPosition.getEntryId(), 4);

        // test for startPosition > current ledger
        searchPosition = PositionFactory.create(999, 0);
        targetPosition = managedLedger.getPositionAfterN(searchPosition, 0, PositionBound.startIncluded);
        assertEquals(targetPosition.getLedgerId(), secondLedger);
        assertEquals(targetPosition.getEntryId(), 4);

        searchPosition = PositionFactory.create(999, 0);
        targetPosition = managedLedger.getPositionAfterN(searchPosition, 10, PositionBound.startExcluded);
        assertEquals(targetPosition.getLedgerId(), secondLedger);
        assertEquals(targetPosition.getEntryId(), 4);
    }

    @Test
    public void testGetNumberOfEntriesInStorage() throws Exception {
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        initManagedLedgerConfig(managedLedgerConfig);
        managedLedgerConfig.setMaxEntriesPerLedger(5);
        ManagedLedgerImpl managedLedger =
                (ManagedLedgerImpl) factory.open("testGetNumberOfEntriesInStorage", managedLedgerConfig);
        // open cursor to prevent ledger to be deleted when ledger rollover
        ManagedCursorImpl managedCursor = (ManagedCursorImpl) managedLedger.openCursor("cursor");
        int numberOfEntries = 10;
        for (int i = 0; i < numberOfEntries; i++) {
            managedLedger.addEntry(("entry-" + i).getBytes(Encoding));
        }

        // trigger ledger rollover and wait for the new ledger created
        Awaitility.await().untilAsserted(() -> {
           assertEquals("LedgerOpened", WhiteboxImpl.getInternalState(managedLedger, "state").toString());
        });
        managedLedger.rollCurrentLedgerIfFull();
        Awaitility.await().untilAsserted(() -> {
            assertEquals(managedLedger.getLedgersInfo().size(), 3);
            assertEquals(managedLedger.getState(), ManagedLedgerImpl.State.LedgerOpened);
        });
        assertEquals(5, managedLedger.getLedgersInfoAsList().get(0).getEntries());
        assertEquals(5, managedLedger.getLedgersInfoAsList().get(1).getEntries());
        log.info("### ledgers {}", managedLedger.getLedgersInfo());
        long length = managedCursor.getNumberOfEntriesInStorage();
        assertEquals(length, numberOfEntries);
    }

    @Test
    public void testEstimatedBacklogSize() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testEstimatedBacklogSize");
        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry(new byte[1024]);
        Position position2 = ledger.addEntry(new byte[1024]);
        ledger.addEntry(new byte[1024]);
        ledger.addEntry(new byte[1024]);
        Position lastPosition = ledger.addEntry(new byte[1024]);

        long backlog = ledger.getEstimatedBacklogSize();
        assertEquals(backlog, 1024 * 5);

        List<Entry> entries = c1.readEntries(2);
        entries.forEach(Entry::release);
        c1.markDelete(position2);

        backlog = ledger.getEstimatedBacklogSize();
        assertEquals(backlog, 1024 * 3);

        entries = c1.readEntries(3);
        entries.forEach(Entry::release);
        c1.markDelete(lastPosition);

        backlog = ledger.getEstimatedBacklogSize();
        assertEquals(backlog, 0);
    }

    @Test
    public void testGetNextValidPosition() throws Exception {
        ManagedLedgerConfig conf = new ManagedLedgerConfig();
        initManagedLedgerConfig(conf);
        conf.setMaxEntriesPerLedger(1);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testGetNextValidPosition", conf);
        ManagedCursor c1 = ledger.openCursor("c1");

        Position p1 = ledger.addEntry("entry1".getBytes());
        Position p2 = ledger.addEntry("entry2".getBytes());
        Position p3 = ledger.addEntry("entry3".getBytes());

        assertEquals(ledger.getNextValidPosition(c1.getMarkDeletedPosition()), p1);
        assertEquals(ledger.getNextValidPosition(p1), p2);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(ledger.getNextValidPosition(p3), PositionFactory.create(p3.getLedgerId() + 1, 0));
        });
        Awaitility.await().untilAsserted(() -> {
            assertEquals(ledger.getNextValidPosition(PositionFactory.create(p3.getLedgerId(), p3.getEntryId() + 1)),
                    PositionFactory.create(p3.getLedgerId() + 1, 0));
        });
        Awaitility.await().untilAsserted(() -> {
            assertEquals(ledger.getNextValidPosition(PositionFactory.create(p3.getLedgerId() + 1, p3.getEntryId() + 1)),
                    PositionFactory.create(p3.getLedgerId() + 1, 0));
        });
    }

    /**
     * Validations:
     *
     * 1. openCursor : activates cursor 2. EntryCache keeps entries: till entry will be read by all active cursors a.
     * active cursor1 reads entry b. EntryCache keeps entry till cursor2 reads c. active cursor2 reads entry d.
     * EntryCache deletes all read entries by cursor1 and cursor2 3. EntryCache discard entries: deactivate slower
     * cursor a. active cursor1 read all entries b. EntryCache keeps entry till cursor2 reads c. deactivate cursor2 d.
     * EntryCache deletes all read entries by cursor1
     *
     * @throws Exception
     */
    @Test
    public void testActiveDeactiveCursorWithDiscardEntriesFromCache() throws Exception {
        ManagedLedgerFactoryConfig conf = new ManagedLedgerFactoryConfig();
        conf.setCacheEvictionIntervalMs(10000);
        conf.setCacheEvictionTimeThresholdMillis(10000);
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        initManagedLedgerConfig(managedLedgerConfig);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, conf, managedLedgerConfig);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("cache_eviction_ledger");

        // Open Cursor also adds cursor into activeCursor-container
        ManagedCursor cursor1 = ledger.openCursor("c1");
        ManagedCursor cursor2 = ledger.openCursor("c2");
        Set<ManagedCursor> activeCursors = new HashSet<>();
        activeCursors.add(cursor1);
        activeCursors.add(cursor2);
        EntryCache entryCache = ledger.entryCache;

        Iterator<ManagedCursor> activeCursor = ledger.getActiveCursors().iterator();

        // (1) validate cursors are part of activeCursorContainer
        activeCursors.remove(activeCursor.next());
        activeCursors.remove(activeCursor.next());
        assertTrue(activeCursors.isEmpty());
        assertFalse(activeCursor.hasNext());

        final int totalInsertedEntries = 50;
        for (int i = 0; i < totalInsertedEntries; i++) {
            String content = "entry"; // 5 bytes
            ledger.addEntry(content.getBytes());
        }

        // (2) Validate: as ledger has active cursors: all entries have been cached
        assertEquals((5 * totalInsertedEntries), entryCache.getSize());

        // read 20 entries
        final int readEntries = 20;
        List<Entry> entries1 = cursor1.readEntries(readEntries);
        // Acknowledge only on last entry
        cursor1.markDelete(entries1.get(entries1.size() - 1).getPosition());
        for (Entry entry : entries1) {
            log.info("Read entry. Position={} Content='{}'", entry.getPosition(), new String(entry.getData()));
            entry.release();
        }

        ledger.waitForPendingCacheEvictions();

        List<Entry> entries2 = cursor2.readEntries(readEntries);
        // Acknowledge only on last entry
        cursor2.markDelete((entries2.get(entries2.size() - 1)).getPosition());
        for (Entry entry : entries2) {
            log.info("Read entry. Position={} Content='{}'", entry.getPosition(), new String(entry.getData()));
            entry.release();
        }

        ledger.waitForPendingCacheEvictions();

        // (3) Validate: cache should remove all entries read by both active cursors
        log.info("expected, found : {}, {}", 5 * (totalInsertedEntries - readEntries), entryCache.getSize());
        assertEquals(entryCache.getSize(), 5 * (totalInsertedEntries - readEntries));

        final int remainingEntries = totalInsertedEntries - readEntries;
        entries1 = cursor1.readEntries(remainingEntries);
        // Acknowledge only on last entry
        cursor1.markDelete(entries1.get(entries1.size() - 1).getPosition());

        for (Entry entry : entries1) {
            log.info("Read entry. Position={} Content='{}'", entry.getPosition(), new String(entry.getData()));
            entry.release();
        }

        ledger.waitForPendingCacheEvictions();

        // (4) Validate: cursor2 is active cursor and has not read these entries yet: so, cache should not remove these
        // entries
        assertEquals(entryCache.getSize(), 5 * (totalInsertedEntries - readEntries));

        ledger.deactivateCursor(cursor1);
        ledger.deactivateCursor(cursor2);

        ledger.waitForPendingCacheEvictions();

        // (5) Validate: cursor2 is not active cursor now: cache should have removed all entries read by active cursor1
        assertEquals(entryCache.getSize(), 0);

        log.info("Finished reading entries");

        ledger.close();
    }

    @Test
    public void testActiveDeactiveCursor() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("cache_eviction_ledger");
        EntryCache entryCache = ledger.entryCache;

        final int totalInsertedEntries = 20;
        for (int i = 0; i < totalInsertedEntries; i++) {
            String content = "entry"; // 5 bytes
            ledger.addEntry(content.getBytes());
        }

        // (1) Validate: cache not stores entries as no active cursor
        assertEquals(entryCache.getSize(), 0);

        // Open Cursor also adds cursor into activeCursor-container
        ManagedCursor cursor1 = ledger.openCursor("c1");
        ManagedCursor cursor2 = ledger.openCursor("c2");
        ledger.deactivateCursor(cursor2);

        for (int i = 0; i < totalInsertedEntries; i++) {
            String content = "entry"; // 5 bytes
            ledger.addEntry(content.getBytes());
        }

        // (2) Validate: cache stores entries as active cursor has not read message
        assertEquals(entryCache.getSize(), 5 * totalInsertedEntries);

        // read 20 entries
        List<Entry> entries1 = cursor1.readEntries(totalInsertedEntries);
        for (Entry entry : entries1) {
            log.info("Read entry. Position={} Content='{}'", entry.getPosition(), new String(entry.getData()));
            entry.release();
        }

        // (3) Validate: cache discards all entries after all cursors are deactivated
        ledger.deactivateCursor(cursor1);
        assertEquals(entryCache.getSize(), 0);

        ledger.close();
    }

    @Test
    public void testCursorRecoveryForEmptyLedgers() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testCursorRecoveryForEmptyLedgers");
        ManagedCursor c1 = ledger.openCursor("c1");

        assertEquals(ledger.getLedgersInfoAsList().size(), 1);
        assertEquals(c1.getMarkDeletedPosition(), ledger.lastConfirmedEntry);

        c1.close();
        ledger.close();

        ledger = (ManagedLedgerImpl) factory.open("testCursorRecoveryForEmptyLedgers");
        c1 = ledger.openCursor("c1");

        assertEquals(ledger.getLedgersInfoAsList().size(), 1);
        assertEquals(c1.getMarkDeletedPosition(), ledger.lastConfirmedEntry);
    }

    @Test
    public void testLazyRecoverCursor() throws Exception {
        ManagedLedger ledger = factory.open("testLedger");
        ManagedCursor cursor = ledger.openCursor("testCursor");

        ledger.addEntry("entry-1".getBytes());
        Position p1 = ledger.addEntry("entry-2".getBytes());
        cursor.markDelete(p1);

        // Re-open from a different factory trigger recovery.
        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);

        // Simulating time consuming cursor recovery.
        CompletableFuture<Void> future = bkc.promiseAfter(2);
        @Cleanup("shutdownNow")
        ScheduledExecutorService scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("lazyCursorRecovery"));
        scheduledExecutorService.schedule(() -> {
            future.complete(null);
        }, 10, TimeUnit.SECONDS);

        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        initManagedLedgerConfig(managedLedgerConfig);
        managedLedgerConfig.setLazyCursorRecovery(true);
        Long startLedgerRecovery = System.currentTimeMillis();

        // Check ledger recovered before time consuming cursor recovery complete.
        ledger = factory2.open("testLedger", managedLedgerConfig);
        assertTrue(System.currentTimeMillis() - startLedgerRecovery < 5000);

        // Check cursor recovered successfully.
        cursor = ledger.openCursor("testCursor");
        assertEquals(cursor.getMarkDeletedPosition(), p1);
    }

    @Test
    public void testConcurrentOpenCursor() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testConcurrentOpenCursor");

        final AtomicReference<ManagedCursor> cursor1 = new AtomicReference<>(null);
        final AtomicReference<ManagedCursor> cursor2 = new AtomicReference<>(null);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch latch = new CountDownLatch(2);

        cachedExecutor.execute(() -> {
            try {
                barrier.await();
            } catch (Exception e) {
            }
            ledger.asyncOpenCursor("c1", new OpenCursorCallback() {

                @Override
                public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                    latch.countDown();
                }

                @Override
                public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                    cursor1.set(cursor);
                    latch.countDown();
                }
            }, null);
        });

        cachedExecutor.execute(() -> {
            try {
                barrier.await();
            } catch (Exception e) {
            }
            ledger.asyncOpenCursor("c1", new OpenCursorCallback() {

                @Override
                public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                    latch.countDown();
                }

                @Override
                public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                    cursor2.set(cursor);
                    latch.countDown();
                }
            }, null);
        });

        latch.await();
        assertNotNull(cursor1.get());
        assertNotNull(cursor2.get());
        assertEquals(cursor1.get(), cursor2.get());

        ledger.close();
    }

    @Test
    public void testConcurrentOpenCursorShouldNotHaveConcurrentAccessOfUninitializedCursors() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("ConcurrentAccessOfUninitializedCursors");

        final CompletableFuture<ManagedCursor> cursorFuture = new CompletableFuture<>();
        final CompletableFuture<Void> removingFuture = new CompletableFuture<>();
        final CompletableFuture<Void> concurrentAccessFuture = new CompletableFuture<>();
        final Throwable concurrentAccessTimeout = new TimeoutException();

        cachedExecutor.execute(() -> {
            removingFuture.join();
            CompletableFuture<Void> lockingFuture = new CompletableFuture<>();
            cachedExecutor.execute(() -> {
                try {
                    lockingFuture.join();

                    // Gives `synchronized (ledger)` a chance to complete if it got lock immediately.
                    Thread.sleep(2);

                    // Normally, following code will process after success or failure contention of
                    // `synchronized (ledger)`. Theoretically, it is possible that following code
                    // complete before contention of `synchronized (ledger)` block, but it is rare
                    // in practice, and it is not harmful as it produces only false positive cases.
                    concurrentAccessFuture.completeExceptionally(concurrentAccessTimeout);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            });
            lockingFuture.complete(null);
            synchronized (ledger) {
                concurrentAccessFuture.complete(null);
            }
        });

        Map<String, CompletableFuture<ManagedCursor>> uninitializedCursors = ledger.uninitializedCursors;
        Map<String, CompletableFuture<ManagedCursor>> spyUninitializedCursors = spy(uninitializedCursors);
        doAnswer(mock -> {
            removingFuture.complete(null);
            try {
                // Access of uninitializedCursors should guarded by synchronized(ledger),
                // so there are must be no concurrent accesses in this scope. If we get this
                // future successfully, then there is a concurrent access.
                concurrentAccessFuture.get();
                Throwable throwable = new IllegalStateException("Detecting concurrent access of uninitializedCursors");
                cursorFuture.completeExceptionally(throwable);
            } catch (Exception ex) {
                assertSame(ExceptionUtils.getRootCause(ex), concurrentAccessTimeout);
            }
            return mock.callRealMethod();
        }).when(spyUninitializedCursors).remove(anyString());
        setFieldValue(ManagedLedgerImpl.class, ledger, "uninitializedCursors", spyUninitializedCursors);

        cachedExecutor.execute(() -> {
            try {
                ledger.asyncOpenCursor("c1", new OpenCursorCallback() {
                    @Override
                    public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                        cursorFuture.completeExceptionally(exception);
                    }

                    @Override
                    public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                        cursorFuture.complete(cursor);
                    }
                }, null);
            } catch (Exception e) {
                cursorFuture.completeExceptionally(e);
            }
        });

        try {
            ManagedCursor cursor = cursorFuture.get();
            assertNotNull(cursor);
        } catch (Exception ex) {
            fail(ExceptionUtils.getRootCauseMessage(ex));
        } finally {
            ledger.close();
        }
    }

    @Test
    public void testConsumerSubscriptionInitializePosition() throws Exception{
        final int maxEntryPerLedger = 2;
        ManagedLedgerConfig config =
                initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(maxEntryPerLedger);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("lastest_earliest_ledger", config);

        final int totalInsertedEntries = 20;
        for (int i = 0; i < totalInsertedEntries; i++) {
            String content = "entry" + i; // 5 bytes
            ledger.addEntry(content.getBytes());
        }
        Awaitility.await().untilAsserted(() -> {
            assertEquals(ledger.currentLedgerSize, 0);
            assertEquals(ledger.ledgers.size(), 1);
        });
        // Open Cursor also adds cursor into activeCursor-container
        ManagedCursor latestCursor = ledger.openCursor("c1", InitialPosition.Latest);
        ManagedCursor earliestCursor = ledger.openCursor("c2", InitialPosition.Earliest);

        // Since getReadPosition returns the next position, we decrease the entryId by 1
        Position p2 = earliestCursor.getReadPosition();

        Pair<Position, Long> latestPositionAndCounter = ledger.getLastPositionAndCounter();
        Pair<Position, Long> earliestPositionAndCounter = ledger.getFirstPositionAndCounter();
        // The read position is the valid next position of the last position instead of the next position.
        assertEquals(ledger.getNextValidPosition(latestPositionAndCounter.getLeft()), latestCursor.getReadPosition());
        assertEquals(ledger.getNextValidPosition(earliestPositionAndCounter.getLeft()), p2);

        assertEquals(latestPositionAndCounter.getRight().longValue(), totalInsertedEntries);
        assertEquals(earliestPositionAndCounter.getRight().longValue(),
                totalInsertedEntries - earliestCursor.getNumberOfEntriesInBacklog(false));

        ledger.close();

    }

    @Test
    public void testManagedLedgerAutoCreate() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(new ManagedLedgerConfig()).setCreateIfMissing(true);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("test", config);
        assertNotNull(ledger);
    }

    @Test
    public void testManagedLedgerWithoutAutoCreate() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(new ManagedLedgerConfig()).setCreateIfMissing(false);

        try {
            factory.open("testManagedLedgerWithoutAutoCreate", config);
            fail("should have thrown ManagedLedgerNotFoundException");
        } catch (ManagedLedgerNotFoundException e) {
            // Expected
        }

        assertFalse(factory.getManagedLedgers().containsKey("testManagedLedgerWithoutAutoCreate"));
    }

    @Test
    public void testManagedLedgerWithCreateLedgerTimeOut() throws Exception {
        ManagedLedgerConfig config =
                initManagedLedgerConfig(new ManagedLedgerConfig()).setMetadataOperationsTimeoutSeconds(3);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("timeout_ledger_test", config);

        BookKeeper bk = mock(BookKeeper.class);
        doNothing().when(bk).asyncCreateLedger(anyInt(), anyInt(), anyInt(), any(), any(), any(), any(), any());
        AtomicInteger response = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Object> ctxHolder = new AtomicReference<>();
        ledger.asyncCreateLedger(bk, config, null, (rc, lh, ctx) -> {
            response.set(rc);
            latch.countDown();
            ctxHolder.set(ctx);
        }, Collections.emptyMap());

        latch.await(config.getMetadataOperationsTimeoutSeconds() + 2, TimeUnit.SECONDS);
        assertEquals(response.get(), BKException.Code.TimeoutException);
        assertTrue(ctxHolder.get() instanceof CompletableFuture);
        CompletableFuture ledgerCreateHook = (CompletableFuture) ctxHolder.get();
        assertTrue(ledgerCreateHook.isCompletedExceptionally());

        ledger.close();
    }

    /**
     * It verifies that asyncRead timesout if it doesn't receive response from bk-client in configured timeout.
     *
     * @throws Exception
     */
    @Test
    public void testManagedLedgerWithReadEntryTimeOut() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(new ManagedLedgerConfig()).setReadEntryTimeoutSeconds(1);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("timeout_ledger_test", config);

        Position position = ledger.addEntry("entry-1".getBytes());

        // ensure that the read isn't cached
        factory.getEntryCacheManager().clear();

        bkc.setReadHandleInterceptor(new PulsarMockReadHandleInterceptor() {
            @Override
            public CompletableFuture<LedgerEntries> interceptReadAsync(long ledgerId, long firstEntry, long lastEntry,
                                                                       LedgerEntries entries) {
                return CompletableFuture.supplyAsync(() -> {
                    return entries;
                }, CompletableFuture.delayedExecutor(3, TimeUnit.SECONDS));
            }
        });

        AtomicReference<ManagedLedgerException> responseException1 = new AtomicReference<>();
        String ctxStr = "timeoutCtx";

        // (1) test read-timeout for: ManagedLedger.asyncReadEntry(..)
        ledger.asyncReadEntry(position, new ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                responseException1.set(null);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                assertEquals(ctxStr, (String) ctx);
                responseException1.set(exception);
            }
        }, ctxStr);

        Awaitility.await().untilAsserted(() -> {
            assertNotNull(responseException1.get());
            assertTrue(responseException1.get().getMessage()
                    .startsWith(BKException.getMessage(BKException.Code.TimeoutException)));
        });

        // ensure that the read isn't cached
        factory.getEntryCacheManager().clear();

        // (2) test read-timeout for: ManagedCursor.asyncReadEntries(..)
        AtomicReference<ManagedLedgerException> responseException2 = new AtomicReference<>();
        ManagedCursor cursor = ledger.openCursor("cursor1", InitialPosition.Earliest);
        cursor.asyncReadEntries(1, new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                assertEquals(ctxStr, (String) ctx);
                responseException2.set(exception);
            }
        }, ctxStr, PositionFactory.LATEST);

        Awaitility.await().untilAsserted(() -> {
            assertNotNull(responseException2.get());
            assertTrue(responseException2.get().getMessage()
                    .startsWith(BKException.getMessage(BKException.Code.TimeoutException)));
        });

        ledger.close();
    }

    @Test
    public void testAddEntryResponseTimeout() throws Exception {
        // Create ML with feature Add Entry Timeout Check.
        final ManagedLedgerConfig config =
                initManagedLedgerConfig(new ManagedLedgerConfig()).setAddEntryTimeoutSeconds(2);
        final ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("ml1", config);
        final ManagedCursor cursor = ledger.openCursor("c1");
        final CollectCtxAddEntryCallback collectCtxAddEntryCallback = new CollectCtxAddEntryCallback();

        // Insert a response delay.
        bkc.addEntryResponseDelay(8, TimeUnit.SECONDS);

        // Add two entries.
        final byte[] msg1 = new byte[]{1};
        final byte[] msg2 = new byte[]{2};
        int ctx1 = 1;
        int ctx2 = 2;
        ledger.asyncAddEntry(msg1, collectCtxAddEntryCallback, ctx1);
        ledger.asyncAddEntry(msg2, collectCtxAddEntryCallback, ctx2);
        // Verify all write requests are completed.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(collectCtxAddEntryCallback.addCompleteCtxList, Arrays.asList(1, 2));
        });
        Entry entry1 = cursor.readEntries(1).get(0);
        assertEquals(entry1.getData(), msg1);
        entry1.release();
        Entry entry2 = cursor.readEntries(1).get(0);
        assertEquals(entry2.getData(), msg2);
        entry2.release();

        // cleanup.
        factory.delete(ledger.name);
    }

    private static class CollectCtxAddEntryCallback implements AddEntryCallback {

        public List<Object> addCompleteCtxList = new BlockingArrayQueue<>();
        public List<Object> addFailedCtxList = new BlockingArrayQueue<>();

        @Override
        public void addComplete(Position position, ByteBuf entryData, Object ctx) {
            addCompleteCtxList.add(ctx);
        }

        @Override
        public void addFailed(ManagedLedgerException exception, Object ctx) {
            addFailedCtxList.add(ctx);
        }
    }

    /**
     * It verifies that if bk-client doesn't complete the add-entry in given time out then broker is resilient enough
     * to create new ledger and add entry successfully.
     *
     *
     * @throws Exception
     */
    @Test(timeOut = 20000)
    public void testManagedLedgerWithAddEntryTimeOut() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(new ManagedLedgerConfig()).setAddEntryTimeoutSeconds(1);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("timeout_ledger_test", config);

        BookKeeper bk = mock(BookKeeper.class);
        doNothing().when(bk).asyncCreateLedger(anyInt(), anyInt(), anyInt(), any(), any(), any(), any(), any());

        PulsarMockBookKeeper bkClient = mock(PulsarMockBookKeeper.class);
        ClientConfiguration conf = new ClientConfiguration();
        doReturn(conf).when(bkClient).getConf();
        class MockLedgerHandle extends PulsarMockLedgerHandle {
            public MockLedgerHandle(PulsarMockBookKeeper bk, long id, DigestType digest, byte[] passwd)
                    throws GeneralSecurityException {
                super(bk, id, digest, passwd);
            }

            @Override
            public void asyncAddEntry(final byte[] data, final AddCallback cb, final Object ctx) {
                // do nothing
            }

            @Override
            public void asyncClose(org.apache.bookkeeper.client.AsyncCallback.CloseCallback cb, Object ctx) {
                cb.closeComplete(BKException.Code.OK, this, ctx);
            }
        }
        MockLedgerHandle ledgerHandle = mock(MockLedgerHandle.class);
        final String data = "data";
        doNothing().when(ledgerHandle).asyncAddEntry(data.getBytes(), null, null);
        AtomicBoolean addSuccess = new AtomicBoolean();

        setFieldValue(ManagedLedgerImpl.class, ledger, "currentLedger", ledgerHandle);

        final int totalAddEntries = 1;
        CountDownLatch latch = new CountDownLatch(totalAddEntries);
        ledger.asyncAddEntry(data.getBytes(), new AddEntryCallback() {

            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                addSuccess.set(true);
                latch.countDown();
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                latch.countDown();
            }
        }, null);

        latch.await();

        assertTrue(addSuccess.get());

        setFieldValue(ManagedLedgerImpl.class, ledger, "currentLedger", null);
    }

    @Test
    public void testAddOpCountWithMessageAdd() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");

        for (int i = 0; i < 10; i++) {
            OpAddEntry op = OpAddEntry.createNoRetainBuffer(ledger,
                    ByteBufAllocator.DEFAULT.buffer(128), null, null, new AtomicBoolean());
            ledger.internalAsyncAddEntry(op);
            long addOpCount = ManagedLedgerImpl.ADD_OP_COUNT_UPDATER.get(ledger);
            Assert.assertEquals(i + 1, addOpCount);
        }
    }


    @Test
    public void avoidUseSameOpAddEntryBetweenDifferentLedger() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");

        List<OpAddEntry> oldOps = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            OpAddEntry op = OpAddEntry.createNoRetainBuffer(ledger,
                    ByteBufAllocator.DEFAULT.buffer(128), null, null, new AtomicBoolean());
            if (i > 4) {
                op.setLedger(mock(LedgerHandle.class));
            }
            oldOps.add(op);
            ledger.pendingAddEntries.add(op);
        }

        ledger.createNewOpAddEntryForNewLedger();
        for (int i = 0; i < 10; i++) {
            OpAddEntry oldOp = oldOps.get(i);
            OpAddEntry newOp = ledger.pendingAddEntries.poll();
            if (i > 4) {
                Assert.assertNotSame(oldOp, newOp);
            } else {
                Assert.assertSame(oldOp, newOp);
            }
            oldOp.getData().release();
        }
    }

    /**
     * It verifies that managed-cursor can recover metadata-version if it fails to update due to version conflict. This
     * test verifies that version recovery happens if checkOwnership supplier is passed while creating managed-ledger.
     *
     * @param checkOwnershipFlag
     * @throws Exception
     */
    @Test(dataProvider = "checkOwnershipFlag")
    public void recoverMLWithBadVersion(boolean checkOwnershipFlag) throws Exception {

        ManagedLedgerFactoryConfig conf = new ManagedLedgerFactoryConfig();
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory1 = new ManagedLedgerFactoryImpl(metadataStore, bkc, conf);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc, conf);

        final MutableObject<ManagedLedger> ledger1 = new MutableObject<>(), ledger2 = new MutableObject<>();
        final MutableObject<ManagedCursorImpl> cursor1 = new MutableObject<>(), cursor2 = new MutableObject<>();

        createLedger(factory1, ledger1, cursor1, checkOwnershipFlag);
        ledger1.getValue().addEntry("test1".getBytes(Encoding));
        ledger1.getValue().addEntry("test2".getBytes(Encoding));
        Entry entry = cursor1.getValue().readEntries(1).get(0);
        cursor1.getValue().delete(entry.getPosition());

        createLedger(factory2, ledger2, cursor2, checkOwnershipFlag);
        entry = cursor2.getValue().readEntries(1).get(0);

        // 1. closing cursor will change the zk-version
        cursor1.getValue().close();

        // 2. try to creatCursorLedger which should fail first time because of BadVersionException
        // However, if checkOwnershipFlag is eanbled the managed-cursor will reover from that exception.
        boolean isFailed = updateCusorMetadataByCreatingMetadataLedger(cursor2);
        Assert.assertTrue(isFailed);

        isFailed = updateCusorMetadataByCreatingMetadataLedger(cursor2);
        if (checkOwnershipFlag) {
            Assert.assertFalse(isFailed);
        } else {
            Assert.assertTrue(isFailed);
        }

        log.info("Test completed");
    }

    private boolean updateCusorMetadataByCreatingMetadataLedger(MutableObject<ManagedCursorImpl> cursor2)
            throws InterruptedException {
        MutableObject<Boolean> failed = new MutableObject<>();
        failed.setValue(false);
        CountDownLatch createLedgerDoneLatch = new CountDownLatch(1);
        cursor2.getValue().createNewMetadataLedger(new VoidCallback() {

            @Override
            public void operationComplete() {
                createLedgerDoneLatch.countDown();
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                failed.setValue(true);
                createLedgerDoneLatch.countDown();
            }

        });
        createLedgerDoneLatch.await();
        return failed.getValue();
    }


    @Test
    public void testPropertiesForMeta() throws Exception {
        final String mLName = "properties_test";
        factory.open(mLName);
        MetaStore store = new MetaStoreImpl(metadataStore, executor);

        ManagedLedgerInfo.Builder builder = ManagedLedgerInfo.newBuilder();
        builder.addProperties(MLDataFormats.KeyValue.newBuilder().setKey("key1").setValue("value1").build());
        builder.addProperties(MLDataFormats.KeyValue.newBuilder().setKey("key2").setValue("value2").build());

        CountDownLatch l2 = new CountDownLatch(1);
        store.asyncUpdateLedgerIds(mLName, builder.build(),
                new Stat(mLName, 1, 0, 0, false, true),
                new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat version) {
                l2.countDown();
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                fail("on asyncUpdateLedgerIds");
            }
        });

        // get ManagedLedgerInfo from meta store
        org.apache.bookkeeper.mledger.ManagedLedgerInfo managedLedgerInfo = factory.getManagedLedgerInfo(mLName);
        Map<String, String> properties = managedLedgerInfo.properties;
        assertEquals(properties.get("key1"), "value1");
        assertEquals(properties.get("key2"), "value2");

        factory.shutdown();
        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);

        // reopen managedLedger
        ManagedLedger ml = factory.open(mLName);
        properties = ml.getProperties();
        assertEquals(properties.get("key1"), "value1");
        assertEquals(properties.get("key2"), "value2");
    }

    private void createLedger(ManagedLedgerFactoryImpl factory, MutableObject<ManagedLedger> ledger1,
            MutableObject<ManagedCursorImpl> cursor1, boolean checkOwnershipFlag) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        factory.asyncOpen("my_test_ledger", initManagedLedgerConfig(new ManagedLedgerConfig()),
                new OpenLedgerCallback() {
            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                ledger1.setValue(ledger);
                ledger.asyncOpenCursor("test-cursor", new OpenCursorCallback() {
                    @Override
                    public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                        cursor1.setValue((ManagedCursorImpl) cursor);
                        latch.countDown();
                    }

                    @Override
                    public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                    }
                }, null);
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
            }
        }, checkOwnershipFlag ? () -> CompletableFuture.completedFuture(true) : null, null);
        latch.await();
    }

    @Test
    public void deleteWithoutOpen() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        assertEquals(ledger.getNumberOfEntries(), 1);
        ledger.close();

        factory.delete("my_test_ledger");

        try {
            factory.open("my_test_ledger",
                    initManagedLedgerConfig(new ManagedLedgerConfig()).setCreateIfMissing(false));
            fail("Should have failed");
        } catch (ManagedLedgerNotFoundException e) {
            // Expected
        }
    }

    private abstract class MockedPlacementPolicy implements EnsemblePlacementPolicy{}

    @Test(timeOut = 10000)
    public void testManagedLedgerWithPlacementPolicyInCustomMetadata() throws Exception {
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        initManagedLedgerConfig(managedLedgerConfig);
        managedLedgerConfig.setBookKeeperEnsemblePlacementPolicyClassName(MockedPlacementPolicy.class);
        managedLedgerConfig.setBookKeeperEnsemblePlacementPolicyProperties(Collections.singletonMap("key", "value"));
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", managedLedgerConfig);
        assertFalse(ledger.createdLedgerCustomMetadata.isEmpty());
        byte[] configData =
                ledger.createdLedgerCustomMetadata.get(EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG);
        EnsemblePlacementPolicyConfig config = EnsemblePlacementPolicyConfig.decode(configData);
        assertEquals(config.getPolicyClass().getName(), MockedPlacementPolicy.class.getName());
        assertEquals(config.getProperties().size(), 1);
        assertTrue(config.getProperties().containsKey("key"));
        assertEquals(config.getProperties().get("key"), "value");
    }

    private void setFieldValue(Class clazz, Object classObj, String fieldName, Object fieldValue) throws Exception {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(classObj, fieldValue);
    }

    public static void retryStrategically(Predicate<Void> predicate, int retryCount, long intSleepTimeInMillis)
            throws Exception {
        for (int i = 0; i < retryCount; i++) {
            if (predicate.test(null) || i == (retryCount - 1)) {
                break;
            }
            Thread.sleep(intSleepTimeInMillis + (intSleepTimeInMillis * i));
        }
    }

    @Test
    public void testManagedLedgerRollOverIfFull() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setRetentionTime(1, TimeUnit.SECONDS);
        config.setMaxEntriesPerLedger(2);
        config.setMaximumRolloverTime(500, TimeUnit.MILLISECONDS);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("test_managedLedger_rollOver", config);
        ManagedCursor cursor = ledger.openCursor("c1");

        int msgNum = 10;

        for (int i = 0; i < msgNum; i++) {
            ledger.addEntry(new byte[1024 * 1024]);
        }

        Awaitility.await().untilAsserted(() -> Assert.assertEquals(ledger.getLedgersInfoAsList().size(),
                msgNum / 2 + 1));
        List<Entry> entries = cursor.readEntries(msgNum);
        Assert.assertEquals(msgNum, entries.size());

        for (Entry entry : entries) {
            cursor.markDelete(entry.getPosition());
        }
        entries.forEach(Entry::release);

        // all the messages have benn acknowledged
        // and all the ledgers have been removed except the last ledger
        Field stateUpdater = ManagedLedgerImpl.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        stateUpdater.set(ledger, ManagedLedgerImpl.State.LedgerOpened);
        ledger.rollCurrentLedgerIfFull();
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        ledger.trimConsumedLedgersInBackground(completableFuture);
        completableFuture.get();
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 1));
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(ledger.getTotalSize(), 0));
    }

    @Test
    public void testLedgerReachMaximumRolloverTime() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setMinimumRolloverTime(1, TimeUnit.MILLISECONDS);
        config.setMaximumRolloverTime(1, TimeUnit.SECONDS);

        ManagedLedger ml = factory.open("ledger-reach-maximum-rollover-time", config);
        long firstLedgerId = ml.addEntry("test".getBytes()).getLedgerId();

        // the ledger rollover scheduled time is between 1000 and 1050 ms,
        // wait 1100 ms, the ledger should be rolled over.
        Awaitility.await()
                .until(() -> firstLedgerId != ml.addEntry("test".getBytes()).getLedgerId());
    }

    @Test(groups = "flaky")
    public void testLedgerNotRolloverWithoutOpenState() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setMaxEntriesPerLedger(2);

        ManagedLedgerImpl ml = spy((ManagedLedgerImpl) factory.open("ledger-not-rollover-without-open-state", config));
        ml.addEntry("test1".getBytes()).getLedgerId();
        long ledgerId2 = ml.addEntry("test2".getBytes()).getLedgerId();
        Field stateUpdater = ManagedLedgerImpl.class.getDeclaredField("state");
        stateUpdater.setAccessible(true);
        // Set state to CreatingLedger to avoid rollover
        stateUpdater.set(ml, ManagedLedgerImpl.State.CreatingLedger);
        ml.rollCurrentLedgerIfFull();
        Field currentLedger = ManagedLedgerImpl.class.getDeclaredField("currentLedger");
        currentLedger.setAccessible(true);
        LedgerHandle lh = (LedgerHandle) currentLedger.get(ml);
        Awaitility.await()
                .until(() -> ledgerId2 == lh.getId());
    }

    @Test
    public void testExpiredLedgerDeletionAfterManagedLedgerRestart() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setRetentionTime(1, TimeUnit.SECONDS);
        config.setMaxEntriesPerLedger(2);
        config.setMinimumRolloverTime(1, TimeUnit.MILLISECONDS);
        config.setMaximumRolloverTime(500, TimeUnit.MILLISECONDS);

        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) factory.open("ml_restart_ledger", config);
        ManagedCursor cursor = managedLedger.openCursor("c1");

        for (int i = 0; i < 3; i++) {
            managedLedger.addEntry(new byte[1024 * 1024]);
        }

        // we have 2 ledgers at the beginning [{entries=2}, {entries=1}]
        Assert.assertEquals(managedLedger.getLedgersInfoAsList().size(), 2);
        List<Entry> entries = cursor.readEntries(3);

        // managed-ledger restart
        managedLedger.close();
        managedLedger = (ManagedLedgerImpl) factory.open("ml_restart_ledger", config);

        // then we have one more empty ledger after managed-ledger initialization
        // and now ledgers are [{entries=2}, {entries=1}, {entries=0}]
        Assert.assertTrue(managedLedger.getLedgersInfoAsList().size() >= 2);

        cursor = managedLedger.openCursor("c1");
        for (Entry entry : entries) {
            cursor.markDelete(entry.getPosition());
        }
        entries.forEach(Entry::release);
        // Now we update the cursors that are still subscribing to ledgers that has been consumed completely
        managedLedger.maybeUpdateCursorBeforeTrimmingConsumedLedger();
        managedLedger.internalTrimConsumedLedgers(Futures.NULL_PROMISE);
        ManagedLedgerImpl finalManagedLedger = managedLedger;
        Awaitility.await().untilAsserted(() -> {
            // We only have one empty ledger at last [{entries=0}]
            Assert.assertEquals(finalManagedLedger.getLedgersInfoAsList().size(), 1);
            Assert.assertEquals(finalManagedLedger.getTotalSize(), 0);
        });
    }

    @Test(timeOut = 20000)
    public void testAsyncTruncateLedgerRetention() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setRetentionSizeInMB(50);
        config.setRetentionTime(1, TimeUnit.DAYS);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("truncate_ledger", config);
        ManagedCursor cursor = ledger.openCursor("test-cursor");
        ledger.addEntry("test-entry-1".getBytes(Encoding));
        ledger.addEntry("test-entry-1".getBytes(Encoding));
        ledger.addEntry("test-entry-1".getBytes(Encoding));
        ledger.addEntry("test-entry-1".getBytes(Encoding));
        ledger.addEntry("test-entry-1".getBytes(Encoding));

        ledger.close();
        ManagedLedgerImpl ledger2 = (ManagedLedgerImpl) factory.open("truncate_ledger", config);
        ledger2.addEntry("test-entry-2".getBytes(Encoding));


        CompletableFuture<Void> future = ledger2.asyncTruncate();
        future.get();

        assertTrue(ledger2.getLedgersInfoAsList().size() <= 1);
    }

    @Test(timeOut = 20000)
    public void testAsyncTruncateLedgerSlowestCursor() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("truncate_ledger", config);
        ManagedCursor cursor = ledger.openCursor("test-cursor");
        ManagedCursor cursor2 = ledger.openCursor("test-cursor2");
        ledger.addEntry("test-entry-1".getBytes(Encoding));
        ledger.addEntry("test-entry-1".getBytes(Encoding));
        ledger.addEntry("test-entry-1".getBytes(Encoding));
        ledger.addEntry("test-entry-1".getBytes(Encoding));
        ledger.addEntry("test-entry-1".getBytes(Encoding));

        ledger.close();
        ManagedLedgerImpl ledger2 = (ManagedLedgerImpl) factory.open("truncate_ledger", config);
        ledger2.addEntry("test-entry-2".getBytes(Encoding));
        ManagedCursor cursor3 = ledger2.openCursor("test-cursor");
        cursor3.resetCursor(PositionFactory.create(ledger2.getLastPosition()));

        CompletableFuture<Void> future = ledger2.asyncTruncate();
        future.get();

        assertTrue(ledger2.getLedgersInfoAsList().size() == 1);
    }

    @Test
    public void testOpEntryAdd_toString_doesNotThrowNPE(){
        ManagedLedger ml = mock(ManagedLedger.class);
        LedgerHandle ledger = mock(LedgerHandle.class);
        when(ml.getName()).thenReturn(null);
        when(ledger.getId()).thenReturn(124L);
        long entryId = 12L;
        long startTime = 1245L;
        int dataLength = 566;
        String test = "OpAddEntry{"
                + "mlName=" + ml != null ? ml.getName() : "null"
                + ", ledgerId=" + ledger != null ? String.valueOf(ledger.getId()) : "null"
                + ", entryId=" + entryId
                + ", startTime=" + startTime
                + ", dataLength=" + dataLength
                + '}';
    }

    @Test
    public void testInvalidateReadHandleWhenDeleteLedger() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setMaxEntriesPerLedger(1);

        // Verify the read handle should be invalidated after ledger been removed.
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testInvalidateReadHandleWhenDeleteLedger", config);
        ManagedCursor cursor = ledger.openCursor("test-cursor");
        ManagedCursor cursor2 = ledger.openCursor("test-cursor2");
        final int entries = 3;
        for (int i = 0; i < entries; i++) {
            ledger.addEntry(String.valueOf(i).getBytes(Encoding));
        }

        // clear the cache to avoid flakiness
        factory.getEntryCacheManager().clear();

        List<Entry> entryList = cursor.readEntries(3);
        assertEquals(entryList.size(), 3);
        Awaitility.await().untilAsserted(() -> {
            log.error("ledger.ledgerCache.size() : " + ledger.ledgerCache.size());
            assertEquals(ledger.ledgerCache.size(), 3);
            assertEquals(ledger.ledgers.size(), 4);
        });

        cursor.clearBacklog();
        cursor2.clearBacklog();
        ledger.trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(ledger.ledgers.size(), 1);
            assertEquals(ledger.ledgerCache.size(), 0);
        });

        cursor.close();
        cursor2.close();
        ledger.close();
    }

    @Test
    public void testLockReleaseWhenTrimLedger() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setMaxEntriesPerLedger(1);

        ManagedLedgerImpl ledger = spy((ManagedLedgerImpl) factory.open("testLockReleaseWhenTrimLedger", config));
        doThrow(new ManagedLedgerException.LedgerNotExistException("First non deleted Ledger is not found"))
                .when(ledger).advanceCursorsIfNecessary(any());
        final int entries = 10;
        ManagedCursor cursor = ledger.openCursor("test-cursor" + UUID.randomUUID());
        for (int i = 0; i < entries; i++) {
            ledger.addEntry(String.valueOf(i).getBytes(Encoding));
        }
        List<Entry> entryList = cursor.readEntries(entries);
        assertEquals(entryList.size(), entries);
        assertEquals(ledger.ledgers.size() - 1, entries);
        assertEquals(ledger.ledgerCache.size() - 1, entries - 1);
        cursor.clearBacklog();
        ledger.trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
        ledger.trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
        // Cleanup fails because ManagedLedgerNotFoundException is thrown
        Awaitility.await().untilAsserted(() -> {
            assertEquals(ledger.ledgers.size() - 1, entries);
            assertEquals(ledger.ledgerCache.size() - 1, entries - 1);
        });
        // The lock is released even if an ManagedLedgerNotFoundException occurs, so it can be called repeatedly
        Awaitility.await().untilAsserted(() ->
                verify(ledger, atLeast(2)).advanceCursorsIfNecessary(any()));
        cursor.close();
        ledger.close();
    }

    @Test
    public void testInvalidateReadHandleWhenConsumed() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setMaxEntriesPerLedger(1);
        // Verify the read handle should be invalidated when all cursors consumed
        // even if the ledger can not been removed due to the data retention
        config.setRetentionSizeInMB(50);
        config.setRetentionTime(1, TimeUnit.DAYS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testInvalidateReadHandleWhenConsumed", config);
        ManagedCursor cursor = ledger.openCursor("test-cursor");
        ManagedCursor cursor2 = ledger.openCursor("test-cursor2");
        final int entries = 3;
        for (int i = 0; i < entries; i++) {
            ledger.addEntry(String.valueOf(i).getBytes(Encoding));
        }

        // clear the cache to avoid flakiness
        factory.getEntryCacheManager().clear();

        final List<Entry> entryList = cursor.readEntries(3);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(entryList.size(), 3);
            assertEquals(ledger.ledgers.size(), 4);
            assertEquals(ledger.ledgerCache.size(), 3);
        });
        cursor.clearBacklog();
        cursor2.clearBacklog();
        ledger.trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(ledger.ledgers.size(), 4);
            assertEquals(ledger.ledgerCache.size(), 0);
        });

        // clear the cache to avoid flakiness
        factory.getEntryCacheManager().clear();

        // Verify the ReadHandle can be reopened.
        ManagedCursor cursor3 = ledger.openCursor("test-cursor3", InitialPosition.Earliest);
        final List<Entry> entryList2 = cursor3.readEntries(3);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(entryList2.size(), 3);
            assertEquals(ledger.ledgerCache.size(), 3);
        });

        cursor3.clearBacklog();
        ledger.trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(ledger.ledgers.size(), 4);
            assertEquals(ledger.ledgerCache.size(), 0);
        });

        cursor.close();
        cursor2.close();
        cursor3.close();
        ledger.close();
    }

    @Test
    public void testDoNotGetOffloadPoliciesMultipleTimesWhenTrimLedgers() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setMaxEntriesPerLedger(1);
        config.setMaxSizePerLedgerMb(1);
        LedgerOffloader ledgerOffloader = mock(NullLedgerOffloader.class);
        OffloadPoliciesImpl offloadPolicies = mock(OffloadPoliciesImpl.class);
        when(ledgerOffloader.getOffloadPolicies()).thenReturn(offloadPolicies);
        when(ledgerOffloader.getOffloadPolicies().getManagedLedgerOffloadThresholdInBytes()).thenReturn(-1L);
        when(ledgerOffloader.getOffloadPolicies().getManagedLedgerOffloadThresholdInSeconds()).thenReturn(-1L);
        when(ledgerOffloader.getOffloadDriverName()).thenReturn("s3");
        config.setLedgerOffloader(ledgerOffloader);
        ManagedLedgerImpl ledger = spy((ManagedLedgerImpl) factory.open(
                "testDoNotGetOffloadPoliciesMultipleTimesWhenTrimLedgers", config));
        doNothing().when(ledger).trimConsumedLedgersInBackground(any(CompletableFuture.class));

        // Retain the data.
        ledger.openCursor("test-cursor");
        final int entries = 10;
        byte[] data = new byte[1024 * 1024];
        for (int i = 0; i < entries; i++) {
            ledger.addEntry(data);
        }
        Awaitility.await().untilAsserted(() -> assertEquals(ledger.ledgers.size(), 11));

        // Set a new offloader to cleanup the execution times of getOffloadPolicies()
        ledgerOffloader = mock(NullLedgerOffloader.class);
        config.setLedgerOffloader(ledgerOffloader);

        ledger.internalTrimConsumedLedgers(Futures.NULL_PROMISE);
        verify(ledgerOffloader, times(1)).isAppendable();
    }

    @Test(timeOut = 30000)
    public void testReadOtherManagedLedgersEntry() throws Exception {
        ManagedLedgerImpl managedLedgerA = (ManagedLedgerImpl) factory.open("my_test_ledger_a");
        ManagedLedgerImpl managedLedgerB = (ManagedLedgerImpl) factory.open("my_test_ledger_b");

        Position pa = managedLedgerA.addEntry("dummy-entry-a".getBytes(Encoding));
        Position pb = managedLedgerB.addEntry("dummy-entry-b".getBytes(Encoding));

        // read managedLegerA's entry using managedLedgerA
        CompletableFuture<byte[]> completableFutureA = new CompletableFuture<>();
        managedLedgerA.asyncReadEntry(pa, new ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                completableFutureA.complete(entry.getData());
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                completableFutureA.completeExceptionally(exception.getCause());
            }
        }, null);

        assertEquals("dummy-entry-a".getBytes(Encoding), completableFutureA.get());

        // read managedLedgerB's entry using managedLedgerA
        CompletableFuture<byte[]> completableFutureB = new CompletableFuture<>();
        managedLedgerA.asyncReadEntry(pb, new ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                completableFutureB.complete(entry.getData());
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                completableFutureB.completeExceptionally(exception);
            }
        }, null);

        try {
            completableFutureB.get();
            Assert.fail();
        } catch (Exception e) {
            assertEquals(e.getCause().getMessage(),
                "Message not found, the ledgerId does not belong to this topic or has been deleted");
        }

        managedLedgerA.close();
        managedLedgerB.close();

    }

    @Test
    public void testCancellationOfScheduledTasks() throws Exception {
        Field timeoutTaskField = ManagedLedgerImpl.class.getDeclaredField("timeoutTask");
        timeoutTaskField.setAccessible(true);
        Field checkLedgerRollTaskField = ManagedLedgerImpl.class.getDeclaredField("checkLedgerRollTask");
        checkLedgerRollTaskField.setAccessible(true);

        ManagedLedgerImpl ledger1 = (ManagedLedgerImpl) factory.open("my_test_ledger_1");
        ledger1.addEntry("dummy-entry-1".getBytes(Encoding));
        ScheduledFuture<?> timeoutTask1 = (ScheduledFuture<?>) timeoutTaskField.get(ledger1);
        assertNotNull(timeoutTask1);
        assertFalse(timeoutTask1.isDone());
        ScheduledFuture<?> checkLedgerRollTask1 = (ScheduledFuture<?>) checkLedgerRollTaskField.get(ledger1);
        assertNotNull(checkLedgerRollTask1);
        assertFalse(checkLedgerRollTask1.isDone());
        ledger1.close();
        assertTrue(timeoutTask1.isCancelled());
        assertTrue(checkLedgerRollTask1.isCancelled());

        ManagedLedgerImpl ledger2 = (ManagedLedgerImpl) factory.open("my_test_ledger_2");
        ledger2.addEntry("dummy-entry-2".getBytes(Encoding));
        ScheduledFuture<?> timeoutTask2 = (ScheduledFuture<?>) timeoutTaskField.get(ledger2);
        assertNotNull(timeoutTask2);
        assertFalse(timeoutTask2.isDone());
        ScheduledFuture<?> checkLedgerRollTask2 = (ScheduledFuture<?>) checkLedgerRollTaskField.get(ledger2);
        assertNotNull(checkLedgerRollTask2);
        assertFalse(checkLedgerRollTask2.isDone());
        ledger2.delete();
        assertTrue(timeoutTask2.isCancelled());
        assertTrue(checkLedgerRollTask2.isCancelled());
    }

    @Test
    public void testInactiveLedgerRollOver() throws Exception {
        int inactiveLedgerRollOverTimeMs = 5;
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setInactiveLedgerRollOverTime(inactiveLedgerRollOverTimeMs, TimeUnit.MILLISECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("rollover_inactive", config);
        ManagedCursor cursor = ledger.openCursor("c1");

        List<Long> ledgerIds = new ArrayList<>();

        int totalAddEntries = 5;
        for (int i = 0; i < totalAddEntries; i++) {
            String content = "entry"; // 5 bytes
            ledger.checkInactiveLedgerAndRollOver();
            ledger.addEntry(content.getBytes());
            Thread.sleep(inactiveLedgerRollOverTimeMs * 5);

            ledgerIds.add(ledger.currentLedger.getId());
        }

        Map<Long, PulsarMockLedgerHandle> ledgerMap = bkc.getLedgerMap();
        // skip check last ledger, it should be open
        for (int i = 0; i < ledgerIds.size() - 1; i++) {
            long ledgerId = ledgerIds.get(i);
            LedgerMetadata ledgerMetadata = ledgerMap.get(ledgerId).getLedgerMetadata();
            if (ledgerMetadata != null) {
                assertTrue(ledgerMetadata.isClosed());
            }
        }

        List<LedgerInfo> ledgers = ledger.getLedgersInfoAsList();
        assertEquals(ledgers.size(), totalAddEntries);
        ledger.close();
    }

    @Test
    public void testDontRollOverEmptyInactiveLedgers() throws Exception {
        int inactiveLedgerRollOverTimeMs = 5;
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setInactiveLedgerRollOverTime(inactiveLedgerRollOverTimeMs, TimeUnit.MILLISECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("rollover_inactive", config);
        ManagedCursor cursor = ledger.openCursor("c1");

        long ledgerId = ledger.currentLedger.getId();

        Thread.sleep(inactiveLedgerRollOverTimeMs * 5);
        ledger.checkInactiveLedgerAndRollOver();

        Thread.sleep(inactiveLedgerRollOverTimeMs * 5);
        ledger.checkInactiveLedgerAndRollOver();

        assertEquals(ledger.currentLedger.getId(), ledgerId);

        ledger.close();
    }

    @Test
    public void testDontRollOverInactiveLedgersWhenMetadataServiceInvalid() throws Exception {
        int inactiveLedgerRollOverTimeMs = 5;
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = spy(new ManagedLedgerFactoryImpl(metadataStore, bkc));
        // mock metadata service invalid
        when(factory.isMetadataServiceAvailable()).thenReturn(false);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setInactiveLedgerRollOverTime(inactiveLedgerRollOverTimeMs, TimeUnit.MILLISECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("rollover_inactive", config);

        long ledgerId = ledger.currentLedger.getId();

        Thread.sleep(inactiveLedgerRollOverTimeMs * 5);
        ledger.checkInactiveLedgerAndRollOver();

        Thread.sleep(inactiveLedgerRollOverTimeMs * 5);
        ledger.checkInactiveLedgerAndRollOver();

        assertEquals(ledger.currentLedger.getId(), ledgerId);

        ledger.close();
    }

    @Test
    public void testOffloadTaskCancelled() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setMaxEntriesPerLedger(2);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);

        OffloadPoliciesImpl offloadPolicies = new OffloadPoliciesImpl();
        offloadPolicies.setManagedLedgerOffloadDriver("mock");
        offloadPolicies.setManagedLedgerOffloadThresholdInBytes(0L);
        LedgerOffloader ledgerOffloader = Mockito.mock(LedgerOffloader.class);
        Mockito.when(ledgerOffloader.getOffloadPolicies()).thenReturn(offloadPolicies);
        Mockito.when(ledgerOffloader.getOffloadDriverName())
                .thenReturn(offloadPolicies.getManagedLedgerOffloadDriver());
        config.setLedgerOffloader(ledgerOffloader);

        CompletableFuture<ReadHandle> readHandle = new CompletableFuture<>();
        readHandle.complete(mock(ReadHandle.class));

        CompletableFuture<Void> offloadFuture = new CompletableFuture<>();
        offloadFuture.complete(null);
        Mockito.when(ledgerOffloader.offload(any(ReadHandle.class), any(UUID.class),
                any(Map.class))).thenReturn(offloadFuture);

        final ManagedLedgerImpl ledgerInit = (ManagedLedgerImpl) factory.open("test-offload-task-close", config);
        final ManagedLedgerImpl ledger = spy(ledgerInit);
        long ledgerId = 3L;
        doReturn(readHandle).when(ledger).getLedgerHandle(ledgerId);
        doReturn(ManagedLedgerImpl.State.Closed).when(ledger).getState();
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        ledger.close();

        Awaitility.await().untilAsserted(() -> {
            CompletableFuture<LedgerInfo> ledgerInfo = ledger.getLedgerInfo(ledgerId);
            Assert.assertFalse(ledgerInfo.get(100, TimeUnit.MILLISECONDS).getOffloadContext().getComplete());
        });
    }

    @Test
    public void testGetTheSlowestNonDurationReadPosition() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("test_",
                initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(1)
                        .setRetentionTime(-1, TimeUnit.SECONDS)
                        .setRetentionSizeInMB(-1));
        ledger.openCursor("c1");

        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            positions.add(ledger.addEntry(("entry-" + i).getBytes(UTF_8)));
        }

        Assert.assertEquals(ledger.getTheSlowestNonDurationReadPosition(), PositionFactory.LATEST);

        ManagedCursor nonDurableCursor = ledger.newNonDurableCursor(PositionFactory.EARLIEST);

        Assert.assertEquals(ledger.getTheSlowestNonDurationReadPosition(), positions.get(0));

        ledger.deleteCursor(nonDurableCursor.getName());

        Assert.assertEquals(ledger.getTheSlowestNonDurationReadPosition(), PositionFactory.LATEST);

        ledger.close();
    }

    @Test
    public void testGetLedgerMetadata() throws Exception {
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) factory.open("testGetLedgerMetadata");
        long lastLedger = managedLedger.ledgers.lastEntry().getKey();
        managedLedger.getLedgerMetadata(lastLedger);
        Assert.assertFalse(managedLedger.ledgerCache.containsKey(lastLedger));
    }

    @Test
    public void testGetEnsemblesAsync() throws Exception {
        // test getEnsemblesAsync of latest ledger will not open it twice and put it in ledgerCache.
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) factory.open("testGetLedgerMetadata");
        long lastLedger = managedLedger.ledgers.lastEntry().getKey();
        managedLedger.getEnsemblesAsync(lastLedger).join();
        Assert.assertFalse(managedLedger.ledgerCache.containsKey(lastLedger));
    }

    @Test
    public void testIsNoMessagesAfterPos() throws Exception {
        final byte[] data = new byte[]{1, 2, 3};
        final String cursorName = "c1";
        final String mlName = UUID.randomUUID().toString().replaceAll("-", "");
        final ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open(mlName);
        final ManagedCursor managedCursor = ml.openCursor(cursorName);

        // One ledger.
        Position p1 = ml.addEntry(data);
        Position p2 = ml.addEntry(data);
        Position p3 = ml.addEntry(data);
        assertFalse(ml.isNoMessagesAfterPos(p1));
        assertFalse(ml.isNoMessagesAfterPos(p2));
        assertTrue(ml.isNoMessagesAfterPos(p3));
        assertTrue(ml.isNoMessagesAfterPos(PositionFactory.create(p3.getLedgerId(), p3.getEntryId() + 1)));
        assertTrue(ml.isNoMessagesAfterPos(PositionFactory.create(p3.getLedgerId() + 1, -1)));

        // More than one ledger.
        ml.ledgerClosed(ml.currentLedger);
        Position p4 = ml.addEntry(data);
        Position p5 = ml.addEntry(data);
        Position p6 = ml.addEntry(data);
        assertFalse(ml.isNoMessagesAfterPos(p1));
        assertFalse(ml.isNoMessagesAfterPos(p2));
        assertFalse(ml.isNoMessagesAfterPos(p3));
        assertFalse(ml.isNoMessagesAfterPos(p4));
        assertFalse(ml.isNoMessagesAfterPos(p5));
        assertTrue(ml.isNoMessagesAfterPos(p6));
        assertTrue(ml.isNoMessagesAfterPos(PositionFactory.create(p6.getLedgerId(), p6.getEntryId() + 1)));
        assertTrue(ml.isNoMessagesAfterPos(PositionFactory.create(p6.getLedgerId() + 1, -1)));

        // Switch ledger and make the entry id of Last confirmed entry is -1;
        ml.ledgerClosed(ml.currentLedger);
        ml.createLedgerAfterClosed();
        Awaitility.await().untilAsserted(() -> {
            assertEquals(ml.currentLedgerEntries, 0);
        });
        ml.lastConfirmedEntry = PositionFactory.create(ml.currentLedger.getId(), -1);
        assertFalse(ml.isNoMessagesAfterPos(p5));
        assertTrue(ml.isNoMessagesAfterPos(p6));
        assertTrue(ml.isNoMessagesAfterPos(PositionFactory.create(p6.getLedgerId(), p6.getEntryId() + 1)));
        assertTrue(ml.isNoMessagesAfterPos(PositionFactory.create(p6.getLedgerId() + 1, -1)));

        // Trim ledgers to make there is no entries in ML.
        ml.deleteCursor(cursorName);
        CompletableFuture<Void> future = new CompletableFuture<>();
        ml.trimConsumedLedgersInBackground(true, future);
        future.get();
        assertEquals(ml.ledgers.size(), 1);
        assertEquals(ml.lastConfirmedEntry.getEntryId(), -1);
        assertTrue(ml.isNoMessagesAfterPos(p1));
        assertTrue(ml.isNoMessagesAfterPos(p2));
        assertTrue(ml.isNoMessagesAfterPos(p3));
        assertTrue(ml.isNoMessagesAfterPos(p4));
        assertTrue(ml.isNoMessagesAfterPos(p5));
        assertTrue(ml.isNoMessagesAfterPos(p6));
        assertTrue(ml.isNoMessagesAfterPos(PositionFactory.create(p6.getLedgerId(), p6.getEntryId() + 1)));
        assertTrue(ml.isNoMessagesAfterPos(PositionFactory.create(p6.getLedgerId() + 1, -1)));

        // cleanup.
        ml.close();
    }

    @Test
    public void testGetEstimatedBacklogSize() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setMaxEntriesPerLedger(2);
        config.setRetentionTime(-1, TimeUnit.SECONDS);
        config.setRetentionSizeInMB(-1);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testGetEstimatedBacklogSize", config);
        List<Position> positions = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            positions.add(ledger.addEntry(new byte[1]));
        }

        Assert.assertEquals(ledger.getEstimatedBacklogSize(PositionFactory.create(-1, -1)), 10);
        Assert.assertEquals(ledger.getEstimatedBacklogSize((positions.get(1))), 8);
        Assert.assertEquals(ledger.getEstimatedBacklogSize((positions.get(9)).getNext()), 0);
        ledger.close();
    }

    @Test
    public void testDeleteCursorTwice() throws Exception {
        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("ml");
        String cursorName = "cursor_1";
        ml.openCursor(cursorName);
        syncRemoveCursor(ml, cursorName);
        syncRemoveCursor(ml, cursorName);
    }

    private void syncRemoveCursor(ManagedLedgerImpl ml, String cursorName){
        CompletableFuture<Void> future = new CompletableFuture<>();
        ml.getStore().asyncRemoveCursor(ml.name, cursorName, new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat stat) {
                future.complete(null);
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                future.completeExceptionally(FutureUtil.unwrapCompletionException(e));
            }
        });
        future.join();
    }

    @Test
    public void testNonDurableCursorCreateForInactiveLedger() throws Exception {
        String mlName = "testLedgerInfoMetaCorrectIfAddEntryTimeOut";
        BookKeeper spyBookKeeper = spy(bkc);
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, spyBookKeeper);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setInactiveLedgerRollOverTime(10, TimeUnit.MILLISECONDS);
        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open(mlName, config);
        ml.addEntry("entry".getBytes(UTF_8));

        MutableBoolean isRolledOver = new MutableBoolean(false);
        retryStrategically((test) -> {
            if (isRolledOver.booleanValue()) {
                return true;
            }
            isRolledOver.setValue(ml.checkInactiveLedgerAndRollOver());
            return isRolledOver.booleanValue();
        }, 5, 1000);
        assertTrue(isRolledOver.booleanValue());

        Position position = PositionFactory.create(-1L, -1L);
        assertNotNull(ml.newNonDurableCursor(position));
    }

    @Test(timeOut = 60 * 1000)
    public void testCreateDataLedgerTimeout() throws Exception {
        String mlName = UUID.randomUUID().toString();
        ManagedLedgerFactoryImpl factory = null;
        ManagedLedger ml = null;
        try {
            factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
            ManagedLedgerConfig config = new ManagedLedgerConfig();
            config.setMetadataOperationsTimeoutSeconds(5);
            bkc.delay(10 * 1000);
            ml = factory.open(mlName, config);
            fail("Should get a timeout ex");
        } catch (ManagedLedgerException ex) {
            assertTrue(ex.getMessage().contains("timeout"));
        } finally {
            // cleanup.
            if (ml != null) {
                ml.delete();
            }
            if (factory != null) {
                factory.shutdown();
            }
        }
    }

    /***
     * When a ML tries to create a ledger, it will create a delay task to check if the ledger create request is timeout.
     * But we should guarantee that the delay task should be canceled after the ledger create request responded.
     */
    @Test
    public void testNoOrphanScheduledTasksAfterCloseML() throws Exception {
        String mlName = UUID.randomUUID().toString();
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setMetadataOperationsTimeoutSeconds(3600);

        // Calculate pending task count.
        long pendingTaskCountBefore = calculatePendingTaskCount(factory.getScheduledExecutor());
        // Trigger create & close ML 1000 times.
        for (int i = 0; i < 1000; i++) {
            ManagedLedger ml = factory.open(mlName, config);
            ml.close();
        }
        // Verify there is no orphan scheduled task.
        long pendingTaskCountAfter = calculatePendingTaskCount(factory.getScheduledExecutor());
        // Maybe there are other components also appended scheduled tasks, so leave 100 tasks to avoid flaky.
        assertTrue(pendingTaskCountAfter - pendingTaskCountBefore < 100);
    }

    /**
     * Calculate how many pending tasks in {@link OrderedScheduler}.
     */
    private long calculatePendingTaskCount(OrderedScheduler orderedScheduler) {
        ExecutorService[] threads = WhiteboxImpl.getInternalState(orderedScheduler, "threads");
        long taskCounter = 0;
        for (ExecutorService thread : threads) {
            BoundedScheduledExecutorService boundedScheduledExecutorService =
                    WhiteboxImpl.getInternalState(thread, "delegate");
            BlockingQueue<Runnable> queue =  WhiteboxImpl.getInternalState(boundedScheduledExecutorService, "queue");
            for (Runnable r : queue) {
                if (r instanceof FutureTask) {
                    FutureTask futureTask = (FutureTask) r;
                    if (!futureTask.isCancelled() && !futureTask.isDone()) {
                        taskCounter++;
                    }
                } else {
                    taskCounter++;
                }
            }
        }
        return taskCounter;
    }

    @Test
    public void testNoCleanupOffloadLedgerWhenMetadataExceptionHappens() throws Exception {
        ManagedLedgerConfig config = spy(new ManagedLedgerConfig());
        ManagedLedgerImpl ml = spy((ManagedLedgerImpl) factory.open("testNoCleanupOffloadLedger", config));

        // mock the ledger offloader
        LedgerOffloader ledgerOffloader = mock(NullLedgerOffloader.class);
        when(config.getLedgerOffloader()).thenReturn(ledgerOffloader);
        when(ledgerOffloader.getOffloadDriverName()).thenReturn("mock");

        // There will have two put call to the metadata store, the first time is prepare the offload.
        // And the second is the complete the offload. This case is testing when completing the offload,
        // the metadata store meets an exception.
        AtomicInteger metadataPutCallCount = new AtomicInteger(0);
        metadataStore.failConditional(new MetadataStoreException("mock completion error"),
            (key, value) -> key.equals(FaultInjectionMetadataStore.OperationType.PUT)
                && metadataPutCallCount.incrementAndGet() == 2);

        // prepare the arguments for the offloadLoop method
        CompletableFuture<Position> future = new CompletableFuture<>();
        Queue<LedgerInfo> ledgersToOffload = new LinkedList<>();
        LedgerInfo ledgerInfo = LedgerInfo.getDefaultInstance().toBuilder().setLedgerId(1).setEntries(10).build();
        ledgersToOffload.add(ledgerInfo);
        Position firstUnoffloaded = PositionFactory.create(1, 0);
        Optional<Throwable> firstError = Optional.empty();

        // mock the read handle to make the offload successful
        CompletableFuture<ReadHandle> readHandle = new CompletableFuture<>();
        readHandle.complete(mock(ReadHandle.class));
        when(ml.getLedgerHandle(eq(ledgerInfo.getLedgerId()))).thenReturn(readHandle);
        when(ledgerOffloader.offload(any(), any(), anyMap())).thenReturn(CompletableFuture.completedFuture(null));

        ml.ledgers.put(ledgerInfo.getLedgerId(), ledgerInfo);

        // do the offload
        ml.offloadLoop(future, ledgersToOffload, firstUnoffloaded, firstError);

        // waiting for the offload complete
        try {
            future.join();
            fail("The offload should fail");
        } catch (Exception e) {
            // the offload should fail
            assertTrue(e.getCause().getMessage().contains("mock completion error"));
        }

        // the ledger deletion shouldn't happen
        verify(ledgerOffloader, times(0))
            .deleteOffloaded(eq(ledgerInfo.getLedgerId()), any(), anyMap());
    }


    @DataProvider(name = "closeLedgerByAddEntry")
    public Object[][] closeLedgerByAddEntry() {
        return new Object[][] {{Boolean.TRUE}, {Boolean.FALSE}};
    }

    @Test(dataProvider = "closeLedgerByAddEntry")
    public void testDeleteCurrentLedgerWhenItIsClosed(boolean closeLedgerByAddEntry) throws Exception {
        // Setup: Open a manageLedger with one initial entry.
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setMaxEntriesPerLedger(10);
        ManagedLedgerImpl ml = spy((ManagedLedgerImpl) factory.open("testDeleteCurrentLedgerWhenItIsClosed",
                config));
        assertEquals(ml.ledgers.size(), 1);
        ml.addEntry(new byte[4]);
        // Act: Trigger the rollover of the current ledger.
        long currentLedgerID = ml.currentLedger.getId();
        ml.config.setMaximumRolloverTime(10, TimeUnit.MILLISECONDS);
        Thread.sleep(10);
        if (closeLedgerByAddEntry) {
            // Detect the current ledger is full before written entry and close the ledger after writing completely.
            ml.addEntry(new byte[4]);
        } else {
            // Detect the current ledger is full by the timed task. (Imitate: the timed task `checkLedgerRollTask` call
            // `rollCurrentLedgerIfFull` periodically).
            ml.rollCurrentLedgerIfFull();
        }
        // wait the new ledger create
        Awaitility.await().untilAsserted(() -> assertEquals(ml.ledgers.size(), 2));

        // Act: Trigger trimming to delete the previous current ledger.
        ml.internalTrimLedgers(false, Futures.NULL_PROMISE);
        // Verify: A new ledger will be opened after the current ledger is closed and the previous current ledger can be
        // deleted.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(ml.state, ManagedLedgerImpl.State.LedgerOpened);
            assertEquals(ml.ledgers.size(), 1);
            assertNotEquals(currentLedgerID, ml.currentLedger.getId());
            assertEquals(ml.currentLedgerEntries, 0);
        });
    }

    @Test
    public void testSetLedgerProperty() throws Exception {
        testSetLedgerProperty0("testSetLedgerProperty");
    }


    private ManagedLedger testSetLedgerProperty0(String name) throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open(name, config);
        ml.addEntry("entry-1".getBytes());
        ml.addEntry("entry-2".getBytes());
        ml.addEntry("entry-3".getBytes());

        Assert.assertEquals(ml.getLedgersInfo().size(), 2);
        long firstLedger = ml.getLedgersInfo().firstKey();
        LedgerInfo firstLedgerInfo = ml.getLedgersInfo().get(firstLedger);
        Assert.assertEquals(firstLedgerInfo.getPropertiesCount(), 0);
        long lastLedger = ml.getLedgersInfo().lastKey();
        LedgerInfo lastLedgerInfo = ml.getLedgersInfo().get(lastLedger);
        Assert.assertEquals(lastLedgerInfo.getPropertiesCount(), 0);

        ml.asyncAddLedgerProperty(firstLedger, "key1", "value1").get();
        ml.asyncAddLedgerProperty(lastLedger, "key2", "value2").get();

        firstLedgerInfo = ml.getLedgersInfo().get(firstLedger);
        Assert.assertEquals(firstLedgerInfo.getPropertiesCount(), 1);
        Assert.assertEquals(ml.asyncGetLedgerProperty(firstLedger, "key1").get(), "value1");

        lastLedgerInfo = ml.getLedgersInfo().get(lastLedger);
        Assert.assertEquals(lastLedgerInfo.getPropertiesCount(), 1);
        Assert.assertEquals(ml.asyncGetLedgerProperty(lastLedger, "key2").get(), "value2");

        return ml;
    }

    @Test
    public void testRemoveLedgerProperty() throws Exception {
        ManagedLedger ml = testSetLedgerProperty0("testRemoveLedgerProperty");

        long firstLedger = ml.getLedgersInfo().firstKey();
        long lastLedger = ml.getLedgersInfo().lastKey();

        ml.asyncRemoveLedgerProperty(firstLedger, "key1").get();
        ml.asyncRemoveLedgerProperty(lastLedger, "key2").get();

        Assert.assertEquals(ml.getLedgersInfo().get(firstLedger).getPropertiesCount(), 0);
        Assert.assertEquals(ml.getLedgersInfo().get(lastLedger).getPropertiesCount(), 0);
    }
}
