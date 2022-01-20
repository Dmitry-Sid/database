import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import server.model.RowIdRepository;
import server.model.StoppableBatchStream;
import server.model.StoppableStream;
import server.model.Utils;
import server.model.impl.DataCompressorImpl;
import server.model.impl.DestroyServiceImpl;
import server.model.impl.ObjectConverterImpl;
import server.model.pojo.RowAddress;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class RowIdRepositoryTest {
    private static final String filePath = "rowIdVariables";
    private static final String filesIdPath = "rowId";
    private static final String filesRowPath = "row";
    private static final int maxIdSize = 500;
    private static final int compressSize = 2;
    private static final int rowAddressSize = 5;

    @Parameterized.Parameters
    public static Object[][] data() {
        return new Object[1][0];
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(new File(filesIdPath));
        FileUtils.deleteDirectory(new File(filesRowPath));
    }

    @Test
    public void newIdTest() {
        final int lastId = 750;
        createFiles(lastId);
        final RowIdRepository rowIdRepository = prepareRowIdRepository(maxIdSize);
        TestUtils.doAndSleep(rowIdRepository, () -> {
            assertEquals(751, rowIdRepository.newId());
            assertEquals(752, rowIdRepository.newId());
        });
    }

    @Test
    public void processTest() {
        final int lastId = 750;
        createFiles(lastId);
        final RowIdRepository rowIdRepository = prepareRowIdRepository(maxIdSize);
        TestUtils.doAndSleep(rowIdRepository, () -> {
            assertFalse(rowIdRepository.process(0, rowAddress -> {
            }));
            assertFalse(rowIdRepository.process(1200, rowAddress -> {
            }));
            assertTrue(rowIdRepository.process(1, rowAddress -> {
                assertEquals(filesRowPath + "/" + filesRowPath + 1, rowAddress.getFilePath());
                assertEquals(1, rowAddress.getId());
                assertEquals(0, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + "/" + filesRowPath + 1, 2, 5, rowAddressSize);
                assertEquals(rowAddressNext.getId(), rowAddress.getNext());
            }));
            assertTrue(rowIdRepository.process(250, rowAddress -> {
                assertEquals(filesRowPath + "/" + filesRowPath + 1, rowAddress.getFilePath());
                assertEquals(250, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertEquals(-1, rowAddress.getNext());
            }));
            assertTrue(rowIdRepository.process(499, rowAddress -> {
                assertEquals(filesRowPath + "/" + filesRowPath + 2, rowAddress.getFilePath());
                assertEquals(499, rowAddress.getId());
                assertEquals(1240, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + "/" + filesRowPath + 2, 500, 1245, rowAddressSize);
                assertEquals(rowAddressNext.getId(), rowAddress.getNext());
            }));
            assertTrue(rowIdRepository.process(500, rowAddress -> {
                assertEquals(filesRowPath + "/" + filesRowPath + 2, rowAddress.getFilePath());
                assertEquals(500, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertEquals(-1, rowAddress.getNext());
            }));
            assertTrue(rowIdRepository.process(501, rowAddress -> {
                assertEquals(filesRowPath + "/" + filesRowPath + 3, rowAddress.getFilePath());
                assertEquals(501, rowAddress.getId());
                assertEquals(0, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + "/" + filesRowPath + 3, 502, 5, rowAddressSize);
                assertEquals(rowAddressNext.getId(), rowAddress.getNext());
            }));
            assertTrue(rowIdRepository.process(749, rowAddress -> {
                assertEquals(filesRowPath + "/" + filesRowPath + 3, rowAddress.getFilePath());
                assertEquals(749, rowAddress.getId());
                assertEquals(1240, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + "/" + filesRowPath + 3, 750, 1245, rowAddressSize);
                assertEquals(rowAddressNext.getId(), rowAddress.getNext());
            }));
            assertTrue(rowIdRepository.process(750, rowAddress -> {
                assertEquals(filesRowPath + "/" + filesRowPath + 3, rowAddress.getFilePath());
                assertEquals(750, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertEquals(-1, rowAddress.getNext());
            }));
            assertFalse(rowIdRepository.process(751, rowAddress -> {
            }));
        });
    }

    @Test
    public void addTest() {
        final int lastId = 750;
        createFiles(lastId);
        final RowIdRepository rowIdRepository = prepareRowIdRepository(maxIdSize);
        TestUtils.doAndSleep(rowIdRepository, () -> {
            try {
                rowIdRepository.add(10, rowAddress -> rowAddress.setSize(10));
                fail("never");
            } catch (Exception e) {
                assertEquals("cannot add existing rowId 10", e.getMessage());
            }
            final int id = rowIdRepository.newId();
            rowIdRepository.add(id, rowAddress -> {
            });
            assertFalse(rowIdRepository.process(id, rowAddress -> fail("never")));
            rowIdRepository.save(id, rowAddress -> rowAddress.setSize(10));
            assertTrue(rowIdRepository.process(751, rowAddress -> {
                assertEquals(filesRowPath + "/" + filesRowPath + 4, rowAddress.getFilePath());
                assertEquals(751, rowAddress.getId());
                assertEquals(0, rowAddress.getPosition());
                assertEquals(10, rowAddress.getSize());
                assertEquals(-1, rowAddress.getPrevious());
                assertEquals(-1, rowAddress.getNext());
            }));
        });
    }

    @Test
    public void saveTest() {
        final int lastId = 750;
        createFiles(lastId);
        final RowIdRepository rowIdRepository = prepareRowIdRepository(maxIdSize);
        {
            TestUtils.doAndSleep(rowIdRepository, () -> {
                rowIdRepository.save(rowIdRepository.newId(), rowAddress -> {
                    rowAddress.setSize(10);
                });
                assertTrue(rowIdRepository.process(250, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 1, rowAddress.getFilePath());
                    assertEquals(250, rowAddress.getId());
                    assertEquals(1245, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    assertEquals(-1, rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(750, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 3, rowAddress.getFilePath());
                    assertEquals(750, rowAddress.getId());
                    assertEquals(1245, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    assertEquals(-1, rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(751, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 4, rowAddress.getFilePath());
                    assertEquals(751, rowAddress.getId());
                    assertEquals(0, rowAddress.getPosition());
                    assertEquals(10, rowAddress.getSize());
                    assertEquals(-1, rowAddress.getPrevious());
                    assertEquals(-1, rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(250, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 1, rowAddress.getFilePath());
                    assertEquals(250, rowAddress.getId());
                    assertEquals(1245, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    assertEquals(-1, rowAddress.getNext());
                }));
                rowIdRepository.save(rowIdRepository.newId(), rowAddress -> {
                    rowAddress.setSize(67);
                });
                assertTrue(rowIdRepository.process(250, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 1, rowAddress.getFilePath());
                    assertEquals(250, rowAddress.getId());
                    assertEquals(1245, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    assertEquals(-1, rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(750, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 3, rowAddress.getFilePath());
                    assertEquals(750, rowAddress.getId());
                    assertEquals(1245, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    assertEquals(-1, rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(751, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 4, rowAddress.getFilePath());
                    assertEquals(751, rowAddress.getId());
                    assertEquals(0, rowAddress.getPosition());
                    assertEquals(10, rowAddress.getSize());
                    assertEquals(-1, rowAddress.getPrevious());
                    final RowAddress rowAddressNext = new RowAddress(filesRowPath + "/" + filesRowPath + 4, 752, 10, 67);
                    assertEquals(rowAddressNext.getId(), rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(752, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 4, rowAddress.getFilePath());
                    assertEquals(752, rowAddress.getId());
                    assertEquals(10, rowAddress.getPosition());
                    assertEquals(67, rowAddress.getSize());
                    final RowAddress rowAddressPrevious = new RowAddress(filesRowPath + "/" + filesRowPath + 4, 751, 0, 10);
                    assertEquals(rowAddressPrevious.getId(), rowAddress.getPrevious());
                    assertEquals(-1, rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(250, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 1, rowAddress.getFilePath());
                    assertEquals(250, rowAddress.getId());
                    assertEquals(1245, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    assertEquals(-1, rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(752, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 4, rowAddress.getFilePath());
                    assertEquals(752, rowAddress.getId());
                    assertEquals(10, rowAddress.getPosition());
                    assertEquals(67, rowAddress.getSize());
                    final RowAddress rowAddressPrevious = new RowAddress(filesRowPath + "/" + filesRowPath + 4, 751, 0, 10);
                    assertEquals(rowAddressPrevious.getId(), rowAddress.getPrevious());
                    assertEquals(-1, rowAddress.getNext());
                }));
            });
        }
        {
            TestUtils.doAndSleep(rowIdRepository, () -> {
                rowIdRepository.save(501, rowAddress -> {
                    rowAddress.setSize(14);
                });
                rowIdRepository.save(250, rowAddress -> {
                    rowAddress.setSize(7);
                });
                assertTrue(rowIdRepository.process(501, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 3, rowAddress.getFilePath());
                    assertEquals(501, rowAddress.getId());
                    assertEquals(0, rowAddress.getPosition());
                    assertEquals(14, rowAddress.getSize());
                    final RowAddress rowAddressNext = new RowAddress(filesRowPath + "/" + filesRowPath + 3, 502, 14, rowAddressSize);
                    assertEquals(rowAddressNext.getId(), rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(650, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 3, rowAddress.getFilePath());
                    assertEquals(650, rowAddress.getId());
                    assertEquals(754, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    final RowAddress rowAddressNext = new RowAddress(filesRowPath + "/" + filesRowPath + 3, 651, 759, rowAddressSize);
                    assertEquals(rowAddressNext.getId(), rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(750, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 3, rowAddress.getFilePath());
                    assertEquals(750, rowAddress.getId());
                    assertEquals(1254, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    assertEquals(-1, rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(1, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 1, rowAddress.getFilePath());
                    assertEquals(1, rowAddress.getId());
                    assertEquals(0, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    final RowAddress rowAddressNext = new RowAddress(filesRowPath + "/" + filesRowPath + 1, 2, 5, rowAddressSize);
                    assertEquals(rowAddressNext.getId(), rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(250, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 1, rowAddress.getFilePath());
                    assertEquals(250, rowAddress.getId());
                    assertEquals(1245, rowAddress.getPosition());
                    assertEquals(7, rowAddress.getSize());
                    assertEquals(-1, rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(499, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 2, rowAddress.getFilePath());
                    assertEquals(499, rowAddress.getId());
                    assertEquals(1240, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    final RowAddress rowAddressNext = new RowAddress(filesRowPath + "/" + filesRowPath + 2, 500, 1245, rowAddressSize);
                    assertEquals(rowAddressNext.getId(), rowAddress.getNext());
                }));
                rowIdRepository.save(100, rowAddress -> {
                    rowAddress.setSize(15);
                });
                assertTrue(rowIdRepository.process(99, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 1, rowAddress.getFilePath());
                    assertEquals(99, rowAddress.getId());
                    assertEquals(490, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    final RowAddress rowAddressNext = new RowAddress(filesRowPath + "/" + filesRowPath + 1, 100, 495, 15);
                    assertEquals(rowAddressNext.getId(), rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(100, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 1, rowAddress.getFilePath());
                    assertEquals(100, rowAddress.getId());
                    assertEquals(495, rowAddress.getPosition());
                    assertEquals(15, rowAddress.getSize());
                    final RowAddress rowAddressNext = new RowAddress(filesRowPath + "/" + filesRowPath + 1, 101, 510, rowAddressSize);
                    assertEquals(rowAddressNext.getId(), rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(250, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 1, rowAddress.getFilePath());
                    assertEquals(250, rowAddress.getId());
                    assertEquals(1255, rowAddress.getPosition());
                    assertEquals(7, rowAddress.getSize());
                    assertEquals(-1, rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(251, rowAddress -> {
                    assertEquals(filesRowPath + "/" + filesRowPath + 2, rowAddress.getFilePath());
                    assertEquals(251, rowAddress.getId());
                    assertEquals(0, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    assertEquals(-1, rowAddress.getPrevious());
                    final RowAddress rowAddressNext = new RowAddress(filesRowPath + "/" + filesRowPath + 2, 252, 5, rowAddressSize);
                    assertEquals(rowAddressNext.getId(), rowAddress.getNext());
                }));
            });
        }
    }

    @Test
    public void deleteTest() {
        final int lastId = 750;
        createFiles(lastId);
        final RowIdRepository rowIdRepository = prepareRowIdRepository(maxIdSize);
        TestUtils.doAndSleep(rowIdRepository, () -> {
            assertTrue(rowIdRepository.process(300, rowAddress -> {
                assertEquals(filesRowPath + "/" + filesRowPath + 2, rowAddress.getFilePath());
                assertEquals(300, rowAddress.getId());
                assertEquals(245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressPrevious = new RowAddress(filesRowPath + "/" + filesRowPath + 2, 299, 240, rowAddressSize);
                assertEquals(rowAddressPrevious.getId(), rowAddress.getPrevious());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + "/" + filesRowPath + 2, 301, 250, rowAddressSize);
                assertEquals(rowAddressNext.getId(), rowAddress.getNext());
            }));
            rowIdRepository.delete(300);
            assertFalse(rowIdRepository.process(300, rowAddress -> {

            }));
            assertTrue(rowIdRepository.process(750, rowAddress -> {
                assertEquals(filesRowPath + "/" + filesRowPath + 3, rowAddress.getFilePath());
                assertEquals(750, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertEquals(-1, rowAddress.getNext());
            }));
            assertTrue(rowIdRepository.process(299, rowAddress -> {
                assertEquals(filesRowPath + "/" + filesRowPath + 2, rowAddress.getFilePath());
                assertEquals(299, rowAddress.getId());
                assertEquals(240, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressPrevious = new RowAddress(filesRowPath + "/" + filesRowPath + 2, 298, 235, rowAddressSize);
                assertEquals(rowAddressPrevious.getId(), rowAddress.getPrevious());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + "/" + filesRowPath + 2, 301, 245, rowAddressSize);
                assertEquals(rowAddressNext.getId(), rowAddress.getNext());
            }));
            assertTrue(rowIdRepository.process(301, rowAddress -> {
                assertEquals(filesRowPath + "/" + filesRowPath + 2, rowAddress.getFilePath());
                assertEquals(301, rowAddress.getId());
                assertEquals(245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressPrevious = new RowAddress(filesRowPath + "/" + filesRowPath + 2, 299, 240, rowAddressSize);
                assertEquals(rowAddressPrevious.getId(), rowAddress.getPrevious());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + "/" + filesRowPath + 2, 302, 250, rowAddressSize);
                assertEquals(rowAddressNext.getId(), rowAddress.getNext());
            }));
            rowIdRepository.delete(750);
            final int[] id = new int[1];
            rowIdRepository.save(rowIdRepository.newId(), rowAddress -> {
                id[0] = rowAddress.getId();
            });
            assertEquals(751, id[0]);
        });
    }

    @Test
    public void streamTest() {
        final int lastId = 750;
        createFiles(lastId);
        final RowIdRepository rowIdRepository = prepareRowIdRepository(maxIdSize);
        TestUtils.doAndSleep(rowIdRepository, () -> {
            {
                final AtomicInteger counter = new AtomicInteger();
                final StoppableBatchStream<RowAddress> stream = rowIdRepository.batchStream();
                final boolean[] batchEnd = new boolean[]{false, false, false, false, false, false};
                final boolean[] streamEnd = new boolean[]{false, false};
                stream.addOnBatchEnd(() -> {
                    assertTrue(counter.get() == maxIdSize / compressSize || counter.get() == maxIdSize || counter.get() == lastId);
                    if (counter.get() == maxIdSize / compressSize) {
                        batchEnd[0] = true;
                    } else if (counter.get() == maxIdSize) {
                        batchEnd[1] = true;
                    } else {
                        batchEnd[2] = true;
                    }
                });
                stream.addOnBatchEnd(() -> {
                    assertTrue(counter.get() == maxIdSize / compressSize || counter.get() == maxIdSize || counter.get() == lastId);
                    if (counter.get() == maxIdSize / compressSize) {
                        batchEnd[3] = true;
                    } else if (counter.get() == maxIdSize) {
                        batchEnd[4] = true;
                    } else {
                        batchEnd[5] = true;
                    }
                });
                stream.addOnStreamEnd(() -> {
                    assertEquals(lastId, counter.get());
                    streamEnd[0] = true;
                });
                stream.addOnStreamEnd(() -> {
                    assertEquals(lastId, counter.get());
                    streamEnd[1] = true;
                });
                stream.forEach(rowAddress -> {
                    assertTrue(rowIdRepository.process(rowAddress.getId(), rowAddressProcessed -> assertEquals(rowAddress, rowAddressProcessed)));
                    counter.incrementAndGet();
                });
                assertEquals(lastId, counter.get());
                assertTrue(batchEnd[0]);
                assertTrue(batchEnd[1]);
                assertTrue(batchEnd[2]);
                assertTrue(batchEnd[3]);
                assertTrue(batchEnd[4]);
                assertTrue(batchEnd[5]);
                assertTrue(streamEnd[0]);
                assertTrue(streamEnd[1]);
            }
            {
                final Set<Integer> idSet = new HashSet<>(Arrays.asList(10, 55, 155, 44, 749, 750, 900));
                final AtomicInteger counter = new AtomicInteger();
                final StoppableBatchStream<RowAddress> stream = rowIdRepository.batchStream(idSet, RowIdRepository.ProcessType.Read);
                final boolean[] batchEnd = new boolean[]{false, false, false, false};
                final boolean[] streamEnd = new boolean[]{false, false};
                stream.addOnBatchEnd(() -> {
                    assertTrue(counter.get() == 4 || counter.get() == idSet.size() - 1);
                    if (counter.get() == 4) {
                        batchEnd[0] = true;
                    } else {
                        batchEnd[1] = true;
                    }
                });
                stream.addOnBatchEnd(() -> {
                    assertTrue(counter.get() == 4 || counter.get() == idSet.size() - 1);
                    if (counter.get() == 4) {
                        batchEnd[2] = true;
                    } else {
                        batchEnd[3] = true;
                    }
                });
                stream.addOnStreamEnd(() -> {
                    assertEquals(idSet.size() - 1, counter.get());
                    streamEnd[0] = true;
                });
                stream.addOnStreamEnd(() -> {
                    assertEquals(idSet.size() - 1, counter.get());
                    streamEnd[1] = true;
                });
                stream.forEach(rowAddress -> {
                    assertTrue(rowIdRepository.process(rowAddress.getId(), rowAddressProcessed -> assertEquals(rowAddress, rowAddressProcessed)));
                    counter.incrementAndGet();
                });
                assertEquals(idSet.size() - 1, counter.get());
                assertTrue(batchEnd[0]);
                assertTrue(batchEnd[1]);
                assertTrue(batchEnd[2]);
                assertTrue(batchEnd[3]);
                assertTrue(streamEnd[0]);
                assertTrue(streamEnd[1]);
            }
            {
                final AtomicInteger counter = new AtomicInteger();
                final StoppableStream<RowAddress> stream = rowIdRepository.batchStream();
                stream.forEach(rowAddress -> {
                    assertTrue(rowIdRepository.process(rowAddress.getId(), rowAddressProcessed -> assertEquals(rowAddress, rowAddressProcessed)));
                    if (counter.incrementAndGet() == 500) {
                        stream.stop();
                    }
                });
                assertEquals(500, counter.get());
            }
        });
    }

    @Test
    public void processTypeTest() throws InterruptedException {
        final int lastId = 750;
        createFiles(lastId);
        final RowIdRepository rowIdRepository = prepareRowIdRepository(maxIdSize);
        rowIdRepository.stop();
        Thread.sleep(3000);
        final ExecutorService executorService = Executors.newCachedThreadPool();
        {
            final StoppableBatchStream<RowAddress> stream = rowIdRepository.batchStream(new HashSet<>(Collections.singletonList(1)), RowIdRepository.ProcessType.Read);
            final Future<Void> future = executorService.submit(() -> {
                stream.forEach(rowAddress -> rowIdRepository.delete(rowAddress.getId()));
                return null;
            });
            try {
                future.get(1000, TimeUnit.MILLISECONDS);
                fail("never");
            } catch (InterruptedException | ExecutionException e) {
                fail("never");
            } catch (TimeoutException e) {
                assertTrue(true);
            }
        }
        {
            final StoppableBatchStream<RowAddress> stream = rowIdRepository.batchStream(new HashSet<>(Collections.singletonList(750)), RowIdRepository.ProcessType.Write);
            final Future<Void> future = executorService.submit(() -> {
                stream.forEach(rowAddress -> rowIdRepository.delete(rowAddress.getId()));
                return null;
            });
            try {
                future.get(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                fail("never");
            }
        }
    }

    @Test
    public void concurrentTest() {
        final int lastId = 750;
        final int max = 750;
        final int maxIdSize = 20;
        createFiles(lastId, maxIdSize);
        final RowIdRepository rowIdRepository = prepareRowIdRepository(maxIdSize);
        TestUtils.doAndSleep(rowIdRepository, () -> {
            final AtomicInteger count = new AtomicInteger();
            final Thread thread1 = new Thread(() -> {
                for (int i = 1; i < max; i++) {
                    int finalI = i;
                    rowIdRepository.save(i, rowAddress -> rowAddress.setSize(rowAddressSize + finalI));
                }
                System.out.println(Thread.currentThread().getName() + " finished");
            });
            final Thread thread2 = new Thread(() -> {
                for (int i = 1; i < max; i++) {
                    rowIdRepository.delete(i);
                }
                System.out.println(Thread.currentThread().getName() + " finished");
            });
            final Thread thread3 = new Thread(() -> {
                for (int i = 1; i < max; i++) {
                    rowIdRepository.batchStream().forEach(rowAddress -> {
                    });
                }
                System.out.println(Thread.currentThread().getName() + " finished");
            });
            final Thread destroyThread = new Thread(() -> {
                for (int i = 0; i < max; i++) {
                    rowIdRepository.destroy();
                }
                System.out.println(Thread.currentThread().getName() + " finished");
            });
            thread1.start();
            thread2.start();
            thread3.start();
            destroyThread.start();
            for (int i = 0; i < max; i++) {
                rowIdRepository.process(i, rowAddress -> count.incrementAndGet());
            }
            System.out.println("count " + count.get());
            System.out.println();
            count.set(0);
            try {
                thread1.join();
                thread2.join();
                thread3.join();
                destroyThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            for (int i = 0; i < 1000; i++) {
                rowIdRepository.process(i, rowAddress -> count.incrementAndGet());
            }
            System.out.println("count " + count.get());
            System.out.println();
            System.out.println("main thread finished");
        });
    }

    private void createFiles(int lastId) {
        createFiles(lastId, maxIdSize);
    }

    private void createFiles(int lastId, int maxIdSize) {
        Utils.createDirectoryTree(new File(filesIdPath));
        Utils.createDirectoryTree(new File(filesRowPath));
        TestUtils.createRowIdFiles(lastId, maxIdSize, compressSize, filesIdPath + "/" + filePath,
                filesIdPath + "/" + filesIdPath, filesRowPath + "/" + filesRowPath, rowAddressSize, null);
    }

    private RowIdRepository prepareRowIdRepository(int maxIdSize) {
        return TestUtils.prepareRowIdRepository("", new DestroyServiceImpl(1000), maxIdSize, compressSize, new ObjectConverterImpl(new DataCompressorImpl()));
    }

}
