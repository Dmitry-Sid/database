import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import server.model.RowIdRepository;
import server.model.Utils;
import server.model.impl.DataCompressorImpl;
import server.model.impl.DestroyServiceImpl;
import server.model.impl.ObjectConverterImpl;
import server.model.pojo.RowAddress;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
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
        {
            TestUtils.doAndSleep(rowIdRepository, () -> {
                rowIdRepository.add(rowIdRepository.newId(), rowAddress -> {
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
                rowIdRepository.add(rowIdRepository.newId(), rowAddress -> {
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
                rowIdRepository.add(501, rowAddress -> {
                    rowAddress.setSize(14);
                });
                rowIdRepository.add(250, rowAddress -> {
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
                rowIdRepository.add(100, rowAddress -> {
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
            rowIdRepository.add(rowIdRepository.newId(), rowAddress -> {
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
                rowIdRepository.stream(rowAddress -> {
                    assertTrue(rowIdRepository.process(rowAddress.getId(), rowAddressProcessed -> {
                        assertEquals(rowAddress, rowAddressProcessed);
                    }));
                    counter.incrementAndGet();
                }, null, null);
                assertEquals(lastId, counter.get());
            }
            {
                final Set<Integer> idSet = new HashSet<>(Arrays.asList(10, 55, 155, 44, 749, 750, 900));
                final AtomicInteger counter = new AtomicInteger();
                rowIdRepository.stream(rowAddress -> {
                    assertTrue(rowIdRepository.process(rowAddress.getId(), rowAddressProcessed -> {
                        assertEquals(rowAddress, rowAddressProcessed);
                    }));
                    counter.incrementAndGet();
                }, null, idSet);
                assertEquals(idSet.size() - 1, counter.get());
            }
            {
                final AtomicBoolean stopChecker = new AtomicBoolean();
                final AtomicInteger counter = new AtomicInteger();
                rowIdRepository.stream(rowAddress -> {
                    assertTrue(rowIdRepository.process(rowAddress.getId(), rowAddressProcessed -> {
                        assertEquals(rowAddress, rowAddressProcessed);
                    }));
                    if (counter.incrementAndGet() == 500) {
                        stopChecker.set(true);
                    }
                }, stopChecker, null);
                assertEquals(500, counter.get());
            }
        });
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
                    rowIdRepository.add(i, rowAddress -> rowAddress.setSize(rowAddressSize + finalI));
                }
                System.out.println(Thread.currentThread().getName() + " finished");
            });
            final Thread thread2 = new Thread(() -> {
                for (int i = 1; i < max; i++) {
                    rowIdRepository.delete(i);
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
