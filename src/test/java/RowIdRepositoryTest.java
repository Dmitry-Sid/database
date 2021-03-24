import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import server.model.RowIdRepository;
import server.model.pojo.RowAddress;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class RowIdRepositoryTest {
    private RowIdRepository rowIdRepository;
    private static final String fileName = "rowIdVariables.test";
    private static final String filesIdPath = "rowId";
    private static final String filesRowPath = "row";
    private static final int maxIdSize = 500;
    private static final int compressSize = 2;
    private static final int rowAddressSize = 5;

    @Before
    public void before() {
        createFiles(0);
        rowIdRepository = prepareRowIdManager();
    }

    @After
    public void after() {
        new File(fileName).delete();
        new File(filesIdPath + 1).delete();
    }

    @Test
    public void newIdTest() {
        final int lastId = 750;
        createFiles(lastId);
        rowIdRepository = prepareRowIdManager();
        try {
            assertEquals(751, rowIdRepository.newId());
            assertEquals(752, rowIdRepository.newId());
        } finally {
            for (Integer value : TestUtils.prepareBoundsBatch(lastId, maxIdSize)) {
                new File(filesIdPath + value).delete();
            }
        }
    }

    @Test
    public void processTest() {
        final int lastId = 750;
        createFiles(lastId);
        rowIdRepository = prepareRowIdManager();
        try {
            assertFalse(rowIdRepository.process(0, rowAddress -> {
            }));
            assertFalse(rowIdRepository.process(1200, rowAddress -> {
            }));
            assertTrue(rowIdRepository.process(1, rowAddress -> {
                assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                assertEquals(1, rowAddress.getId());
                assertEquals(0, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 1, 2, 5, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            assertTrue(rowIdRepository.process(250, rowAddress -> {
                assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                assertEquals(250, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertNull(rowAddress.getNext());
            }));
            assertTrue(rowIdRepository.process(499, rowAddress -> {
                assertEquals(filesRowPath + 2, rowAddress.getFilePath());
                assertEquals(499, rowAddress.getId());
                assertEquals(1240, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 2, 500, 1245, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            assertTrue(rowIdRepository.process(500, rowAddress -> {
                assertEquals(filesRowPath + 2, rowAddress.getFilePath());
                assertEquals(500, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertNull(rowAddress.getNext());
            }));
            assertTrue(rowIdRepository.process(501, rowAddress -> {
                assertEquals(filesRowPath + 3, rowAddress.getFilePath());
                assertEquals(501, rowAddress.getId());
                assertEquals(0, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 3, 502, 5, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            assertTrue(rowIdRepository.process(749, rowAddress -> {
                assertEquals(filesRowPath + 3, rowAddress.getFilePath());
                assertEquals(749, rowAddress.getId());
                assertEquals(1240, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 3, 750, 1245, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            assertTrue(rowIdRepository.process(750, rowAddress -> {
                assertEquals(filesRowPath + 3, rowAddress.getFilePath());
                assertEquals(750, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertNull(rowAddress.getNext());
            }));
            assertFalse(rowIdRepository.process(751, rowAddress -> {
            }));
        } finally {
            for (Integer value : TestUtils.prepareBoundsBatch(lastId, maxIdSize)) {
                new File(filesIdPath + value).delete();
            }
        }
    }

    @Test
    public void addTest() {
        final int lastId = 750;
        createFiles(lastId);
        rowIdRepository = prepareRowIdManager();
        try {
            {
                rowIdRepository.add(rowIdRepository.newId(), rowAddress -> {
                    rowAddress.setSize(10);
                });
                assertTrue(rowIdRepository.process(250, rowAddress -> {
                    assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                    assertEquals(250, rowAddress.getId());
                    assertEquals(1245, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    assertNull(rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(750, rowAddress -> {
                    assertEquals(filesRowPath + 3, rowAddress.getFilePath());
                    assertEquals(750, rowAddress.getId());
                    assertEquals(1245, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    assertNull(rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(751, rowAddress -> {
                    assertEquals(filesRowPath + 4, rowAddress.getFilePath());
                    assertEquals(751, rowAddress.getId());
                    assertEquals(0, rowAddress.getPosition());
                    assertEquals(10, rowAddress.getSize());
                    assertNull(rowAddress.getPrevious());
                    assertNull(rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(250, rowAddress -> {
                    assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                    assertEquals(250, rowAddress.getId());
                    assertEquals(1245, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    assertNull(rowAddress.getNext());
                }));
                rowIdRepository.add(rowIdRepository.newId(), rowAddress -> {
                    rowAddress.setSize(67);
                });
                assertTrue(rowIdRepository.process(250, rowAddress -> {
                    assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                    assertEquals(250, rowAddress.getId());
                    assertEquals(1245, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    assertNull(rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(750, rowAddress -> {
                    assertEquals(filesRowPath + 3, rowAddress.getFilePath());
                    assertEquals(750, rowAddress.getId());
                    assertEquals(1245, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    assertNull(rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(751, rowAddress -> {
                    assertEquals(filesRowPath + 4, rowAddress.getFilePath());
                    assertEquals(751, rowAddress.getId());
                    assertEquals(0, rowAddress.getPosition());
                    assertEquals(10, rowAddress.getSize());
                    assertNull(rowAddress.getPrevious());
                    final RowAddress rowAddressNext = new RowAddress(filesRowPath + 4, 752, 10, 67);
                    assertEquals(rowAddressNext, rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(752, rowAddress -> {
                    assertEquals(filesRowPath + 4, rowAddress.getFilePath());
                    assertEquals(752, rowAddress.getId());
                    assertEquals(10, rowAddress.getPosition());
                    assertEquals(67, rowAddress.getSize());
                    final RowAddress rowAddressPrevious = new RowAddress(filesRowPath + 4, 751, 0, 10);
                    assertEquals(rowAddressPrevious, rowAddress.getPrevious());
                    assertNull(rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(250, rowAddress -> {
                    assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                    assertEquals(250, rowAddress.getId());
                    assertEquals(1245, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    assertNull(rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(752, rowAddress -> {
                    assertEquals(filesRowPath + 4, rowAddress.getFilePath());
                    assertEquals(752, rowAddress.getId());
                    assertEquals(10, rowAddress.getPosition());
                    assertEquals(67, rowAddress.getSize());
                    final RowAddress rowAddressPrevious = new RowAddress(filesRowPath + 4, 751, 0, 10);
                    assertEquals(rowAddressPrevious, rowAddress.getPrevious());
                    assertNull(rowAddress.getNext());
                }));
            }
            {
                rowIdRepository.add(501, rowAddress -> {
                    rowAddress.setSize(14);
                });
                rowIdRepository.add(250, rowAddress -> {
                    rowAddress.setSize(7);
                });
                assertTrue(rowIdRepository.process(501, rowAddress -> {
                    assertEquals(filesRowPath + 3, rowAddress.getFilePath());
                    assertEquals(501, rowAddress.getId());
                    assertEquals(0, rowAddress.getPosition());
                    assertEquals(14, rowAddress.getSize());
                    final RowAddress rowAddressNext = new RowAddress(filesRowPath + 3, 502, 14, rowAddressSize);
                    assertEquals(rowAddressNext, rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(650, rowAddress -> {
                    assertEquals(filesRowPath + 3, rowAddress.getFilePath());
                    assertEquals(650, rowAddress.getId());
                    assertEquals(754, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    final RowAddress rowAddressNext = new RowAddress(filesRowPath + 3, 651, 759, rowAddressSize);
                    assertEquals(rowAddressNext, rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(750, rowAddress -> {
                    assertEquals(filesRowPath + 3, rowAddress.getFilePath());
                    assertEquals(750, rowAddress.getId());
                    assertEquals(1254, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    assertNull(rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(1, rowAddress -> {
                    assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                    assertEquals(1, rowAddress.getId());
                    assertEquals(0, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    final RowAddress rowAddressNext = new RowAddress(filesRowPath + 1, 2, 5, rowAddressSize);
                    assertEquals(rowAddressNext, rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(250, rowAddress -> {
                    assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                    assertEquals(250, rowAddress.getId());
                    assertEquals(1245, rowAddress.getPosition());
                    assertEquals(7, rowAddress.getSize());
                    assertNull(rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(499, rowAddress -> {
                    assertEquals(filesRowPath + 2, rowAddress.getFilePath());
                    assertEquals(499, rowAddress.getId());
                    assertEquals(1240, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    final RowAddress rowAddressNext = new RowAddress(filesRowPath + 2, 500, 1245, rowAddressSize);
                    assertEquals(rowAddressNext, rowAddress.getNext());
                }));
                rowIdRepository.add(100, rowAddress -> {
                    rowAddress.setSize(15);
                });
                assertTrue(rowIdRepository.process(99, rowAddress -> {
                    assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                    assertEquals(99, rowAddress.getId());
                    assertEquals(490, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    final RowAddress rowAddressNext = new RowAddress(filesRowPath + 1, 100, 495, 15);
                    assertEquals(rowAddressNext, rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(100, rowAddress -> {
                    assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                    assertEquals(100, rowAddress.getId());
                    assertEquals(495, rowAddress.getPosition());
                    assertEquals(15, rowAddress.getSize());
                    final RowAddress rowAddressNext = new RowAddress(filesRowPath + 1, 101, 510, rowAddressSize);
                    assertEquals(rowAddressNext, rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(250, rowAddress -> {
                    assertEquals(filesRowPath + 1, rowAddress.getFilePath());
                    assertEquals(250, rowAddress.getId());
                    assertEquals(1255, rowAddress.getPosition());
                    assertEquals(7, rowAddress.getSize());
                    assertNull(rowAddress.getNext());
                }));
                assertTrue(rowIdRepository.process(251, rowAddress -> {
                    assertEquals(filesRowPath + 2, rowAddress.getFilePath());
                    assertEquals(251, rowAddress.getId());
                    assertEquals(0, rowAddress.getPosition());
                    assertEquals(rowAddressSize, rowAddress.getSize());
                    assertNull(rowAddress.getPrevious());
                    final RowAddress rowAddressNext = new RowAddress(filesRowPath + 2, 252, 5, rowAddressSize);
                    assertEquals(rowAddressNext, rowAddress.getNext());
                }));
            }
        } finally {
            for (Integer value : TestUtils.prepareBoundsBatch(752, maxIdSize)) {
                new File(filesIdPath + value).delete();
            }
        }
    }

    @Test
    public void deleteTest() {
        final int lastId = 750;
        createFiles(lastId);
        rowIdRepository = prepareRowIdManager();
        try {
            assertTrue(rowIdRepository.process(300, rowAddress -> {
                assertEquals(filesRowPath + 2, rowAddress.getFilePath());
                assertEquals(300, rowAddress.getId());
                assertEquals(245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressPrevious = new RowAddress(filesRowPath + 2, 299, 240, rowAddressSize);
                assertEquals(rowAddressPrevious, rowAddress.getPrevious());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 2, 301, 250, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            rowIdRepository.delete(300);
            assertFalse(rowIdRepository.process(300, rowAddress -> {

            }));
            assertTrue(rowIdRepository.process(750, rowAddress -> {
                assertEquals(filesRowPath + 3, rowAddress.getFilePath());
                assertEquals(750, rowAddress.getId());
                assertEquals(1245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                assertNull(rowAddress.getNext());
            }));
            assertTrue(rowIdRepository.process(299, rowAddress -> {
                assertEquals(filesRowPath + 2, rowAddress.getFilePath());
                assertEquals(299, rowAddress.getId());
                assertEquals(240, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressPrevious = new RowAddress(filesRowPath + 2, 298, 235, rowAddressSize);
                assertEquals(rowAddressPrevious, rowAddress.getPrevious());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 2, 301, 245, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            assertTrue(rowIdRepository.process(301, rowAddress -> {
                assertEquals(filesRowPath + 2, rowAddress.getFilePath());
                assertEquals(301, rowAddress.getId());
                assertEquals(245, rowAddress.getPosition());
                assertEquals(rowAddressSize, rowAddress.getSize());
                final RowAddress rowAddressPrevious = new RowAddress(filesRowPath + 2, 299, 240, rowAddressSize);
                assertEquals(rowAddressPrevious, rowAddress.getPrevious());
                final RowAddress rowAddressNext = new RowAddress(filesRowPath + 2, 302, 250, rowAddressSize);
                assertEquals(rowAddressNext, rowAddress.getNext());
            }));
            rowIdRepository.delete(750);
            final int[] id = new int[1];
            rowIdRepository.add(rowIdRepository.newId(), rowAddress -> {
                id[0] = rowAddress.getId();
            });
            assertEquals(751, id[0]);
        } finally {
            for (Integer value : TestUtils.prepareBoundsBatch(lastId, maxIdSize)) {
                new File(filesIdPath + value).delete();
            }
        }
    }

    @Test
    public void streamTest() {
        final int lastId = 750;
        createFiles(lastId);
        rowIdRepository = prepareRowIdManager();
        try {
            {
                final AtomicInteger counter = new AtomicInteger();
                rowIdRepository.stream(rowAddress -> {
                    assertTrue(rowIdRepository.process(rowAddress.getId(), rowAddressProcessed -> {
                        assertEquals(rowAddress, rowAddressProcessed);
                    }));
                    counter.incrementAndGet();
                }, new AtomicBoolean(false), null);
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
                }, new AtomicBoolean(false), idSet);
                assertEquals(idSet.size() - 1, counter.get());
            }
        } finally {
            for (Integer value : TestUtils.prepareBoundsBatch(lastId, maxIdSize)) {
                new File(filesIdPath + value).delete();
            }
        }
    }

    private void createFiles(int lastId) {
        TestUtils.createIdFiles(lastId, maxIdSize, compressSize, fileName, filesIdPath, filesRowPath, rowAddressSize, null);
    }

    private RowIdRepository prepareRowIdManager() {
        return TestUtils.prepareRowIdManager(fileName, filesIdPath, filesRowPath, maxIdSize, compressSize);
    }

}
