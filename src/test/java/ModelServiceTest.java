import org.junit.Test;
import server.model.ModelService;
import server.model.impl.DataCompressorImpl;
import server.model.impl.ModelServiceImpl;
import server.model.impl.ObjectConverterImpl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class ModelServiceTest {

    @Test
    public void addAndContainsTest() {
        final ModelService modelService = new ModelServiceImpl("", true, new ObjectConverterImpl(new DataCompressorImpl()), null);
        modelService.add("test1", Integer.class);
        modelService.add("test2", String.class);
        assertEquals(new HashSet<>(Arrays.asList("test1", "test2")), modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
        assertTrue(modelService.contains("test1"));
        assertTrue(modelService.contains("test2"));
        assertFalse(modelService.contains("test3"));
    }

    @Test
    public void getValueTest() {
        final ModelService modelService = new ModelServiceImpl("", true, new ObjectConverterImpl(new DataCompressorImpl()), null);
        modelService.add("test1", Byte.class);
        modelService.add("test2", Character.class);
        modelService.add("test3", Short.class);
        modelService.add("test4", Integer.class);
        modelService.add("test5", Long.class);
        modelService.add("test6", Float.class);
        modelService.add("test7", Double.class);
        modelService.add("test8", String.class);
        assertEquals((byte) 5, modelService.getValue("test1", "5"));
        assertEquals('c', modelService.getValue("test2", "c"));
        assertEquals((short) 10, modelService.getValue("test3", "10"));
        assertEquals(15, modelService.getValue("test4", "15"));
        assertEquals((long) 20, modelService.getValue("test5", "20"));
        assertEquals((float) 25.5, modelService.getValue("test6", "25.5"));
        assertEquals(38.3, modelService.getValue("test7", "38.3"));
        assertEquals("abc", modelService.getValue("test8", "abc"));
    }

    @Test
    public void deleteTest() {
        final ModelService modelService = new ModelServiceImpl("", true, new ObjectConverterImpl(new DataCompressorImpl()), null);
        modelService.add("test1", Byte.class);
        modelService.add("test2", Character.class);
        modelService.add("test3", Short.class);
        assertEquals(new HashSet<>(Arrays.asList("test1", "test2", "test3")), modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
        assertTrue(modelService.contains("test1"));
        assertTrue(modelService.contains("test2"));
        assertTrue(modelService.contains("test3"));
        modelService.delete("test1");
        assertFalse(modelService.contains("test1"));
        assertTrue(modelService.contains("test2"));
        assertTrue(modelService.contains("test3"));
        assertEquals(new HashSet<>(Arrays.asList("test2", "test3")), modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
        modelService.delete("test2");
        assertFalse(modelService.contains("test1"));
        assertFalse(modelService.contains("test2"));
        assertTrue(modelService.contains("test3"));
        assertEquals(new HashSet<>(Collections.singletonList("test3")), modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
        modelService.delete("test2");
        assertFalse(modelService.contains("test1"));
        assertFalse(modelService.contains("test2"));
        assertTrue(modelService.contains("test3"));
        assertEquals(new HashSet<>(Collections.singletonList("test3")), modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
    }

    @Test
    public void addIndexTest() {
        final ModelService modelService = new ModelServiceImpl("", true, new ObjectConverterImpl(new DataCompressorImpl()), null);
        modelService.add("test1", Byte.class);
        modelService.add("test2", Character.class);
        modelService.add("test3", Short.class);
        assertEquals(new HashSet<>(Collections.emptySet()), modelService.getIndexedFields());
        modelService.addIndex("test2");
        assertEquals(new HashSet<>(Arrays.asList("test1", "test2", "test3")), modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Collections.singletonList("test2")), modelService.getIndexedFields());
        modelService.addIndex("test3");
        assertEquals(new HashSet<>(Arrays.asList("test1", "test2", "test3")), modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Arrays.asList("test2", "test3")), modelService.getIndexedFields());
    }

    @Test
    public void deleteIndexTest() {
        final ModelService modelService = new ModelServiceImpl("", true, new ObjectConverterImpl(new DataCompressorImpl()), null);
        modelService.add("test1", Byte.class);
        modelService.add("test2", Character.class);
        modelService.add("test3", Short.class);
        assertEquals(new HashSet<>(Collections.emptySet()), modelService.getIndexedFields());
        modelService.addIndex("test2");
        assertEquals(new HashSet<>(Arrays.asList("test1", "test2", "test3")), modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Collections.singletonList("test2")), modelService.getIndexedFields());
        modelService.addIndex("test3");
        assertEquals(new HashSet<>(Arrays.asList("test1", "test2", "test3")), modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Arrays.asList("test2", "test3")), modelService.getIndexedFields());
        modelService.deleteIndex("test3");
        assertEquals(new HashSet<>(Arrays.asList("test1", "test2", "test3")), modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Collections.singletonList("test2")), modelService.getIndexedFields());
        modelService.deleteIndex("test1");
        assertEquals(new HashSet<>(Arrays.asList("test1", "test2", "test3")), modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Collections.singletonList("test2")), modelService.getIndexedFields());
        modelService.deleteIndex("test2");
        assertEquals(new HashSet<>(Collections.emptySet()), modelService.getIndexedFields());
    }

    @Test
    public void subscribeOnFieldsChangesTest() {
        final ModelService modelService = new ModelServiceImpl("", true, new ObjectConverterImpl(new DataCompressorImpl()), null);
        final AtomicReference<Set<String>> atomicReference = new AtomicReference<>();
        modelService.subscribeOnFieldsChanges(atomicReference::set);
        modelService.add("test1", Byte.class);
        assertEquals(atomicReference.get(), modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
        modelService.add("test2", Short.class);
        assertEquals(atomicReference.get(), modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
        modelService.addIndex("test1");
        assertEquals(atomicReference.get(), modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
        modelService.addIndex("test2");
        assertEquals(atomicReference.get(), modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
        modelService.delete("test1");
        assertEquals(atomicReference.get(), modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
        modelService.delete("test2");
        assertEquals(atomicReference.get(), modelService.getFields().stream().map(ModelService.FieldInfo::getName).collect(Collectors.toSet()));
    }

    @Test
    public void subscribeOnIndexesChangesTest() {
        final ModelService modelService = new ModelServiceImpl("", true, new ObjectConverterImpl(new DataCompressorImpl()), null);
        final AtomicReference<Set<String>> atomicReference = new AtomicReference<>();
        modelService.subscribeOnIndexesChanges(atomicReference::set);
        modelService.add("test1", Byte.class);
        assertEquals(new HashSet<>(), modelService.getIndexedFields());
        modelService.add("test2", Short.class);
        assertEquals(new HashSet<>(), modelService.getIndexedFields());
        modelService.addIndex("test1");
        assertEquals(atomicReference.get(), modelService.getIndexedFields());
        modelService.addIndex("test2");
        assertEquals(atomicReference.get(), modelService.getIndexedFields());
        modelService.deleteIndex("test1");
        assertEquals(atomicReference.get(), modelService.getIndexedFields());
        modelService.deleteIndex("test2");
        assertEquals(atomicReference.get(), modelService.getIndexedFields());
        modelService.delete("test1");
        assertEquals(atomicReference.get(), modelService.getIndexedFields());
        modelService.delete("test2");
        assertEquals(atomicReference.get(), modelService.getIndexedFields());
    }

}
