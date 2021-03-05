import org.junit.Test;
import sample.model.BinaryTree;
import sample.model.ConditionServiceImpl;
import sample.model.ModelServiceImpl;
import sample.model.pojo.ICondition;
import sample.model.pojo.SimpleCondition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BinaryTreeTest {

    @Test
    public void insertTest() {
        final BinaryTree<Integer, Integer> tree = prepareBinaryTree(Integer.class, "int");
        assertEquals(Collections.emptySet(), tree.search(10));
        tree.insert(10, 1);
        tree.insert(10, 2);
        {
            final List<Integer> list = new ArrayList(tree.search(10));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(1, 2), list);
        }
        tree.insert(11, 3);
        tree.insert(10, 3);
        tree.insert(9, 4);
        {
            final List<Integer> list = new ArrayList(tree.search(10));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(1, 2, 3), list);
        }
        {
            final List<Integer> list = new ArrayList(tree.search(11));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(3), list);
        }
        {
            final List<Integer> list = new ArrayList(tree.search(9));
            list.sort(Integer::compareTo);
            assertEquals(Arrays.asList(4), list);
        }
    }

    @Test
    public void searchTest() {
        {
            final BinaryTree<Integer, Integer> tree = prepareBinaryTree(Integer.class, "int");
            assertEquals(Collections.emptySet(), tree.search(10));
            tree.insert(10, 1);
            tree.insert(8, 2);
            tree.insert(9, 3);
            tree.insert(6, 4);
            tree.insert(7, 5);
            tree.insert(12, 6);
            tree.insert(11, 7);
            tree.insert(14, 8);
            tree.insert(13, 9);
            {
                final List<Integer> list = new ArrayList(tree.search(new SimpleCondition(ICondition.SimpleType.EQ, "int", 6)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(4), list);
            }
            {
                final List<Integer> list = new ArrayList(tree.search(new SimpleCondition(ICondition.SimpleType.NOT, "int", 6)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 3, 5, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(tree.search(new SimpleCondition(ICondition.SimpleType.GT, "int", 9)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(tree.search(new SimpleCondition(ICondition.SimpleType.GTE, "int", 9)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 3, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(tree.search(new SimpleCondition(ICondition.SimpleType.LT, "int", 10)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(2, 3, 4, 5), list);
            }
            {
                final List<Integer> list = new ArrayList(tree.search(new SimpleCondition(ICondition.SimpleType.LTE, "int", 10)));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 3, 4, 5), list);
            }
        }
        {
            final BinaryTree<String, Integer> tree = prepareBinaryTree(String.class, "String");
            assertEquals(Collections.emptySet(), tree.search("test10"));
            tree.insert("test10", 1);
            tree.insert("test8", 2);
            tree.insert("test9", 3);
            tree.insert("test6", 4);
            tree.insert("test7", 5);
            tree.insert("test12", 6);
            tree.insert("test11", 7);
            tree.insert("test14", 8);
            tree.insert("test13", 9);
            {
                final List<Integer> list = new ArrayList(tree.search(new SimpleCondition(ICondition.SimpleType.EQ, "String", "test6")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(4), list);
            }
            {
                final List<Integer> list = new ArrayList(tree.search(new SimpleCondition(ICondition.SimpleType.NOT, "String", "test6")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 3, 5, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(tree.search(new SimpleCondition(ICondition.SimpleType.LT, "String", "test9")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 4, 5, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(tree.search(new SimpleCondition(ICondition.SimpleType.LTE, "String", "test9")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(tree.search(new SimpleCondition(ICondition.SimpleType.GT, "String", "test10")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(2, 3, 4, 5, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(tree.search(new SimpleCondition(ICondition.SimpleType.GTE, "String", "test10")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), list);
            }
            {
                final List<Integer> list = new ArrayList(tree.search(new SimpleCondition(ICondition.SimpleType.LIKE, "String", "test1")));
                list.sort(Integer::compareTo);
                assertEquals(Arrays.asList(1, 6, 7, 8, 9), list);
            }
        }
    }

    private <T extends Comparable> BinaryTree<T, Integer> prepareBinaryTree(Class<T> clazz, String fieldName) {
        return new BinaryTree<>(new ConditionServiceImpl(new ModelServiceImpl()), fieldName);
    }

}
