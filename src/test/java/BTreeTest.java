import org.junit.Test;
import server.model.ObjectConverter;
import server.model.impl.BTree;
import server.model.impl.ObjectConverterImpl;
import server.model.pojo.Pair;

import java.util.Collections;

public class BTreeTest {

    @Test
    public void test() {
        final ObjectConverter objectConverter = new ObjectConverterImpl();
        final BTree<Integer, Integer> tree = new BTree<>("int", "test", new ObjectConverterImpl(), 2);
        tree.variables.root.pairs.add(new Pair<>(1, Collections.emptySet()));
        tree.variables.root.pairs.add(new Pair<>(2, Collections.emptySet()));
        tree.variables.root.leaf = false;
        for (int i = 1; i <= 3; i++) {
            final BTree.Node<Integer, Integer> node = new BTree.Node<>(String.valueOf(i));
            node.init();
            for (int j = 1; j < 5; j++) {
                if (j < 4) {
                    node.pairs.add(new Pair<>(i + j * 10, Collections.emptySet()));
                }
                node.children.add(String.valueOf(i + j * 10));
            }
            node.leaf = false;
            tree.variables.root.children.add(node.fileName);
            objectConverter.toFile(node, node.fileName);
        }
        tree.split(tree.variables.root, 2);
        final BTree.Node<Integer, Integer> node1 = tree.read(tree.variables.root.children.get(0));
        final BTree.Node<Integer, Integer> node2 = tree.read(tree.variables.root.children.get(1));
        final BTree.Node<Integer, Integer> node3 = tree.read(tree.variables.root.children.get(2));
        final BTree.Node<Integer, Integer> node4 = tree.read(tree.variables.root.children.get(3));
        System.out.println("sss");

        {
            int index = 10;
            for (; index >= 0; index--) {
                if (index == 10) {
                    return;
                }
            }
            System.out.println(index);
        }
        {
            int index = 0;
            for (; index < 10; index++) {

            }
            System.out.println(index);
        }

    }
}
