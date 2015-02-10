package de.frosner.hive.datastructure;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

import java.util.Comparator;

public class OrderedPairTest {

    @Test
    public void testComparator_objectsNotEqual() {
        OrderedPair<String, String> pair1 = new OrderedPair<>("value1", "a");
        OrderedPair<String, String> pair2 = new OrderedPair<>("value2", "b");
        Comparator<OrderedPair> comparator = OrderedPair.comparator();
        assertThat(comparator.compare(pair1, pair2)).isLessThan(0);
        assertThat(comparator.compare(pair2, pair1)).isGreaterThan(0);
    }

    @Test
    public void testComparator_objectsEqual() {
        OrderedPair<String, String> pair1 = new OrderedPair<>("value1", "a");
        OrderedPair<String, String> pair2 = new OrderedPair<>("value2", "a");
        Comparator<OrderedPair> comparator = OrderedPair.comparator();
        assertThat(comparator.compare(pair1, pair2)).isEqualTo(0);
        assertThat(comparator.compare(pair2, pair1)).isEqualTo(0);
    }

}
