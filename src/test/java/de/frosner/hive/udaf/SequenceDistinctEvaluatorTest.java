package de.frosner.hive.udaf;

import de.frosner.hive.datastructure.OrderedPair;
import org.junit.Before;
import org.junit.Test;

import de.frosner.hive.udaf.SequenceDistinct.SequenceDistinctEvaluator;

import static org.fest.assertions.Assertions.assertThat;

public class SequenceDistinctEvaluatorTest {

    private SequenceDistinctEvaluator evaluator;

    @Before
    public void initEvaluator() {
        evaluator = new SequenceDistinctEvaluator();
        evaluator.init();
    }

    @Test
    public void testTerminatePartial() {
        evaluator.iterate("v2", "b");
        evaluator.iterate("v3", "c");
        evaluator.iterate("v1", "a");
        assertThat(evaluator.terminatePartial()).containsExactly(
                new OrderedPair<>("v2", "b"),
                new OrderedPair<>("v3", "c"),
                new OrderedPair<>("v1", "a")
        );
    }

    @Test
    public void testTerminate_sortingButNoDistinct() {
        evaluator.iterate("v2", "b");
        evaluator.iterate("v3", "c");
        evaluator.iterate("v1", "a");
        assertThat(evaluator.terminate()).containsExactly("v1", "v2", "v3");
    }

    @Test
    public void testTerminate_sortingAndDistinct() {
        evaluator.iterate("v1", "a");
        evaluator.iterate("v2", "c");
        evaluator.iterate("v3", "d");
        evaluator.iterate("v1", "b");
        evaluator.iterate("v3", "e");
        assertThat(evaluator.terminate()).containsExactly("v1", "v2", "v3");
    }

    @Test
    public void testMerge() {
        evaluator.iterate("v1", "c");
        evaluator.iterate("v1", "d");
        SequenceDistinctEvaluator anotherEvaluator = new SequenceDistinctEvaluator();
        anotherEvaluator.init();
        anotherEvaluator.iterate("v1", "a");
        anotherEvaluator.iterate("v2", "b");
        evaluator.merge(anotherEvaluator.terminatePartial());
        assertThat(evaluator.terminatePartial()).containsExactly(
                new OrderedPair<>("v1", "c"),
                new OrderedPair<>("v1", "d"),
                new OrderedPair<>("v1", "a"),
                new OrderedPair<>("v2", "b")
        );
        assertThat(evaluator.terminate()).containsExactly("v1", "v2", "v1");
    }

}
