package de.frosner.hive.udaf;

import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.MapAssert.entry;

import org.junit.Before;
import org.junit.Test;

import de.frosner.hive.udaf.Mode.ModeEvaluator;

public class ModeEvaluatorTest {

    private ModeEvaluator evaluator;

    @Before
    public void initEvaluator() {
        evaluator = new ModeEvaluator();
        evaluator.init();
    }

    @Test
    public void testTerminatePartial() {
        evaluator.iterate("a");
        evaluator.iterate("a");
        evaluator.iterate("b");
        assertThat(evaluator.terminatePartial()).includes(entry("a", 2), entry("b", 1));
    }

    @Test
    public void testTerminate() {
        evaluator.iterate("a");
        evaluator.iterate("b");
        evaluator.iterate("b");
        assertThat(evaluator.terminate()).isEqualTo("b");
    }

    @Test
    public void testMerge() {
        ModeEvaluator anotherEvaluator = new ModeEvaluator();
        anotherEvaluator.init();
        anotherEvaluator.iterate("c");
        anotherEvaluator.iterate("b");
        evaluator.iterate("a");
        evaluator.iterate("b");
        evaluator.merge(anotherEvaluator.terminatePartial());
        assertThat(evaluator.terminatePartial()).includes(entry("a", 1), entry("b", 2), entry("c", 1));
        assertThat(evaluator.terminate()).isEqualTo("b");
    }

}
