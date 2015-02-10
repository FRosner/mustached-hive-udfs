package de.frosner.hive.udaf;

import de.frosner.hive.datastructure.OrderedPair;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.*;

@Description(name = "SequenceDistinct", value = "_FUNC_(sequenceColumn, sortColumn) - Aggregates all column values of" +
        " this group to a sequence while removing consecutive elements that are equal. The sequence is formed by" +
        " sorting the values according to the second argument. Example: {key:a, values:[a,a,b,c,a]} => " +
        "{key:a, values:[a,b,c,a]}.")
public final class SequenceDistinct extends UDAF {

    public static class SequenceDistinctEvaluator implements UDAFEvaluator {

        private List<OrderedPair<String, Comparable>> sequence;

        public SequenceDistinctEvaluator() {
            super();
            init();
        }

        public void init() {
            sequence = new ArrayList<>();
        }

        public boolean iterate(String value, Comparable orderValue) {
            if (orderValue == null || value == null) {
                return false;
            }
            return sequence.add(new OrderedPair<String, Comparable>(value, orderValue));
        }

        public List<OrderedPair<String, Comparable>> terminatePartial() {
           return sequence;
        }

        public boolean merge(List<OrderedPair<String, Comparable>> intermediateResult) {
            return sequence.addAll(intermediateResult);
        }

        public List<String> terminate() {
            Collections.sort(sequence, OrderedPair.comparator());

            List<String> distinctSequence = new ArrayList<>();
            switch (sequence.size()) {
                case 1: distinctSequence.add(sequence.get(0).getValue());
                case 0: return distinctSequence;
            }

            String lastValue = null;
            for (OrderedPair<String, Comparable> event : sequence) {
                if (lastValue == null) {
                    lastValue = event.getValue();
                    distinctSequence.add(lastValue);
                } else {
                    String currentValue = event.getValue();
                    if (!currentValue.equals(lastValue)) {
                        distinctSequence.add(currentValue);
                        lastValue = currentValue;
                    }
                }
            }
            return distinctSequence;
        }

    }

}
