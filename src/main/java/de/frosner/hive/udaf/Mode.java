package de.frosner.hive.udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.HashMap;
import java.util.Map;

@Description(name = "Mode", value = "_FUNC_(column) - Computes the statistical mode of a column inside each group. " +
        "The mode is defined as the value with the highest frequency. If there is no unique mode, the function returns " +
        "one of the possible values.")
public final class Mode extends UDAF {

    public static class ModeEvaluator implements UDAFEvaluator {

        private Map<String, Integer> counts;

        public ModeEvaluator() {
            super();
            init();
        }

        public void init() {
            counts = new HashMap<>();
        }

        public boolean iterate(String value) {
            if (value == null) {
                return true;
            }

            int currentCount = 0;
            if (counts.containsKey(value)) {
                currentCount = counts.get(value);
            }
            counts.put(value, currentCount + 1);

            return true;
        }

        public Map<String, Integer> terminatePartial() {
           return counts;
        }

        public boolean merge(Map<String, Integer> intermediateResult) {
            for (Map.Entry<String, Integer> intermediateEntry : intermediateResult.entrySet()) {
                String currentKey = intermediateEntry.getKey();
                Integer currentValue = intermediateEntry.getValue();
                if (counts.containsKey(currentKey)) {
                    counts.put(currentKey, counts.get(currentKey) + currentValue);
                } else {
                    counts.put(currentKey, currentValue);
                }
            }
            return true;
        }

        public String terminate() {
            int maxCount = -1;
            String mode = null;
            for (Map.Entry<String, Integer> count : counts.entrySet()) {
                if (count.getValue() > maxCount) {
                    mode = count.getKey();
                    maxCount = count.getValue();
                }
            }
            return mode;
        }
    }

}
