package de.frosner.hive.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Description(
        name = "Mode",
        value = "_FUNC_(expr) - Computes the statistical mode of a group.",
        extended = "_FUNC_(expr) - Computes the statistical mode of a group. The mode is defined as the value with the highest" +
                "frequency. If the mode is not unique, this function returns one of the values. Null values will be ignored."
)
public final class UDAFMode extends AbstractGenericUDAFResolver {

    private static final Log LOG = LogFactory.getLog(UDAFMode.class);
    private static final int MODE_ARGUMENT_POSITION = 0;

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] typeInfos) throws SemanticException {
        if (typeInfos.length != 1) {
            throw new UDFArgumentException("Please specify exactly one argument.");
        }
        TypeInfo typeInfo = typeInfos[MODE_ARGUMENT_POSITION];
        if (typeInfo.getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
            if (primitiveTypeInfo.getPrimitiveCategory().equals(PrimitiveObjectInspector.PrimitiveCategory.STRING)) {
                return new UDAFModeEvaluator();
            } else {
                throw new UDFArgumentTypeException(MODE_ARGUMENT_POSITION, "Only String primitives supported.");
            }
        } else {
            throw new UDFArgumentTypeException(MODE_ARGUMENT_POSITION, "Only String primitives supported.");
        }
    }

    public static class UDAFModeEvaluator extends GenericUDAFEvaluator {

        public static class Counts extends AbstractAggregationBuffer {

            private Map<String, Integer> counts = new HashMap<>();

            public void increase(String of, int by) {
                int currentCount = 0;
                if (counts.containsKey(of)) {
                    currentCount = counts.get(of);
                }
                counts.put(of, currentCount + by);
            }

            public void increaseAll(Counts counts) {
                for (Map.Entry<String, Integer> intermediateEntry : counts.getCounts()) {
                    increase(intermediateEntry.getKey(), intermediateEntry.getValue());
                }
            }

            public void increment(String of) {
                increase(of, 1);
            }

            public Set<Map.Entry<String, Integer>> getCounts() {
                return counts.entrySet();
            }

            public void reset() {
                counts = new HashMap<>();
            }

            public Map<String, Integer> getMap() {
                return counts;
            }

        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            ObjectInspector inputOi = parameters[0];
            return ObjectInspectorUtils.getStandardObjectInspector(inputOi,
                    ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new Counts();
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            ((Counts) aggregationBuffer).reset();
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] parameters) throws HiveException {
            String value = (String) parameters[0];
            if (value != null) {
                ((Counts) aggregationBuffer).increment(value);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            return ((Counts) aggregationBuffer).getMap();
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            Counts thisCounts = (Counts) aggregationBuffer;
            Counts intermediateCounts = (Counts) o;
            thisCounts.increaseAll(intermediateCounts);
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            int maxCount = -1;
            String mode = null;
            for (Map.Entry<String, Integer> count : ((Counts) aggregationBuffer).getCounts()) {
                if (count.getValue() > maxCount) {
                    mode = count.getKey();
                    maxCount = count.getValue();
                }
            }
            return mode;
        }

    }
}
