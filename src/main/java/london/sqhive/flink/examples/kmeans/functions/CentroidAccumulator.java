package london.sqhive.flink.examples.kmeans.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import london.sqhive.flink.examples.kmeans.model.Point;

/** Sums and counts point coordinates. */
@FunctionAnnotation.ForwardedFields("0")
public final class CentroidAccumulator
    implements ReduceFunction<Tuple3<Integer, Point, Long>> {

    @Override
    public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
        return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
    }
}