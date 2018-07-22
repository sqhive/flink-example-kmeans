package london.sqhive.flink.examples.kmeans.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import london.sqhive.flink.examples.kmeans.model.Centroid;
import london.sqhive.flink.examples.kmeans.model.Point;

/** Computes new centroid from coordinate sum and count of points. */
@FunctionAnnotation.ForwardedFields("0->id")
public final class CentroidAverager
    implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

    @Override
    public Centroid map(Tuple3<Integer, Point, Long> value) {
        return new Centroid(value.f0, value.f1.div(value.f2));
    }
}