package london.sqhive.flink.examples.kmeans.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import london.sqhive.flink.examples.kmeans.model.Centroid;
import london.sqhive.flink.examples.kmeans.model.Point;

import java.util.Collection;

@FunctionAnnotation.ForwardedFields("*->1")
public final class SelectNearestCenter
    extends RichMapFunction<Point, Tuple2<Integer, Point>> {

    private Collection<Centroid> centroids;

    /** Reads the centroid values from a broadcast variable into a collection. */
    @Override
    public void open(Configuration parameters) throws Exception {
        this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
    }

    @Override
    public Tuple2<Integer, Point> map(Point p) throws Exception {

        double minDistance = Double.MAX_VALUE;
        int closestCentroidId = -1;

        // check all cluster centers
        for (Centroid centroid : centroids) {
            // compute distance
            double distance = p.euclideanDistance(centroid);

            // update nearest cluster if necessary
            if (distance < minDistance) {
                minDistance = distance;
                closestCentroidId = centroid.id;
            }
        }

        // emit a new record with the center id and the data point.
        return new Tuple2<>(closestCentroidId, p);
    }
}