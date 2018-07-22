/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package london.sqhive.flink.examples.kmeans;

import london.sqhive.flink.examples.kmeans.functions.CentroidAccumulator;
import london.sqhive.flink.examples.kmeans.functions.CentroidAverager;
import london.sqhive.flink.examples.kmeans.functions.CountAppender;
import london.sqhive.flink.examples.kmeans.functions.SelectNearestCenter;
import london.sqhive.flink.examples.kmeans.model.Centroid;
import london.sqhive.flink.examples.kmeans.model.Point;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import london.sqhive.flink.examples.kmeans.data.KMeansData;

public class Application {

	public static void main(String[] args) throws Exception {
		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface

		// get input data:
		// read the points and centroids from the provided paths or fall back to default data
		DataSet<Point> points = getPointDataSet(params, env);
		DataSet<Centroid> centroids = getCentroidDataSet(params, env);

		// set number of bulk iterations for KMeans algorithm
		IterativeDataSet<Centroid> loop = centroids.iterate(params.getInt("iterations", 10));

		DataSet<Centroid> newCentroids = points
				// compute closest centroid for each point
				.map(new SelectNearestCenter())
				.withBroadcastSet(loop, "centroids")
				// count and sum point coordinates for each centroid
				.map(new CountAppender())
				.groupBy(0)
				.reduce(new CentroidAccumulator())
				// compute new centroids from point counts and coordinate sums
				.map(new CentroidAverager());

		// feed new centroids back into next iteration
		DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

		DataSet<Tuple2<Integer, Point>> clusteredPoints = points
				// assign points to final clusters
				.map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

		// emit result
		if (params.has("output")) {
			clusteredPoints.writeAsCsv(params.get("output"), "\n", " ");

			// since file sinks are lazy, we trigger the execution explicitly
			env.execute("KMeans Example");
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			clusteredPoints.print();
		}
	}

	private static DataSet<Centroid> getCentroidDataSet(ParameterTool params, ExecutionEnvironment env) {
		DataSet<Centroid> centroids;
		if (params.has("centroids")) {
			centroids = env.readCsvFile(params.get("centroids"))
					.fieldDelimiter(" ")
					.pojoType(Centroid.class, "id", "x", "y");
		} else {
			System.out.println("Executing K-Means example with default centroid data set.");
			System.out.println("Use --centroids to specify file input.");
			centroids = KMeansData.getDefaultCentroidDataSet(env);
		}
		return centroids;
	}

	private static DataSet<Point> getPointDataSet(ParameterTool params, ExecutionEnvironment env) {
		DataSet<Point> points;
		if (params.has("points")) {
			// read points from CSV file
			points = env.readCsvFile(params.get("points"))
					.fieldDelimiter(" ")
					.pojoType(Point.class, "x", "y");
		} else {
			System.out.println("Executing K-Means example with default point data set.");
			System.out.println("Use --points to specify file input.");
			points = KMeansData.getDefaultPointDataSet(env);
		}
		return points;
	}

}
