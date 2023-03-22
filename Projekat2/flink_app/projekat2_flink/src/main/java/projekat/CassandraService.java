package projekat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * CassandraService is a class that sinks DataStream into CassandraDB.
 */
public final class   CassandraService {

    private static final Logger LOGGER = Logger.getLogger(DataStreamJob.class);

    /**
     * Creating environment for Cassandra and sink some Data of car stream into CassandraDB
     *
     * @param sinkFilteredStream  DataStream of type Bus.
     */
    public final void sinkToCassandraDB(final DataStream<Tuple5<String, Double, Double,
            Double, Double>> sinkFilteredStream) throws Exception {

        LOGGER.info("Creating car data to sink into cassandraDB.");

        sinkFilteredStream.print();
        LOGGER.info("Open Cassandra connection and Sinking bus data into cassandraDB.");
        CassandraSink
                .addSink(sinkFilteredStream)
                .setHost("cassandra")
                .setQuery("INSERT INTO flink_keyspace.statistika(busLine, min_speed, max_speed, mean_speed, count) " +
                        "values (?, ?, ?, ?, ?);")
                .build();
    }
    public final void sinkToCassandraDB2(final DataStream<Tuple1<String>> sinkFilteredStream) throws Exception {

        LOGGER.info("Creating car data to sink into cassandraDB.");

        sinkFilteredStream.print();
        LOGGER.info("Open Cassandra connection and Sinking bus data into cassandraDB.");
        CassandraSink
                .addSink(sinkFilteredStream)
                .setHost("cassandra")
                .setQuery("INSERT INTO flink_keyspace.top_locations(latitude_longitude_count) values (?);")
                .build();

    }
}