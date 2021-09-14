package minsait.ttaa.datio;

import minsait.ttaa.datio.engine.Transformer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import static minsait.ttaa.datio.common.Common.SPARK_MODE;

public class Runner {
    static SparkSession spark = SparkSession
            .builder()
            .master(SPARK_MODE)
            .getOrCreate();

    public static void main(String[] args) {
        Transformer engine = new Transformer(spark);
        Logger.getLogger("org.apache").setLevel(Level.WARN);
    }
}
