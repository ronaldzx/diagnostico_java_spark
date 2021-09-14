package minsait.ttaa.datio;

import minsait.ttaa.datio.engine.Transformer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static minsait.ttaa.datio.common.Common.SPARK_MODE;

public class TrasnformerTest {
    private  Transformer transformer;
    @Before
    public void setUpClass(){

    }

    /**
     * Falta optimizar el test
     */
    @Test
    public void ageFilterTest(){
        SparkSession spark = SparkSession
                .builder()
                .master(SPARK_MODE)
                .getOrCreate();
        Transformer engine = new Transformer(spark);
//        SparkSession spark = SparkSession.builder().appName("Build a DataFrame from Scratch").master("local[*]")
//                .getOrCreate();

        List<String> stringAsList = new ArrayList<>();
        stringAsList.add("34");

        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());

        JavaRDD<Row> rowRDD = sparkContext.parallelize(stringAsList).map((String row) -> RowFactory.create(row));

        // Creates schema
        StructType schema = DataTypes.createStructType(
                new StructField[] { DataTypes.createStructField("age", DataTypes.StringType, false) });

        Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema).toDF();
        engine.ageFilter(df);
        df.show();
    }
}
