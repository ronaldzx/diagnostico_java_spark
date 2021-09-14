package minsait.ttaa.datio.engine;

import minsait.ttaa.datio.utils.Constants;
import minsait.ttaa.datio.utils.PropertiesUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {
    private SparkSession spark;

    public Transformer(@NotNull SparkSession spark) {
        this.spark = spark;
        Dataset<Row> df = readInput();

        df.printSchema();

        df = cleanData(df);
        df = ageFilter(df);
        df = columnSelection(df);
        df = nationalityTeamPositionFilter(df);
        df = potentialVsOverall(df);
        // for show 100 records after your transformations and show the Dataset schema
        df.show(100, false);
        df.printSchema();

        // Uncomment when you want write your final output
        write(df);
    }

    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                overall.column(),
                heightCm.column(),
                teamPosition.column(),
                ageRange.column(),
                longName.column(),
                age.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                potential.column()
        );
    }

    /**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput() {
        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(INPUT_PATH);
        return df;
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df;
    }

    /**
     * Question 2
     */
    public Dataset<Row> ageFilter(Dataset<Row> df) {

        Column rule = when(col(age.getName()).$less(Constants.NUMBER_23), Constants.LETTER_A)
                .when(col(age.getName()).$less(Constants.NUMBER_27), Constants.LETTER_B)
                .when(col(age.getName()).$less(Constants.NUMBER_32), Constants.LETTER_C)
                .otherwise(Constants.LETTER_D);

        df = df.withColumn(ageRange.getName(), rule);

        return df;
    }

    /**
     * Question 3
     */

    private Dataset<Row> nationalityTeamPositionFilter(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(nationality.column(), teamPosition.column())
                .orderBy(overall.column().desc());

        Column rowNumber = row_number().over(w);

        df = df.withColumn(rankByNationality.getName(), rowNumber);

        return df;
    }

    /**
     *  Question 4
     */

    private Dataset<Row> potentialVsOverall(Dataset<Row> df) {
        Column result = col(potential.getName()).divide(col(overall.getName()));
        System.out.println(result);
        df = df.withColumn(potentialVsOverall.getName(), result);
        return df;
    }

    /**
     * What should I do when conditions are met. Question 5
     */
    private Dataset<Row> conditions(Dataset<Row> df) {
        Column rule_A = when(col(rankByNationality.getName()).$less(Constants.NUMBER_3), "What should I put here");
        Column rule_B = when(col(ageRange.getName()).equalTo(Constants.LETTER_B).equalTo(Constants.LETTER_C)
                .and(col(potentialVsOverall.getName()).$greater(Constants.DECIMAL_1_15)), "What should I put here");
        Column rule_C = when(col(ageRange.getName()).equalTo(Constants.LETTER_A).and(col(potentialVsOverall.getName())
                .$greater(Constants.DECIMAL_1_25)), "What should I put here");
        Column rule_D = when(col(ageRange.getName()).equalTo(Constants.LETTER_D).and(col(potentialVsOverall.getName())
                .$less(Constants.DECIMAL_5)), "What should I put here");
//        df = df.withColumn(ageRange.getName(), rule_A);
        return df;
    }

}
