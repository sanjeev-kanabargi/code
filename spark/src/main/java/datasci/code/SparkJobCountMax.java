package datasci.code;

import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import org.apache.spark.sql.DataFrame;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

/**
 *
 * @author Sanjeev_Kanabargi Program to count the max number of score of each
 *         user on each date
 */

public class SparkJobCountMax {

	public static SparkSession ss;

	public static void main(String[] args) {

		ss = SparkSession.builder().appName("Count_Max").master("local").getOrCreate();

		ss.sparkContext().setLogLevel("Error");

		ss.udf().register("MERGESTRING", MERGESTRING, DataTypes.StringType);

		createTableFromCSV(ss, "src/main/resources/Purchase.csv", "purchase");
		System.out.println("Purchase Dataset : ");
		Dataset<Row> purchaseDS = ss
				.sql("select MERGESTRING(table.emailID,table.date) as email_date, score from purchase table");
		purchaseDS.show();

		createTableFromCSV(ss, "src/main/resources/Browser.csv", "browse");
		System.out.println("Browser dataset : ");
		Dataset<Row> browseDS = ss
				.sql("select MERGESTRING(table.emailID,table.date) as email_date, score from browse table");
		;
		browseDS.show();

		findMaxScore(purchaseDS, browseDS);
	}

	/*
	 * logic to find max score.
	 */

	public static void findMaxScore(Dataset<Row> purchaseDS, Dataset<Row> browseDS) {

		// Creating superset of purchseDS and broweserDS
		Dataset<Row> unionData = purchaseDS.union(browseDS);

		// Converting into RDD for map functions
		JavaRDD<Row> jRDD = unionData.toJavaRDD();

		// combiling email and date ex: ram,25 = ram_25
		JavaPairRDD<String, Integer> jPRDD = jRDD.mapToPair(
				val -> new Tuple2<String, Integer>(val.get(0) + "", Integer.parseInt(val.get(1).toString())));

		// Reducing by key, i.e summing all the score happened on same date with same
		// use.
		jPRDD = jPRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer score_x, Integer score_y) throws Exception {
				return score_x + score_y;
			}
		});

		// Creating JavaRDD from paired RDD
		jRDD = jPRDD.map(val -> new RowFactory().create(val._1, val._2));

		StructType st = new StructType().add("email_date", DataTypes.StringType, false).add("score",
				DataTypes.IntegerType, false);
		Dataset<Row> dsCombined = ss.createDataFrame(jRDD, st);

		Iterator<Row> unionItr = ss.sql("SELECT emailID FROM purchase " + " UNION" + " SELECT emailID FROM browse")
				.distinct().toLocalIterator();

		// Iterating on unionDS, get all email ID and check the max score of that
		// repective email ID :
		while (unionItr.hasNext()) {

			Row unionDataRow = unionItr.next();
			String unionEmail = unionDataRow.getString(0);
			Iterator<Row> itrDsCombined = dsCombined.toLocalIterator();
			int max = 0;
			String date = "date ";
			String emailID = "emailID";
			String score = "score";
			while (itrDsCombined.hasNext()) {

				Row rowDSCombined = itrDsCombined.next();
				int intScore = Integer.parseInt(rowDSCombined.get(1).toString());
				String email_date = rowDSCombined.get(0).toString();
				if (email_date.contains(unionEmail) && intScore > max) {

					date = email_date.split("_")[1];
					emailID = email_date.split("_")[0];
					score = intScore + "";
					max = intScore;
				}
			}

			if (score.equals("score")) {
				System.out.println("No recored found for email ID :" + unionEmail);
			} else {
				System.out.println(emailID + "," + date + "," + score);
			}
		}

	}

	/*
	 * Metod to fetch the data from CSV file, convert it into Dataset and return
	 */
	public static Dataset<Row> createTableFromCSV(SparkSession ss, String filePath, String tableName) {

		String[] colArr = new String[] { "emailID", "date", "score" };
		Dataset<Row> temp_DS = ss.read().format("csv").option("delimiter", ",").load(filePath).toDF(colArr);
		DataFrameWriter dfw = temp_DS.write();
		dfw.format("orc").option("orc.create.index", "true").option("orc.compress", "SNAPPY").mode(SaveMode.Overwrite)
				.saveAsTable(tableName);

		return temp_DS;
	}

	static UDF2<String, String, String> MERGESTRING = (emailID, date) -> {
		return emailID + "_" + date;
	};

}