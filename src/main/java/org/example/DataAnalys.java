package org.example;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

public class DataAnalys {
    public static void main(String[] args) {
        // Créer une session Spark
        SparkSession spark = SparkSession.builder()
                .appName("MongoSparkConnectorIntro")
                .master("local")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/projetSup.mediacollection")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // Créer une configuration de lecture
        ReadConfig readConfig = ReadConfig.create(jsc).withOption("uri", "mongodb://127.0.0.1/projetSup.mediacollection");

        // Charger les données de MongoDB
        JavaRDD<Document> rdd = MongoSpark.load(jsc, readConfig);

        // Effectuer des analyses (par exemple, compter le nombre de documents)
        long count = rdd.count();
        System.out.println("Nombre total de documents : " + count);

        // Fermer le contexte Spark
        jsc.close();
        spark.stop();
    }
}
