package org.example;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Accumulators.sum;

public class Analyse {
    public static void main(String[] args) {
        // Connexion à MongoDB
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("projetSup");
        MongoCollection<Document> collection = database.getCollection("mediacollection");

        // Agrégation des données
        List<Bson> pipeline = Arrays.asList(
                match(exists("Agency")),  // Filtrer les documents avec une agence
                group("$Agency",
                        sum("totalApps", 1),
                        avg("averageLikes", "$Likes/Followers/Visits/Downloads"))
        );

        // Exécuter l'agrégation
        List<Document> results = collection.aggregate(pipeline).into(new ArrayList<>());

        // Afficher les résultats
        for (Document result : results) {
            System.out.println(result.toJson());
        }

        // Fermer la connexion
        mongoClient.close();
    }
}

