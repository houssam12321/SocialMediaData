package org.example;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.ne;

public class CleanCollect {
    public static void main(String[] args) {
        // Connexion à MongoDB
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("projetSup");
        MongoCollection<Document> collection = database.getCollection("mediacollection");

        // Filtrer les documents où "Likes/Followers/Visits/Downloads" n'est pas vide
        Bson filter = ne("Likes/Followers/Visits/Downloads", "");
        List<Document> cleanedDocuments = collection.find(filter).into(new ArrayList<>());

        // Créer une nouvelle collection et insérer les documents nettoyés
        MongoCollection<Document> newCollection = database.getCollection("cleaned_mediacollection");
        newCollection.insertMany(cleanedDocuments);

        System.out.println("Les données ont été nettoyées et insérées dans la nouvelle collection 'cleaned_mediacollection'.");

        // Fermer la connexion
        mongoClient.close();
    }
}
