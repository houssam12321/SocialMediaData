package org.example;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.bson.Document;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class Main {
    public static void main(String[] args) {
        // Connexion à MongoDB
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("projetSup");
        MongoCollection<Document> collection = database.getCollection("mediacollection");

        // Lire le fichier CSV
        try (CSVReader reader = new CSVReader(new FileReader("data/doc.csv"))) {
            List<String[]> records = reader.readAll();
            String[] headers = records.get(0); // La première ligne contient les en-têtes

            List<Document> documents = new ArrayList<>();
            for (int i = 1; i < records.size(); i++) {
                String[] row = records.get(i);
                Document doc = new Document();
                for (int j = 0; j < headers.length; j++) {
                    doc.append(headers[j], row[j]);
                }
                documents.add(doc);
            }

            // Insérer les documents dans MongoDB
            collection.insertMany(documents);
            System.out.println("Données importées avec succès !");
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        } finally {
            mongoClient.close();
        }
    }
}
