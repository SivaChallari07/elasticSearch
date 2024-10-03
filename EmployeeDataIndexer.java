package com.agile.employee_data_indexer;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.apache.http.HttpHost;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException; // Make sure to import this

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EmployeeDataIndexer {
    private static RestHighLevelClient client;

    // Initialize Elasticsearch client
    public static void initClient() {
    	RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"))
    		    .setRequestConfigCallback(requestConfigBuilder -> 
    		        requestConfigBuilder
    		            .setConnectTimeout(5000) // 5 seconds
    		            .setSocketTimeout(60000)); // 60 seconds

        client = new RestHighLevelClient(builder);
    }

    // Close the client connection
    public static void closeClient() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    public static void createCollection(String collectionName) throws IOException {
        IndexRequest request = new IndexRequest(collectionName);
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("created", true);
        request.source(jsonMap, XContentType.JSON);
        client.index(request, RequestOptions.DEFAULT);
        System.out.println("Collection created: " + collectionName);
    }

    public static void indexDataFromCSV(String collectionName, String excludeColumn, String filePath) throws IOException {
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                Map<String, Object> employeeData = new HashMap<>();
                employeeData.put("EmployeeID", nextLine[0]);
                employeeData.put("Name", nextLine[1]);
                employeeData.put("Department", nextLine[2]);
                employeeData.put("Gender", nextLine[3]);

                // Check and remove the excluded column
                if (excludeColumn != null && employeeData.containsKey(excludeColumn)) {
                    employeeData.remove(excludeColumn);
                }

                // Index each employee data
                IndexRequest request = new IndexRequest(collectionName).source(employeeData, XContentType.JSON);
                client.index(request, RequestOptions.DEFAULT);
            }
            System.out.println("Employee data indexed into: " + collectionName);
        } catch (CsvValidationException | IOException e) {
            e.printStackTrace(); // Handle CSV validation and IO exceptions
        }
    }

    public static void searchByColumn(String collectionName, String columnName, String columnValue) throws IOException {
        SearchRequest searchRequest = new SearchRequest(collectionName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery(columnName, columnValue));
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("Search results: " + searchResponse);
    }

    public static long getEmpCount(String collectionName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(collectionName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        long count = searchResponse.getHits().getTotalHits().value;
        System.out.println("Total employees: " + count);
        return count;
    }

    public static void delEmpById(String collectionName, String employeeId) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(collectionName, employeeId);
        client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println("Deleted employee with ID: " + employeeId);
    }

    public static void getDepFacet(String collectionName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(collectionName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("Department facet: " + searchResponse);
    }

    public static void main(String[] args) throws IOException {
        initClient();

        String v_nameCollection = "Hash_John";
        String v_phoneCollection = "Hash_1234";
        String csvFilePath = "G:\\assign\\Employee Sample Data 1.csv";

        createCollection(v_nameCollection);
        createCollection(v_phoneCollection);
        
        getEmpCount(v_nameCollection);
        
        indexDataFromCSV(v_nameCollection, "Department", csvFilePath);
        indexDataFromCSV(v_phoneCollection, "Gender", csvFilePath);
        
        delEmpById(v_nameCollection, "E02003");
        
        getEmpCount(v_nameCollection);
        
        searchByColumn(v_nameCollection, "Department", "IT");
        searchByColumn(v_nameCollection, "Gender", "Male");
        searchByColumn(v_phoneCollection, "Department", "IT");
        
        getDepFacet(v_nameCollection);
        getDepFacet(v_phoneCollection);

        closeClient();
    }
}
