package org.example.realvalue_migration.service;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

@Service
public class MigrationService {

    private final String CLICKHOUSE_URL = "";
    private final String CLICKHOUSE_USER = "default";
    private final String CLICKHOUSE_PASSWORD = "default";

    private final String POSTGRES_URL = "";
    private final String POSTGRES_USER = "postgres";
    private final String POSTGRES_PASSWORD = "admin";

    public void migrateData(int batchSize) throws Exception {

        String tempDir = System.getProperty("java.io.tmpdir");
        String csvFilePath = tempDir + "data.csv";


        Files.createDirectories(Paths.get(tempDir));

        try (
                Connection clickhouseConn = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
                BufferedWriter csvWriter = new BufferedWriter(new FileWriter(csvFilePath))
        ) {
            System.out.println("Connections to ClickHouse established successfully.");


            String queryTemplate = "SELECT * FROM listings LIMIT %d OFFSET %d";
            int offset = 0;

            while (true) {
                String query = String.format(queryTemplate, batchSize, offset);
                ResultSet rs = clickhouseConn.createStatement().executeQuery(query);

                if (!rs.next()) break;

                do {
                    String uid = rs.getString("uid");
                    long listingId = rs.getLong("listing_id");
                    int platformId = rs.getInt("platform_id");
                    String listingUrl = rs.getString("listing_url");
                    double price = rs.getDouble("price");
                    double pricePerSqm = rs.getDouble("price_per_sqm");
                    float mortgageRate = rs.getFloat("mortgage_rate");
                    String address = rs.getString("address");
                    long addressId = rs.getLong("address_id");
                    float area = rs.getFloat("area");
                    Integer rooms = rs.getObject("rooms", Integer.class);
                    Integer floor = rs.getObject("floor", Integer.class);
                    String description = rs.getString("description");
                    String publishedDate = rs.getString("published_date");
                    String updatedDate = rs.getString("updated_date");
                    Long sellerId = rs.getObject("seller_id", Long.class);
                    String sellerNameHash = rs.getString("seller_name_hash");
                    String companyName = rs.getString("company_name");
                    Long companyId = rs.getObject("company_id", Long.class);
                    String propertyType = rs.getString("property_type");
                    String category = rs.getString("category");
                    Integer houseFloors = rs.getObject("house_floors", Integer.class);
                    String dealType = rs.getString("deal_type");
                    String discountStatus = rs.getString("discount_status");
                    float discountValue = rs.getFloat("discount_value");
                    int placementPaid = rs.getInt("placement_paid");
                    int bigCard = rs.getInt("big_card");
                    boolean pinColor = rs.getBoolean("pin_color");
                    double latitude = rs.getDouble("latitude");
                    double longitude = rs.getDouble("longitude");


                    String subwayNames = convertArrayToPostgresFormat(rs.getArray("subway_names"));
                    String subwayDistances = convertArrayToPostgresFormat(rs.getArray("subway_distances"));
                    String photoUrls = convertArrayToPostgresFormat(rs.getArray("photo_urls"));

                    double monthlyPayment = rs.getDouble("monthly_payment");
                    double advancePayment = rs.getDouble("advance_payment");
                    boolean auctionStatus = rs.getBoolean("auction_status");
                    String createdAt = rs.getString("created_at");


                    csvWriter.write(String.join("~",
                            quoteIfNotNull(uid), quoteIfNotNull(listingId), quoteIfNotNull(platformId),
                            quoteIfNotNull(listingUrl), quoteIfNotNull(price), quoteIfNotNull(pricePerSqm),
                            quoteIfNotNull(mortgageRate), quoteIfNotNull(address), quoteIfNotNull(addressId),
                            quoteIfNotNull(area), quoteIfNotNull(rooms), quoteIfNotNull(floor),
                            quoteIfNotNull(description), quoteIfNotNull(publishedDate), quoteIfNotNull(updatedDate),
                            quoteIfNotNull(sellerId), quoteIfNotNull(sellerNameHash), quoteIfNotNull(companyName),
                            quoteIfNotNull(companyId), quoteIfNotNull(propertyType), quoteIfNotNull(category),
                            quoteIfNotNull(houseFloors), quoteIfNotNull(dealType), quoteIfNotNull(discountStatus),
                            quoteIfNotNull(discountValue), quoteIfNotNull(placementPaid), quoteIfNotNull(bigCard),
                            quoteIfNotNull(pinColor), quoteIfNotNull(latitude), quoteIfNotNull(longitude),
                            quoteIfNotNull(subwayNames), quoteIfNotNull(subwayDistances), quoteIfNotNull(photoUrls),
                            quoteIfNotNull(monthlyPayment), quoteIfNotNull(advancePayment), quoteIfNotNull(auctionStatus),
                            quoteIfNotNull(createdAt)
                    ) + "\n");



                } while (rs.next());

                offset += batchSize;
            }

            csvWriter.close();
            System.out.println("CSV файл создан: " + csvFilePath);


            try (Connection postgresConn = DriverManager.getConnection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD);
                 BufferedReader csvReader = new BufferedReader(new FileReader(csvFilePath))) {

                String copyQuery = "COPY estate.listings FROM STDIN WITH (FORMAT csv, NULL '', DELIMITER '~', QUOTE '\"');";
                CopyManager copyManager = new CopyManager((BaseConnection) postgresConn);
                copyManager.copyIn(copyQuery, csvReader);
                System.out.println("Data loaded into PostgreSQL.");
            }

        }
    }

    private String quoteIfNotNull(Object value) {
        if (value == null) return "";
        String stringValue = value.toString().replace("\"", "\"\"");
        if (value instanceof String || value instanceof Character) {
            return "\"" + stringValue + "\"";
        }
        return stringValue;
    }


    private String convertArrayToPostgresFormat(java.sql.Array array) throws Exception {
        if (array == null) return "{}";
        Object elements = array.getArray();

        if (elements instanceof Object[]) {
            Object[] objArray = (Object[]) elements;
            if (objArray.length == 0) return "{}";
            StringBuilder sb = new StringBuilder("{");
            for (Object element : objArray) {
                sb.append("\"").append(element).append("\",");
            }
            sb.deleteCharAt(sb.length() - 1).append("}");
            return sb.toString();
        } else if (elements instanceof float[]) {
            float[] floatArray = (float[]) elements;
            if (floatArray.length == 0) return "{}";
            StringBuilder sb = new StringBuilder("{");
            for (float element : floatArray) {
                sb.append(element).append(",");
            }
            sb.deleteCharAt(sb.length() - 1).append("}");
            return sb.toString();
        } else if (elements instanceof double[]) {
            double[] doubleArray = (double[]) elements;
            if (doubleArray.length == 0) return "{}";
            StringBuilder sb = new StringBuilder("{");
            for (double element : doubleArray) {
                sb.append(element).append(",");
            }
            sb.deleteCharAt(sb.length() - 1).append("}");
            return sb.toString();
        } else {
            throw new IllegalArgumentException("Unsupported array type: " + elements.getClass().getName());
        }
    }
}
