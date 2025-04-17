package org.example.realvalue_migration.service;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;

@Service
public class MigrationService {

    private final String CLICKHOUSE_URL = "jdbc:clickhouse://84.252.133.205:8123/dwh?compress=0";
    private final String CLICKHOUSE_USER = "default";
    private final String CLICKHOUSE_PASSWORD = "default";

    private final String POSTGRES_URL = "jdbc:postgresql://localhost:5432/postgres?currentSchema=estate";
    private final String POSTGRES_USER = "postgres";
    private final String POSTGRES_PASSWORD = "admin";

    private final String CLICKHOUSE_TABLE = "dwh.v_listings_actual_new_c";

    public void migrateData(int batchSize) throws Exception {
        String tempDir = System.getProperty("java.io.tmpdir");
        String csvFilePath = tempDir + "/data.csv";
        Files.createDirectories(Paths.get(tempDir));

        try (
                Connection clickhouseConn = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
                BufferedWriter csvWriter = new BufferedWriter(new FileWriter(csvFilePath))
        ) {
            System.out.println("Connection to ClickHouse established successfully.");
            verifyTableStructure(clickhouseConn);

            String queryTemplate = "SELECT * FROM " + CLICKHOUSE_TABLE + " LIMIT %d OFFSET %d";
            int offset = 0;
            int totalRecords = 0;

            while (true) {
                String query = String.format(queryTemplate, batchSize, offset);
                try (ResultSet rs = clickhouseConn.createStatement().executeQuery(query)) {
                    if (!rs.next()) break;

                    do {
                        // Пишем поля строго по схеме
                        csvWriter.write(String.join("~",
                                quoteIfNotNull(rs.getLong("dwh_uid")),
                                quoteIfNotNull(rs.getString("uid")),
                                quoteIfNotNull(rs.getLong("listing_id")),
                                quoteIfNotNull(rs.getString("listing_url")),
                                quoteIfNotNull(rs.getInt("platform_id")),
                                quoteIfNotNull(rs.getDouble("price")),
                                quoteIfNotNull(rs.getDouble("price_per_sqm")),
                                quoteIfNotNull(rs.getFloat("mortgage_rate")),
                                quoteIfNotNull(rs.getDouble("monthly_payment")),
                                quoteIfNotNull(rs.getDouble("advance_payment")),
                                quoteIfNotNull(rs.getString("address")),
                                quoteIfNotNull(rs.getLong("address_id")),
                                quoteIfNotNull(rs.getDouble("area")),
                                quoteIfNotNull(rs.getObject("rooms", Integer.class)),
                                quoteIfNotNull(rs.getObject("floor", Integer.class)),
                                quoteIfNotNull(rs.getString("description")),
                                quoteIfNotNull(rs.getString("published_date")),
                                quoteIfNotNull(rs.getString("updated_date")),
                                quoteIfNotNull(rs.getString("created_at")),
                                quoteIfNotNull(rs.getLong("seller_id")),
                                quoteIfNotNull(rs.getString("seller_name_hash")),
                                quoteIfNotNull(convertEnumValue(rs.getString("seller_type"))),
                                quoteIfNotNull(rs.getString("company_name")),
                                quoteIfNotNull(rs.getObject("company_id", Long.class)),
                                quoteIfNotNull(convertEnumValue(rs.getString("property_type"))),
                                quoteIfNotNull(convertEnumValue(rs.getString("category"))),
                                quoteIfNotNull(rs.getObject("house_floors", Integer.class)),
                                quoteIfNotNull(convertEnumValue(rs.getString("deal_type"))),
                                quoteIfNotNull(convertEnumValue(rs.getString("discount_status"))),
                                quoteIfNotNull(rs.getDouble("discount_value")),
                                quoteIfNotNull(rs.getInt("placement_paid")),
                                quoteIfNotNull(rs.getInt("big_card")),
                                quoteIfNotNull(rs.getInt("pin_color")),
                                quoteIfNotNull(rs.getDouble("longitude")),
                                quoteIfNotNull(rs.getDouble("latitude")),
                                quoteIfNotNull(convertArrayToPostgresFormat(rs.getArray("subway_distances"))),
                                quoteIfNotNull(convertArrayToPostgresFormat(rs.getArray("subway_names"))),
                                quoteIfNotNull(convertArrayToPostgresFormat(rs.getArray("photo_urls"))),
                                quoteIfNotNull(rs.getObject("auction_status", Double.class)),
                                quoteIfNotNull(convertEnumValue(rs.getString("flat_type"))),
                                quoteIfNotNull(rs.getObject("height", Double.class)),
                                quoteIfNotNull(rs.getObject("area_rooms", Double.class)),
                                quoteIfNotNull(rs.getObject("previous_price", Double.class)),
                                quoteIfNotNull(rs.getString("renovation_offer")),
                                quoteIfNotNull(convertEnumValue(rs.getString("balcony_type"))),
                                quoteIfNotNull(convertEnumValue(rs.getString("window_view"))),
                                quoteIfNotNull(rs.getObject("built_year_offer", Integer.class)),
                                quoteIfNotNull(convertEnumValue(rs.getString("building_state"))),
                                quoteIfNotNull(rs.getInt("valid")),
                                quoteIfNotNull(rs.getObject("predicted_price", Double.class)),
                                quoteIfNotNull(rs.getObject("flat_rating", Double.class)),
                                quoteIfNotNull(rs.getObject("house_rating", Double.class)),
                                quoteIfNotNull(rs.getObject("location_rating", Double.class)),
                                quoteIfNotNull(rs.getString("group_id"))
                        ) + "\n");

                        totalRecords++;
                    } while (rs.next());
                    offset += batchSize;
                    System.out.printf("Processed batch: offset=%d, total=%d%n", offset, totalRecords);
                }
            }

            csvWriter.close();
            System.out.printf("CSV file created with %d records: %s%n", totalRecords, csvFilePath);

            try (Connection postgresConn = DriverManager.getConnection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD);
                 BufferedReader csvReader = new BufferedReader(new FileReader(csvFilePath))) {

                String copyQuery = "COPY estate.property_listings FROM STDIN WITH (FORMAT csv, NULL '', DELIMITER '~', QUOTE '\"')";
                CopyManager copyManager = new CopyManager((BaseConnection) postgresConn);
                long copiedRows = copyManager.copyIn(copyQuery, csvReader);
                System.out.printf("Successfully loaded %d records into PostgreSQL.%n", copiedRows);
            }
        } catch (Exception e) {
            System.err.println("Migration failed: " + e.getMessage());
            throw e;
        }
    }

    private void verifyTableStructure(Connection clickhouseConn) throws Exception {
        try (ResultSet rs = clickhouseConn.createStatement().executeQuery("SELECT count() FROM " + CLICKHOUSE_TABLE + " LIMIT 1")) {
            if (!rs.next()) {
                throw new RuntimeException("Table " + CLICKHOUSE_TABLE + " doesn't exist or is empty");
            }
            System.out.println("Table " + CLICKHOUSE_TABLE + " exists and contains data");
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
        Object rawArray = array.getArray();

        if (rawArray instanceof Object[] objArray) {
            if (objArray.length == 0) return "{}";
            StringBuilder sb = new StringBuilder("{");
            for (Object el : objArray) {
                sb.append("\"").append(el).append("\",");
            }
            sb.setLength(sb.length() - 1);
            sb.append("}");
            return sb.toString();
        } else if (rawArray instanceof double[] doubleArray) {
            if (doubleArray.length == 0) return "{}";
            StringBuilder sb = new StringBuilder("{");
            for (double el : doubleArray) {
                sb.append(el).append(",");
            }
            sb.setLength(sb.length() - 1);
            sb.append("}");
            return sb.toString();
        } else if (rawArray instanceof float[] floatArray) {
            if (floatArray.length == 0) return "{}";
            StringBuilder sb = new StringBuilder("{");
            for (float el : floatArray) {
                sb.append(el).append(",");
            }
            sb.setLength(sb.length() - 1);
            sb.append("}");
            return sb.toString();
        } else {
            throw new IllegalArgumentException("Unsupported array type: " + rawArray.getClass().getName());
        }
    }


    private String convertEnumValue(String enumValue) {
        if (enumValue == null) return null;

        String cleaned = enumValue.replaceAll("=\\d+$", "").toUpperCase();

        if ("category".equalsIgnoreCase(enumValue)) {
            switch (cleaned) {
                case "living":
                    return "1";
                case "Unknown":
                    return "2";
                default:
                    return "2";
            }
        }

        switch (cleaned) {
            case "AGENT": case "AGENCY": case "DEVELOPER": case "OWNER": case "UNKNOWN": case "PRIVATE_AGENT":
            case "LAYOUT": case "TOWNHOUSE": case "HOUSE": case "FLAT": case "ROOM":
            case "LIVING":
            case "SALE": case "LEASE": case "RENT":
            case "ACTIVE": case "EXPIRED": case "NONE": case "DISCOUNT_RECEIVED":
            case "SECONDARY": case "NEW_FLAT": case "NEW_SECONDARY":
            case "BALCONY": case "LOGGIA": case "TWO_LOGGIA": case "BALCONY__LOGGIA": case "TWO_BALCONY":
            case "YARD": case "YARD_STREET": case "STREET":
            case "UNFINISHED": case "HAND_OVER":
                return cleaned;
            default:
                return "UNKNOWN";
        }
    }

}
