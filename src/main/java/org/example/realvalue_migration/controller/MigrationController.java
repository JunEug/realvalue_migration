package org.example.realvalue_migration.controller;

import org.example.realvalue_migration.service.MigrationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MigrationController {

    @Autowired
    private MigrationService migrationService;

    @GetMapping("/migrate")
    public String migrateData(@RequestParam int batchSize) {
        try {
            migrationService.migrateData(batchSize);
            return "Data migration completed successfully.";
        } catch (Exception e) {
            return "Error during migration: " + e.getMessage();
        }
    }
}
