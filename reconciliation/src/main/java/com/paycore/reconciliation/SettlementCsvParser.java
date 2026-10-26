package com.paycore.reconciliation;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Component;

/** Parses and validates the positional settlement CSV format independently of reconciliation policy. */
@Component
public class SettlementCsvParser {

    public Map<String, PspSettlementRow> parse(String csvContent, LocalDate settlementDate) {
        Map<String, PspSettlementRow> rowsByPspRef = new LinkedHashMap<>();
        if (csvContent == null || csvContent.isBlank()) {
            return rowsByPspRef;
        }
        try (CSVParser parser = CSVParser.parse(csvContent, CSVFormat.DEFAULT)) {
            boolean firstLine = true;
            for (CSVRecord record : parser) {
                if (record.size() == 0) continue;
                if (firstLine && record.get(0).trim().equalsIgnoreCase("psp_ref")) {
                    firstLine = false;
                    continue;
                }
                firstLine = false;
                if (record.size() != 4 && record.size() != 5) {
                    throw new IllegalArgumentException("Invalid CSV record " + record.getRecordNumber()
                            + "; expected 4 columns and optional currency");
                }
                String pspRef = record.get(0).trim();
                long amount = Long.parseLong(record.get(1).trim());
                String status = record.get(2).trim();
                String settledAt = record.get(3).trim();
                String currency = record.size() == 5 ? record.get(4).trim().toUpperCase() : "INR";
                if (pspRef.isEmpty() || status.isEmpty() || settledAt.isEmpty() || currency.isEmpty()) {
                    throw new IllegalArgumentException("Settlement record " + record.getRecordNumber()
                            + " has an empty required field");
                }
                LocalDate rowDate = settlementDate(settledAt);
                if (!rowDate.equals(settlementDate)) {
                    throw new IllegalArgumentException("Settlement row date " + rowDate
                            + " does not match requested date " + settlementDate);
                }
                if (rowsByPspRef.putIfAbsent(pspRef,
                        new PspSettlementRow(pspRef, amount, status, settledAt, currency)) != null) {
                    throw new IllegalArgumentException("Duplicate PSP reference in settlement file: " + pspRef);
                }
            }
        } catch (IOException | RuntimeException failure) {
            throw new IllegalArgumentException("Failed to parse settlement CSV: " + failure.getMessage(), failure);
        }
        return rowsByPspRef;
    }

    private LocalDate settlementDate(String value) {
        try {
            return LocalDate.parse(value);
        } catch (java.time.format.DateTimeParseException ignored) {
            return Instant.parse(value).atZone(ZoneOffset.UTC).toLocalDate();
        }
    }
}
