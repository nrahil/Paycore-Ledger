package com.paycore.reconciliation;

/** One normalized row from a PSP settlement file. */
public record PspSettlementRow(String pspRef, long amount, String status, String settledAt, String currency) {}
