package br.com.zenon;

import java.math.BigDecimal;

public record Transaction(
        int step,
        TransactionType type,
        BigDecimal amount,
        TransactionCustomer transactionCustomerOrigin,
        TransactionCustomer transactionCustomerRecipient,
        boolean isFraud,
        boolean isFlaggedFraud
        ) {


}
