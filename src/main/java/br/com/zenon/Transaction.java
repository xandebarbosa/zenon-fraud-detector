package br.com.zenon;

import java.math.BigDecimal;

public record Transaction(
        int step,
        TransactionType type,
        BigDecimal amount,
        TransactionCustomer transactionCustomerOrigin,
        TransactionCustomer transactionCustomerRecipient,
        int isFraud,
        int isFlaggedFraud
        ) {


}
