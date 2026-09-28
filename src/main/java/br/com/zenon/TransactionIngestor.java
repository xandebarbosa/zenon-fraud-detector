package br.com.zenon;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class TransactionIngestor {

    /* Maneira nova de escrever codigo, mais enxuto, estamos usadno stream  */
    public List<Transaction> readTransactionsNew(String fileName) {
        Path path = Paths.get(fileName);

        try {
            List<String> lines = Files.readAllLines(path); //Retorna uma lista de linhas
            return lines.stream()
                    .skip(1)
                    .limit(1000)
                    .map(this::parseTransaction)
                    .toList();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /* Maneira antiga de implementar o metodo abaixo, usando FileInputStream, Scanner  */
    public List<Transaction> readTransactionsOld(String filename) {

        List<Transaction> transactions = new ArrayList<>();  // Criando a Lista de transações

        try (FileInputStream fis = new FileInputStream(filename)) {
            Scanner scanner = new Scanner(fis);

            int lineCount = 0;

            while (scanner.hasNextLine()) {

                String line = scanner.nextLine();

                lineCount++;

                if (lineCount == 1) {
                    continue;
                }

                if (lineCount > 1001) {
                    break;
                }

                var transaction = parseTransaction(line);
                transactions.add(transaction);


            }

        } catch (Exception e) {
            throw new RuntimeException("Error reading the file: " + filename, e);
        }

        return transactions;
    }

    /*Metodo que separa cada posiçao do arquivo que esta sendo lido */
    private Transaction parseTransaction(String line) {
        String[] chunks = line.split(",");
        int step = Integer.parseInt(chunks[0]);
        TransactionType type = TransactionType.valueOf(chunks[1]);
        BigDecimal amount = new BigDecimal(chunks[2]);
        var origin = new TransactionCustomer(chunks[3], new BigDecimal(chunks[4]), new BigDecimal(chunks[5]));
        var recipient = new TransactionCustomer(chunks[6], new BigDecimal(chunks[7]), new BigDecimal(chunks[8]));

        int fraudValue = Integer.parseInt(chunks[9]);
        int flaggedFraudValue = Integer.parseInt(chunks[10]);

        boolean isFraud = fraudValue == 1;
        boolean isFlaggedFraud = flaggedFraudValue == 1;

        var transaction = new Transaction(step, type, amount, origin, recipient, isFraud, isFlaggedFraud);
        return transaction;
    }
}
