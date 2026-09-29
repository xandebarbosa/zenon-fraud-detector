package br.com.zenon;

import java.math.BigDecimal;
import java.util.List;

/**
 * Classe principal (ponto de entrada da aplicação) utilizada para testar e exercitar
 * os conceitos de modelagem com Records, Enums, Streams e tratamento de erros do sistema antifraude Zenon.
 */
public class Main {

    /**
     * Método principal (Main method no Java moderno 21+ com simplificação de declaração de métodos).
     */
    static void main() {
        // Criação manual de um objeto Transaction representando uma operação comum (PAYMENT, não fraudulenta)
        Transaction transaction1 = new Transaction(
                1,
                TransactionType.PAYMENT,
                new BigDecimal("9839.64"),
                new TransactionCustomer("C1231006815", new BigDecimal("170136.0"), new BigDecimal("160296.36")),
                new TransactionCustomer("M1979787155", new BigDecimal("0.0"), new BigDecimal("0.0")),
                false,
                false
        );

        // Criação manual de um objeto Transaction representando um caso de fraude real confirmada (isFraud = true)
        Transaction transaction2 = new Transaction(
                743,
                TransactionType.CASH_OUT,
                new BigDecimal("850002.52"),
                new TransactionCustomer("C1280323807", new BigDecimal("850002.52"), new BigDecimal("0.0")),
                new TransactionCustomer("C873221189", new BigDecimal("6510099.11"), new BigDecimal("7360101.63")),
                true,
                false
        );

        // O compilador do Java invoca automaticamente o método toString() gerado pelo Record para cada instância
        System.out.println("Imprimindo transação 1: " + transaction1);
        System.out.println("Imprimindo transação 2: " + transaction2);

        IO.println("---------- Usando a classe TransactionIngestor---------------------------------------");

        // Instancia o serviço de ingestão de arquivos CSV
        TransactionIngestor transactionIngestor = new TransactionIngestor();

        // Processa o arquivo com dados reais do dataset PaySim (limitado a 1000 registros válidos)
        List<Transaction> transactions = transactionIngestor.readTransactionsNew(
                "/home/alexandre/Projetos-Pratica-UNIPDS/zenon-fraud-detector/data/PS_20174392719_1491204439457_log.csv"
        );
        IO.println(transactions.size()); // Imprime a quantidade de transações carregadas com sucesso

        // Exibe no console as 10 primeiras transações através de uma Stream
        transactions.stream()
                .limit(10)
                .forEach(System.out::println);

        IO.println("---------- Usando a classe TransactionIngestor2 com tratamento de erros-------------------");

        // Processa o arquivo contendo intencionalmente anomalias e erros (steps <= 0, valores negativos, tipos inválidos, etc.)
        // Demonstra como a aplicação resiste a falhas (resiliência) sem travar o processamento
        List<Transaction> transactionsBadData = transactionIngestor.readTransactionsNew(
                "/home/alexandre/Projetos-Pratica-UNIPDS/zenon-fraud-detector/data/paysim_with_bad_data.csv"
        );
        IO.println(transactionsBadData.size()); // Imprime a quantidade total de linhas válidas que foram aceitas
        transactionsBadData.forEach(IO::println); // Exibe cada registro válido aceito
    }
}
