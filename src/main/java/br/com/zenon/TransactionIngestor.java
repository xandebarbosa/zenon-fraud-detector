package br.com.zenon;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Classe responsável pela ingestão e processamento de arquivos de dados (CSV).
 * 
 * Demonstra duas abordagens de leitura de arquivos em Java:
 * 1. readTransactionsNew: moderna, funcional e declarativa utilizando Java NIO e Stream API.
 * 2. readTransactionsOld: clássica e imperativa utilizando FileInputStream e Scanner.
 */
public class TransactionIngestor {

    /**
     * Abordagem moderna e funcional utilizando Java NIO (Files/Paths) e Stream API.
     * 
     * Lê todo o arquivo e executa um pipeline de processamento:
     * pula cabeçalho -> limita registros -> faz parse para Optional -> filtra erros -> coleta lista.
     *
     * @param fileName Caminho absoluto ou relativo do arquivo CSV a ser ingerido.
     * @return Lista contendo apenas as transações válidas (linhas inválidas são descartadas).
     */
    public List<Transaction> readTransactionsNew(String fileName) {
        // Paths.get(fileName): cria uma referência abstrata para o arquivo no sistema operacional
        Path path = Paths.get(fileName);

        try {
            // Files.readAllLines(path): lê todas as linhas do arquivo em memória e retorna uma List<String>
            List<String> lines = Files.readAllLines(path);

            return lines.stream()
                    // skip(1): descarta a primeira linha (cabeçalho com nomes das colunas: step, type, amount, ...)
                    .skip(1)
                    // limit(1000): restringe o processamento às primeiras 1000 linhas para ganho de performance em testes
                    .limit(1000)
                    // map(this::parseTransaction): aplica o método parseTransaction a cada linha, retornando Optional<Transaction>
                    .map(this::parseTransaction)
                    // filter(Optional::isPresent): descarta os Optionals vazios (linhas que geraram erro no parse)
                    .filter(Optional::isPresent)
                    // map(Optional::get): extrai o objeto Transaction de dentro de cada Optional preenchido
                    .map(Optional::get)
                    // toList(): coleta todos os elementos em uma lista imutável (funcionalidade a partir do Java 16)
                    .toList();
        } catch (Exception e) {
            // Caso ocorra erro de I/O na abertura ou leitura do arquivo físico, encapsula em RuntimeException
            throw new RuntimeException("Erro ao ler o arquivo: " + fileName, e);
        }
    }

    /**
     * Abordagem tradicional / imperativa utilizando FileInputStream e Scanner.
     * 
     * Útil para fins didáticos para comparar o estilo pré-Java 8 com o estilo funcional moderno.
     *
     * @param filename Caminho do arquivo CSV a ser lido.
     * @return Lista de Optional<Transaction>, mantendo o resultado de cada linha (sucesso ou falha).
     */
    public List<Optional<Transaction>> readTransactionsOld(String filename) {

        // Cria a lista mutável que acumulará os resultados processados
        List<Optional<Transaction>> transactions = new ArrayList<>();

        // try-with-resources: garante o fechamento automático do recurso (FileInputStream) mesmo se houver exceções
        try (FileInputStream fis = new FileInputStream(filename)) {
            // Scanner lê o fluxo de bytes do arquivo de forma sequencial
            Scanner scanner = new Scanner(fis);

            int lineCount = 0; // Contador manual de linhas lidas

            // Enquanto houver uma próxima linha disponível para leitura no arquivo
            while (scanner.hasNextLine()) {

                String line = scanner.nextLine(); // Obtém a linha de texto atual

                lineCount++;

                // Pula a linha 1 correspondente ao cabeçalho das colunas do CSV
                if (lineCount == 1) {
                    continue;
                }

                // Interrompe a execução após ler 1000 linhas de dados (linha 1002 em diante)
                if (lineCount > 1001) {
                    break;
                }

                // Efetua a conversão da linha e adiciona o Optional retornado à lista
                var transaction = parseTransaction(line);
                transactions.add(transaction);
            }

        } catch (Exception e) {
            // Trata falhas de acesso ao sistema de arquivos
            throw new RuntimeException("Error reading the file: " + filename, e);
        }

        return transactions;
    }

    /**
     * Método auxiliar privado responsável por converter uma linha em texto CSV em um objeto Transaction.
     * 
     * Se os dados forem válidos, retorna um Optional com a Transaction.
     * Se qualquer erro ou violação de regra de negócio ocorrer, captura o erro e retorna Optional.empty().
     *
     * @param line Linha única contendo os valores separados por vírgula.
     * @return Optional<Transaction> contendo a transação se bem-sucedido, ou Optional.empty() em caso de erro.
     */
    private Optional<Transaction> parseTransaction(String line) {
        try {
            // split(","): particiona a linha de texto delimitada por vírgulas em um array de Strings
            String[] chunks = line.split(",");

            // chunks[0]: número da hora/etapa (step)
            int step = Integer.parseInt(chunks[0]);

            // chunks[1]: tipo da transação (ex: PAYMENT, TRANSFER) validado pelo método seguro do enum
            TransactionType type = TransactionType.fromString(chunks[1]);

            // chunks[2]: validação prévia de campo vazio/nulo antes de converter para BigDecimal
            if (chunks[2] == null || chunks[2].trim().isEmpty()) {
                throw new IllegalArgumentException("O valor de amount não pode ser nulo nem vazio");
            }
            BigDecimal amount = new BigDecimal(chunks[2]);

            // chunks[3], chunks[4], chunks[5]: identificador e saldos de origem (TransactionCustomer)
            var origin = new TransactionCustomer(chunks[3], new BigDecimal(chunks[4]), new BigDecimal(chunks[5]));

            // chunks[6], chunks[7], chunks[8]: identificador e saldos de destino (TransactionCustomer)
            var recipient = new TransactionCustomer(chunks[6], new BigDecimal(chunks[7]), new BigDecimal(chunks[8]));

            // chunks[9] e chunks[10]: colunas booleanas representadas como 0 (falso) ou 1 (verdadeiro)
            int fraudValue = Integer.parseInt(chunks[9]);
            int flaggedFraudValue = Integer.parseInt(chunks[10]);

            // Conversão dos inteiros para tipos booleanos primitivos
            boolean isFraud = fraudValue == 1;
            boolean isFlaggedFraud = flaggedFraudValue == 1;

            // Optional.of: encapsula a instância Transaction criada em um container Optional não-nulo
            var transaction = Optional.of(new Transaction(step, type, amount, origin, recipient, isFraud, isFlaggedFraud));
            return transaction;
        } catch (Exception e) {
            // Captura qualquer exceção (IllegalArgumentException, NumberFormatException, ArrayIndexOutOfBoundsException, etc.)
            // Exibe mensagem no System.err para depuração sem interromper a execução do fluxo da aplicação
            System.err.println("Error ao fazer parse: " + line + " | " + e);
            //e.printStackTrace(); // Imprime a pilha de execução (útil em testes e depuração)

            // Retorna um Optional vazio, permitindo que a Stream descarte esta linha inválida suavemente
            return Optional.empty();
        }
    }
}
