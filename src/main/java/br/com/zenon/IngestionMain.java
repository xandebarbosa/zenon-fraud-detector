package br.com.zenon;

import java.util.List;

/**
 * Ponto de entrada (Entry Point) e classe de benchmarking para a ingestão massiva de transações no banco de dados.
 *
 * <p><b>Propósito e Responsabilidade:</b></p>
 * Orquestrar o fluxo de ponta a ponta do pipeline de alta performance (ETL), integrando o leitor concorrente
 * {@link EfficientTransactionIngestor} com o repositório de persistência relacional {@link TransactionSQLRepository}.
 * Além da execução do processo, esta classe mede com precisão o tempo gasto na ingestão total das transações
 * para fins de estudo de desempenho e benchmarking comparativo.
 *
 * <p><b>Integração com o Projeto:</b></p>
 * Serve como aplicação executável para testar a ingestão dos dados volumosos do arquivo de log
 * ({@code "data/PS_20174392719_1491204439457_log.csv"}) diretamente no banco de dados MySQL
 * gerenciado via Docker na porta 3307.
 *
 * <p><b>Conceitos Educacionais Aplicados:</b></p>
 * <ul>
 *   <li><b>Método Main de Instância (Java 21+):</b> Utilização da funcionalidade moderna introduzida no Java 21
 *       (JEP 445 / JEP 463 / JEP 477), permitindo que métodos {@code void main()} sejam executados sem a necessidade
 *       do clássico e verboso cabeçalho {@code public static void main(String[] args)};</li>
 *   <li><b>Inferência de Tipos de Variáveis Locais ({@code var}):</b> Recurso introduzido no Java 10 que permite ao
 *       compilador inferir automaticamente o tipo estático da variável pelo lado direito da atribuição, tornando o código
 *       mais limpo e focado no domínio;</li>
 *   <li><b>Referências a Métodos (Method References):</b> A sintaxe {@code repository::saveAll} passa o método
 *       como parâmetro funcional compatível com a interface {@link java.util.function.Consumer}, eliminando expressões lambda desnecessárias;</li>
 *   <li><b>Medição de Tempo de Alta Precisão:</b> Uso de {@link System#nanoTime()} em vez de {@link System#currentTimeMillis()},
 *       garantindo medições monotônicas não afetadas por alterações de relógio do sistema operacional ou sincronização NTP.</li>
 * </ul>
 */
public class IngestionMain {

    /**
     * Ponto de entrada da aplicação.
     *
     * <p><b>Lógica de Execução:</b></p>
     * <ol>
     *   <li>Instancia o repositório SQL ({@link TransactionSQLRepository});</li>
     *   <li>Instancia o ingestor concorrente em lotes ({@link EfficientTransactionIngestor});</li>
     *   <li>Marca o instante de início em nanossegundos via {@link System#nanoTime()};</li>
     *   <li>Dispara a leitura em lotes do arquivo CSV passando a referência {@code repository::saveAll} para consumo direto;</li>
     *   <li>Marca o instante de encerramento e calcula o tempo total transcorrido em milissegundos;</li>
     *   <li>Exibe as métricas de tempo consolidadas no console via {@link IO#println(Object)}.</li>
     * </ol>
     */
    void main() {

        // repository: Instância do repositório JDBC responsável pelas operações de persistência e batches no MySQL
        var repository = new TransactionSQLRepository();

        // transactionIngestor: Instância do ingestor concorrente que lê em streaming e paraleliza o parsing das linhas em lotes
        var transactionIngestor = new EfficientTransactionIngestor();
        //var transactionIngestor = new TransactionIngestor();

        // As linhas comentadas abaixo ilustram a abordagem anterior (monolítica / em memória):
        // Nela, todo o arquivo era carregado em uma List<Transaction> na memória heap antes de iniciar a gravação,
        // o que gerava alta pressão no Garbage Collector e risco iminente de OutOfMemoryError em grandes volumes de dados.
        //List<Transaction> transactions = transactionIngestor.readTransactionsNew("data/PS_20174392719_1491204439457_log.csv");
        //IO.println(transactions.size());

        // startTimeSQL: Variável do tipo primitivo 'long' que armazena a contagem de nanossegundos antes da ingestão.
        // System.nanoTime() é a API padrão do Java para medição de intervalos transcorridos (elapsed time).
        long startTimeSQL = System.nanoTime();

        // Executa a ingestão concorrente por lotes:
        // 1º argumento: caminho relativo do arquivo CSV contendo os registros de transações
        // 2º argumento: Method Reference (repository::saveAll), que implementa Consumer<List<Transaction>>,
        // conectando a saída de cada lote processado pelo ingestor diretamente à gravação em lote do repositório SQL.
        transactionIngestor.readAsBatch("data/PS_20174392719_1491204439457_log.csv", repository::saveAll);

        // Abordagem síncrona anterior onde os dados salvos em memória eram enviados todos de uma só vez:
        //IO.println("Iniciando adição das transaões no BD...");
        //repository.saveAll(transactions);

        // endTimeSQL: Variável do tipo 'long' registrando a marca temporal após a finalização de todas as threads e commits
        long endTimeSQL = System.nanoTime();

        // Exibe o tempo total formatado em milissegundos:
        // A subtração (endTimeSQL - startTimeSQL) resulta no tempo em nanossegundos;
        // Dividindo por 1_000_000.0 (ponto flutuante duplo), obtemos o valor exato em milissegundos (ms).
        IO.println("Tempo de ingestão no BD (ms): " + (endTimeSQL - startTimeSQL) / 1_000_000.0);
    }
}
