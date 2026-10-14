package br.com.zenon;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Ingestor eficiente de transações financeiras com suporte a streaming de arquivos e concorrência multithread.
 *
 * <p><b>Propósito e Responsabilidade:</b></p>
 * Atuar como um componente de alta performance da camada de ingestão de dados (<i>ETL - Extract, Transform, Load</i>),
 * especializado no processamento de arquivos CSV volumosos (como logs com centenas de milhares ou milhões de linhas).
 * Sua principal responsabilidade é ler os dados em fluxo contínuo (streaming), particionar o arquivo em lotes (<i>chunks</i>)
 * e delegar a conversão dos dados (parsing) e persistência a um pool de threads assíncronas, maximizando a taxa de transferência
 * (throughput) e minimizando o consumo de memória RAM da aplicação.
 *
 * <p><b>Como se integra ao projeto:</b></p>
 * Representa uma evolução arquitetural sobre a classe {@link TransactionIngestor}. Enquanto a abordagem anterior
 * lia todas as linhas para a memória de uma só vez (via {@code Files.readAllLines}), esta classe utiliza {@link Files#lines(Path)}
 * sob demanda e despacha os lotes processados diretamente para consumidores funcionais, como o método
 * {@link TransactionSQLRepository#saveAll(List)}, permitindo alimentar a base de dados em paralelo com a leitura do disco.
 *
 * <p><b>Conceitos Educacionais e de Engenharia de Software Aplicados:</b></p>
 * <ul>
 *   <li><b>Leitura Preguiçosa (Lazy Evaluation / Streaming I/O):</b> Uso de {@link Files#lines(Path)} com {@link Stream},
 *       onde os dados são lidos do disco linha a linha através de um buffer, mantendo o consumo de memória constante (O(1))
 *       independentemente do tamanho do arquivo (mesmo em arquivos de dezenas de gigabytes);</li>
 *   <li><b>Padrão Produtor-Consumidor (Producer-Consumer) e Chunking:</b> A thread principal lê as linhas do CSV (produtora)
 *       e as agrupa em blocos de tamanho fixo ({@link #LINE_BATCH_SIZE}). Ao atingir o tamanho, o bloco é despachado
 *       para threads trabalhadoras (consumidoras);</li>
 *   <li><b>Concorrência com Thread Pool ({@link ExecutorService}):</b> Uso de {@link Executors#newFixedThreadPool(int)}
 *       para distribuir o processamento intensivo de CPU (quebra de Strings, conversões numéricas e instanciação de objetos)
 *       entre múltiplos núcleos do processador;</li>
 *   <li><b>Gerenciamento Automático de Tarefas (AutoCloseable ExecutorService):</b> A partir do Java 19/21,
 *       {@link ExecutorService} implementa {@link AutoCloseable}. Ao usá-lo dentro de um bloco {@code try-with-resources},
 *       o compilador garante o encerramento gracioso ({@code shutdown()}) e aguarda a conclusão de todas as tarefas submetidas;</li>
 *   <li><b>Segurança em Threads (Thread Safety e Imutabilidade):</b> Uso de {@link List#copyOf(java.util.Collection)}
 *       para criar cópias imutáveis e defensivas de cada lote antes do envio assíncrono, evitando condições de corrida (<i>race conditions</i>)
 *       enquanto o buffer principal é limpo com {@link List#clear()};</li>
 *   <li><b>Resiliência a Falhas:</b> Parsing tolerante com {@link Optional}, descartando registros corrompidos sem interromper o fluxo total.</li>
 * </ul>
 */
public class EfficientTransactionIngestor {

    /**
     * Quantidade de linhas brutas do arquivo CSV agrupadas em cada lote de processamento paralelo.
     *
     * <p><b>Critério de Escolha:</b> O valor {@code 2_500} foi calibrado para equilibrar:
     * <ul>
     *   <li>O overhead de troca de contexto e despacho de tarefas no {@link ExecutorService};</li>
     *   <li>A pegada de memória (footprint) na heap da JVM;</li>
     *   <li>A compatibilidade com o tamanho de lote do JDBC ({@link TransactionSQLRepository#JDBC_BATCH_SIZE}).</li>
     * </ul>
     * <p>Tipo: {@code int}. Constante estática e pública ({@code public static final}).</p>
     */
    public static final int LINE_BATCH_SIZE = 2_500;

    /**
     * Realiza a leitura do arquivo CSV em lotes (batches), processando o parsing e despachando cada lote
     * para o consumidor de forma concorrente utilizando um pool de threads.
     *
     * <p><b>Lógica Passo a Passo:</b></p>
     * <ol>
     *   <li><b>Obtenção do Caminho:</b> Cria a referência {@link Path} correspondente ao nome do arquivo informado.</li>
     *   <li><b>Abertura de Recursos Concorrentes (try-with-resources):</b>
     *       <ul>
     *         <li>Instancia um {@link ExecutorService} com um pool fixo de 10 threads para paralelização;</li>
     *         <li>Abre o {@link Stream} de linhas através de {@link Files#lines(Path)}, descartando a linha 1 (cabeçalho) com {@code .skip(1)}.</li>
     *       </ul>
     *   </li>
     *   <li><b>Iteração com Buffer:</b> Obtém um {@link java.util.Iterator} a partir do Stream e acumula as linhas em uma {@code List<String>}.</li>
     *   <li><b>Disparo de Lote Cheio:</b> Quando o buffer atinge {@link #LINE_BATCH_SIZE} (2.500 linhas):
     *       <ul>
     *         <li>Cria um snapshot imutável via {@link List#copyOf(java.util.Collection)};</li>
     *         <li>Submete a tarefa de parsing e consumo assíncrono ao executor ({@code executor.submit});</li>
     *         <li>Limpa o buffer local ({@code lineBatch.clear()}) para acumular o próximo lote na thread principal.</li>
     *       </ul>
     *   </li>
     *   <li><b>Despacho do Lote Residual:</b> Ao final da iteração, se ainda restarem linhas no buffer que não completaram
     *       2.500 registros, um último lote é criado e despachado.</li>
     *   <li><b>Bloqueio e Fechamento:</b> Ao sair do bloco {@code try-with-resources}, o {@link ExecutorService}
     *       bloqueia automaticamente a execução até que todas as 10 threads terminem suas tarefas pendentes.</li>
     * </ol>
     *
     * @param filename Caminho do arquivo CSV de origem (ex: {@code "data/PS_log.csv"}). Tipo: {@link String}.
     * @param batchConsumer Função de retorno (<i>callback</i>) que receberá cada lista de transações convertidas
     *                      (ex: {@code repository::saveAll}). Tipo: {@link Consumer}&lt;{@link List}&lt;{@link Transaction}&gt;&gt;.
     * @throws RuntimeException Caso ocorra qualquer falha de leitura de arquivo, I/O ou processamento.
     */
    public void readAsBatch(String filename, Consumer<List<Transaction>> batchConsumer) {
        // path: Representação independente de plataforma do caminho do arquivo no sistema operacional
        Path path = Path.of(filename);

        // Bloco try-with-resources gerenciando dois recursos fundamentais:
        // 1. executor: Pool com 10 threads reutilizáveis (Executors.newFixedThreadPool(10)).
        //    No Java 21, ExecutorService implementa AutoCloseable, aguardando automaticamente todas as tarefas terminarem.
        // 2. lines: Stream lazy de linhas do arquivo CSV. Garante que o descritor de arquivo aberto seja fechado.
        //    skip(1): Pula a primeira linha do CSV contendo os nomes das colunas (cabeçalho).
        try (ExecutorService executor = Executors.newFixedThreadPool(10);
             Stream<String> lines = Files.lines(path).skip(1)) {

            // iterator: Iterador tradicional obtido do Stream para permitir extração sob demanda em laço imperativo (while)
            var iterator = lines.iterator();

            // lineBatch: Lista que funciona como buffer temporário na thread leitora.
            // Inicializada com capacidade LINE_BATCH_SIZE (2.500) para evitar sucessivos redimensionamentos do ArrayList.
            List<String> lineBatch = new ArrayList<>(LINE_BATCH_SIZE);

            // Percorre o arquivo linha a linha enquanto houver dados disponíveis no stream
            while (iterator.hasNext()) {

                // line: Conteúdo em texto puro da linha atual do CSV
                String line = iterator.next();
                lineBatch.add(line);

                // Verifica se o lote atingiu a capacidade pré-configurada de 2.500 registros
                if (lineBatch.size() >= LINE_BATCH_SIZE) {
                    IO.println("Executando batch ingestor...");

                    // Cópia defensiva e imutável do lote atual via List.copyOf():
                    // Fundamental para a segurança concorrente (Thread-Safety)! Sem essa cópia,
                    // a linha lineBatch.clear() abaixo esvaziaria os dados enquanto a thread do pool ainda os estivesse lendo.
                    final List<String> currentLineBatch = List.copyOf(lineBatch);

                    // Submete de forma assíncrona o processamento deste lote para uma das 10 threads do pool
                    executor.submit(() -> executeBatch(currentLineBatch, batchConsumer));

                    // Limpa o buffer local da thread principal para começar a acumular o próximo lote do arquivo
                    lineBatch.clear();
                }

            }

            // Tratamento do lote residual: caso a quantidade total de linhas não seja múltiplo exato de 2.500,
            // processa os registros restantes acumulados no buffer antes de finalizar.
            if (!lineBatch.isEmpty()) {
                IO.println("Executando batch final ingestor...");
                final List<String> currentLineBatch = List.copyOf(lineBatch);
                executor.submit(() -> executeBatch(currentLineBatch, batchConsumer));
            }

        } catch (Exception ex) {
            // Encapsula quaisquer exceções de I/O ou de concorrência em uma RuntimeException informativa
            throw new RuntimeException("Erro ao ler o arquivo: " + filename, ex);
        }
    }

    /**
     * Método auxiliar privado executado concorrentemente pelas threads trabalhadoras do pool.
     *
     * <p><b>Propósito do Método:</b></p>
     * Receber uma lista de linhas em texto bruto, convertê-las em objetos de domínio {@link Transaction}
     * utilizando a Stream API, filtrar eventuais registros com erro de formatação e repassar a lista
     * resultante para o consumidor informado ({@code batchConsumer}).
     *
     * <p><b>Pipeline de Transformação:</b></p>
     * <ul>
     *   <li>{@code .stream()}: Converte o lote de linhas de texto em um fluxo;</li>
     *   <li>{@code .map(this::parseTransaction)}: Converte cada linha em {@code Optional<Transaction>};</li>
     *   <li>{@code .filter(Optional::isPresent)}: Mantém somente os registros que foram parseados com sucesso;</li>
     *   <li>{@code .map(Optional::get)}: Extrai a instância de {@link Transaction} do Optional;</li>
     *   <li>{@code .toList()}: Coleta as transações válidas em uma lista imutável;</li>
     *   <li>{@code batchConsumer.accept(transactionBatch)}: Entrega o lote de transações para o consumidor (ex: persistência no banco).</li>
     * </ul>
     *
     * @param lineBatch Lista imutável contendo as linhas de texto em formato CSV. Tipo: {@link List}&lt;{@link String}&gt;.
     * @param batchConsumer Ação de destino a ser executada com o lote de transações gerado.
     *                      Tipo: {@link Consumer}&lt;{@link List}&lt;{@link Transaction}&gt;&gt;.
     */
    private void executeBatch(List<String> lineBatch, Consumer<List<Transaction>> batchConsumer) {
        // transactionBatch: Lista imutável contendo as transações já convertidas e validadas a partir do lote de texto
        List<Transaction> transactionBatch = lineBatch
                .stream()
                .map(this::parseTransaction)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();
        // Invoca o callback consumidor fornecido passando o lote processado (ex: inserção em batch no MySQL)
        batchConsumer.accept(transactionBatch);
    }

    /**
     * Realiza a leitura e processamento do arquivo CSV utilizando uma abordagem puramente sequencial baseada em Streams.
     *
     * <p><b>Propósito do Método:</b></p>
     * Fornecer uma alternativa de leitura orientada a eventos item a item (streaming reativo simples),
     * onde cada transação individual é convertida e entregue imediatamente a um consumidor unitário.
     *
     * <p><b>Comparativo Didático:</b></p>
     * Diferente de {@link #readAsBatch(String, Consumer)}, este método não utiliza lotes nem múltiplas threads.
     * Ele é ideal para cenários onde a ação a ser executada para cada registro é muito leve e não exige inserções
     * agrupadas em banco de dados, ou onde a ordem estrita de cada linha precisa ser rigorosamente preservada.
     *
     * @param filename Caminho do arquivo CSV. Tipo: {@link String}.
     * @param consumer Ação a ser executada para cada transação individual processada. Tipo: {@link Consumer}&lt;{@link Transaction}&gt;.
     * @throws RuntimeException Caso ocorra erro de I/O na leitura do arquivo.
     */
    public void readAsStream(String filename, Consumer<Transaction> consumer) {
        Path path = Path.of(filename);
        // Abre o stream preguiçoso sobre o arquivo em bloco try-with-resources
        try (Stream<String> lines = Files.lines(path)) {
            lines
                    .skip(1) // Ignora o cabeçalho do arquivo CSV
                    .map(this::parseTransaction) // Converte linha em Optional<Transaction>
                    .filter(Optional::isPresent) // Remove falhas de parsing
                    .map(Optional::get) // Extrai o objeto Transaction
                    .forEach(consumer); // Executa o consumidor para cada transação individualmente
        } catch (Exception ex) {
            // Relança exceções de I/O de baixo nível como RuntimeException
            throw new RuntimeException("Erro ao ler o arquivo: " + filename, ex);
        }
    }

    /**
     * Converte uma linha individual de texto no formato CSV para o modelo de domínio {@link Transaction}.
     *
     * <p><b>Propósito e Resiliência:</b></p>
     * Desmembrar os campos separados por vírgula, validar os tipos de dados e montar a estrutura de objetos
     * do domínio financeiro (incluindo clientes de origem e destino). O retorno empacotado em {@link Optional}
     * implementa o padrão <i>Null-Object / Resilient Parsing</i>: caso uma linha contenha valores corrompidos,
     * colunas faltantes ou formatos numéricos inválidos, o método captura o erro, emite log e retorna
     * {@link Optional#empty()}, permitindo que o processamento do restante do arquivo continue sem interrupções.
     *
     * <p><b>Esquema de Colunas do CSV:</b></p>
     * <ul>
     *   <li>{@code chunks[0]}: {@code step} (int) — Unidade temporal da simulação;</li>
     *   <li>{@code chunks[1]}: {@code type} (String / TransactionType) — Tipo da transação (PAYMENT, CASH_OUT, etc.);</li>
     *   <li>{@code chunks[2]}: {@code amount} (BigDecimal) — Valor monetário envolvido na transação;</li>
     *   <li>{@code chunks[3]}: {@code name_origin} (String) — Identificador da conta de origem;</li>
     *   <li>{@code chunks[4]}: {@code old_balance_origin} (BigDecimal) — Saldo de origem antes da movimentação;</li>
     *   <li>{@code chunks[5]}: {@code new_balance_origin} (BigDecimal) — Saldo de origem após a movimentação;</li>
     *   <li>{@code chunks[6]}: {@code name_recipient} (String) — Identificador da conta de destino;</li>
     *   <li>{@code chunks[7]}: {@code old_balance_recipient} (BigDecimal) — Saldo de destino antes da movimentação;</li>
     *   <li>{@code chunks[8]}: {@code new_balance_recipient} (BigDecimal) — Saldo de destino após a movimentação;</li>
     *   <li>{@code chunks[9]}: {@code is_fraud} (boolean) — Marcador real de fraude (valor "1" para verdadeiro);</li>
     *   <li>{@code chunks[10]}: {@code is_flagged_fraud} (boolean) — Marcador de heurística de fraude (valor "1" para verdadeiro).</li>
     * </ul>
     *
     * @param line Linha bruta em texto lida do arquivo CSV. Tipo: {@link String}.
     * @return {@link Optional} contendo o objeto {@link Transaction} se o parse for bem-sucedido, ou {@link Optional#empty()} em caso de falha.
     */
    private Optional<Transaction> parseTransaction(String line) {
        try {
            // chunks: Array de Strings gerado pela divisão da linha com base no delimitador vírgula (,)
            String[] chunks = line.split(",");

            // Conversão dos atributos primários da transação:
            int step = Integer.parseInt(chunks[0]);
            // Converte o texto da coluna de tipo na constante correspondente do Enum TransactionType
            TransactionType type = TransactionType.valueOf(chunks[1]);

            // Validação defensiva: assegura que o campo de valor financeiro não seja nulo nem esteja em branco
            if (chunks[2] == null || chunks[2].trim().isEmpty()) throw new IllegalArgumentException("O valor de amount nao pode ser nulo nem vazio");
            // BigDecimal é utilizado para valores monetários evitando problemas de arredondamento inerentes a float/double
            BigDecimal amount = new BigDecimal(chunks[2]);

            // Instanciação do cliente de origem (Customer Origin) com nome e saldos antes/depois
            var origin = new TransactionCustomer(chunks[3], new BigDecimal(chunks[4]), new BigDecimal(chunks[5]));
            // Instanciação do cliente recebedor (Customer Recipient) com nome e saldos antes/depois
            var recipient = new TransactionCustomer(chunks[6], new BigDecimal(chunks[7]), new BigDecimal(chunks[8]));

            // Avalia se o campo de fraude real é igual a "1"
            boolean isFraud = "1".equals(chunks[9]);

            // Avalia se a suspeita de fraude sinalizada pelo sistema de monitoramento é igual a "1"
            boolean isFlaggedFraud = "1".equals(chunks[10]);

            // Retorna o objeto Transaction construído empacotado em um Optional não-nulo
            return Optional.of(new Transaction(step, type, amount, origin, recipient, isFraud, isFlaggedFraud));
        } catch (Exception e) {
            // Em caso de formato inválido (NumberFormatException, ArrayIndexOutOfBoundsException, etc.),
            // registra o erro no canal padrão de erro (System.err) e retorna Optional.empty() para descartar a linha com segurança
            System.err.println("Erro ao fazer parse: " + line + " | " + e);
            return Optional.empty();
        }
    }

}
