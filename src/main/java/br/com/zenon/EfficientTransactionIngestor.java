package br.com.zenon;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Ingestor eficiente de transações financeiras com suporte a streaming de arquivos,
 * Threads Virtuais (Java 21) e controle de concorrência via Semáforo.
 *
 * <p><b>Propósito e Responsabilidade:</b></p>
 * Atuar como o componente de alta vazão (throughput) da camada de ingestão de dados (<i>ETL - Extract, Transform, Load</i>),
 * projetado para processar arquivos CSV massivos (com milhões de registros) com consumo constante e previsível de memória RAM (O(1)).
 * Sua responsabilidade abrange:
 * <ol>
 *   <li>Ler o arquivo texto de forma preguiçosa (lazy evaluation) via streaming contínuo;</li>
 *   <li>Particionar os dados em blocos homogêneos (lotes de {@link #LINE_BATCH_SIZE} linhas);</li>
 *   <li>Disparar o parsing dos dados em paralelo através de <b>Threads Virtuais</b> (Project Loom - Java 21);</li>
 *   <li>Limitar a concorrência de escrita no banco de dados via {@link Semaphore}, evitando sobrecarga no SGBD;</li>
 *   <li>Despachar os lotes de entidades de domínio {@link Transaction} diretamente para a persistência.</li>
 * </ol>
 *
 * <p><b>Como se integra ao projeto:</b></p>
 * Representa a evolução arquitetural de ponta sobre a classe {@link TransactionIngestor}. É instanciado na classe
 * de execução {@link IngestionMain}, conectando a leitura concorrente do disco diretamente ao método em lote
 * {@link TransactionSQLRepository#saveAll(List)}, alimentando a base de dados MySQL de maneira altamente eficiente.
 *
 * <p><b>Conceitos Educacionais e de Engenharia de Software Aplicados:</b></p>
 * <ul>
 *   <li><b>Streaming I/O (Java NIO.2):</b> Utiliza {@link Files#lines(Path)} para ler dados sob demanda em buffer,
 *       sem carregar o arquivo inteiro para a memória heap, eliminando o risco de {@link OutOfMemoryError};</li>
 *   <li><b>Threads Virtuais (Virtual Threads - Java 21 / JEP 444):</b> Empregando {@link Executors#newVirtualThreadPerTaskExecutor()},
 *       a aplicação cria threads ultraleves gerenciadas pela JVM (e não pelo Sistema Operacional). Cada thread virtual consome
 *       apenas alguns centenas de bytes (em contraste com ~1MB de uma thread de plataforma do SO), permitindo criar
 *       milhares de tarefas concorrentes de parsing com custo negligenciável de chaveamento de contexto;</li>
 *   <li><b>Controle de Vazão e Backpressure com Semáforo ({@link Semaphore}):</b> Enquanto threads virtuais são abundantes e baratas,
 *       recursos de infraestrutura externa — como pools de conexões e buffers de sockets do MySQL — são finitos e caros.
 *       O semáforo {@link #dbPermits} restringe o número de lotes que podem interagir simultaneamente com o banco de dados a 10,
 *       impedindo saturação do MySQL, estouro de conexões (<i>connection exhaustion</i>) ou travamento por contenção de locks;</li>
 *   <li><b>Padrão Produtor-Consumidor (Producer-Consumer) e Chunking:</b> A thread principal atua como produtora
 *       fatiando o arquivo em blocos de 2.500 linhas, enquanto as threads virtuais atuam como consumidoras do parsing;</li>
 *   <li><b>Imutabilidade e Segurança de Memória (Thread Safety):</b> O uso de {@link List#copyOf(java.util.Collection)}
 *       produz um snapshot imutável de cada lote antes do envio assíncrono, prevenindo condições de corrida (<i>race conditions</i>)
 *       com a limpeza do buffer principal via {@link List#clear()};</li>
 *   <li><b>Gerenciamento de Ciclo de Vida (AutoCloseable ExecutorService):</b> No Java 21, o fechamento do {@code try-with-resources}
 *       aguarda automaticamente o término de todas as tarefas submetidas ao executor antes de prosseguir;</li>
 *   <li><b>Resiliência no Parsing:</b> Tratamento com {@link Optional} em {@link #parseTransaction(String)},
 *       descartando linhas defeituosas individualmente sem abortar a ingestão dos demais registros válidos.</li>
 * </ul>
 */
public class EfficientTransactionIngestor {

    /**
     * Quantidade de linhas brutas de texto do arquivo CSV agrupadas em cada lote de processamento paralelo.
     *
     * <p><b>Critério de Engenharia:</b> O valor {@code 2_500} equilibra a granulação do lote com o custo
     * de criação de tarefas assíncronas, garantindo volume suficiente para que cada lote amortize os custos
     * de rede e I/O de disco no banco de dados.
     * <p>Tipo: {@code int}. Constante estática e pública ({@code public static final}).</p>
     */
    public static final int LINE_BATCH_SIZE = 2_500;

    /**
     * Semáforo contador utilizado para impor um teto máximo de concorrência nas operações de banco de dados.
     *
     * <p><b>Propósito e Responsabilidade:</b></p>
     * Atuar como mecanismo de <i>Throttling / Backpressure</i>. Mesmo que a JVM seja capaz de criar dezenas
     * ou centenas de threads virtuais para fazer o parsing dos arquivos de texto em paralelo, o banco de dados
     * relacional (MySQL) possui limites estritos de conexões simultâneas (geralmente entre 100 e 151 por padrão).
     * <p>Configurado com 10 permissões ({@code permits}), este semáforo garante que no máximo 10 lotes de dados
     * tentem executar comandos de inserção ({@code batchConsumer.accept}) concorrentemente. Qualquer outra thread virtual
     * que tentar gravar dados ficará suspensa (sem consumir CPU) até que uma permissão seja liberada.</p>
     * <p>Tipo: {@link Semaphore}. Modificadores: {@code private final}.</p>
     */
    private final Semaphore dbPermits = new Semaphore(10);

    /**
     * Lê o arquivo CSV linha a linha, fatia em lotes de {@value #LINE_BATCH_SIZE} registros e despacha
     * o parsing e a ingestão de forma concorrente utilizando Threads Virtuais (Java 21).
     *
     * <p><b>Lógica Passo a Passo:</b></p>
     * <ol>
     *   <li><b>Conversão do Caminho:</b> Instancia o objeto {@link Path} a partir do nome do arquivo fornecido;</li>
     *   <li><b>Abertura de Recursos com try-with-resources:</b>
     *       <ul>
     *         <li>{@link Executors#newVirtualThreadPerTaskExecutor()}: Cria um executor baseado em Threads Virtuais
     *             (JEP 444), onde cada tarefa submetida roda em sua própria thread virtual leve;</li>
     *         <li>{@link Files#lines(Path)}: Abre um fluxo sob demanda (stream) sobre o arquivo em disco,
     *             descartando a primeira linha de cabeçalho via {@code .skip(1)};</li>
     *       </ul>
     *   </li>
     *   <li><b>Acumulação em Buffer:</b> Extrai o {@link java.util.Iterator} do Stream e adiciona as linhas na lista
     *       temporária {@code lineBatch};</li>
     *   <li><b>Disparo de Lote Cheio:</b> Ao acumular 2.500 linhas:
     *       <ul>
     *         <li>Cria um snapshot imutável via {@link List#copyOf(java.util.Collection)};</li>
     *         <li>Submete a tarefa para execução assíncrona em uma thread virtual ({@code executor.submit});</li>
     *         <li>Limpa o buffer local da thread leitora com {@code lineBatch.clear()};</li>
     *       </ul>
     *   </li>
     *   <li><b>Processamento Residual:</b> Se restarem linhas que não formaram um lote completo de 2.500 ao final do arquivo,
     *       um lote final é enviado para o executor;</li>
     *   <li><b>Sincronização e Fechamento:</b> Ao término do bloco {@code try}, o {@link ExecutorService} bloqueia e aguarda
     *       a conclusão de todas as threads virtuais ativas antes de liberar o fluxo da aplicação.</li>
     * </ol>
     *
     * @param filename Caminho relativo ou absoluto do arquivo CSV a ser ingerido. Tipo: {@link String}.
     * @param batchConsumer Função de callback funcional que receberá a lista de transações convertidas
     *                      (ex: {@code repository::saveAll}). Tipo: {@link Consumer}&lt;{@link List}&lt;{@link Transaction}&gt;&gt;.
     * @throws RuntimeException Caso ocorra qualquer falha irrecuperável de leitura de disco ou execução.
     */
    public void readAsBatch(String filename, Consumer<List<Transaction>> batchConsumer) {
        // path: Representação independente de sistema operacional do caminho do arquivo CSV
        Path path = Path.of(filename);

        // Bloco try-with-resources gerenciando dois recursos com fechamento automático:
        // 1. executor: Instância de ExecutorService baseada em Threads Virtuais do Java 21 (Executors.newVirtualThreadPerTaskExecutor()).
        //    Ao sair do bloco try, o método close() do executor aguarda a conclusão de todas as tarefas submetidas.
        // 2. lines: Stream lazy de linhas do arquivo CSV. Garante que os recursos de sistema (file descriptors) sejam desalocados.
        //    skip(1): Ignora a primeira linha do CSV contendo os nomes das colunas.
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
             Stream<String> lines = Files.lines(path).skip(1)) {

            // iterator: Iterador tradicional obtido a partir do Stream, permitindo avançar linha a linha em laço imperativo (while)
            var iterator = lines.iterator();

            // lineBatch: Lista que atua como buffer temporário na thread de leitura principal.
            // Inicializada com capacidade pré-alocada de 2.500 posições para evitar realocações dinâmicas de memória.
            List<String> lineBatch = new ArrayList<>(LINE_BATCH_SIZE);

            // Percorre o arquivo sequencialmente enquanto houver próximas linhas disponíveis no stream
            while (iterator.hasNext()) {

                // line: Conteúdo de texto puro da linha atual do CSV
                String line = iterator.next();
                lineBatch.add(line);

                // Condicional de disparo de lote: atinge o tamanho ideal de 2.500 registros
                if (lineBatch.size() >= LINE_BATCH_SIZE) {
                    IO.println("Executando batch ingestor...");

                    // Cópia defensiva e imutável do lote via List.copyOf():
                    // Essencial para Thread-Safety! Se passássemos 'lineBatch' diretamente para a thread virtual,
                    // a chamada subsequente lineBatch.clear() apagaria os dados enquanto a outra thread ainda os estivesse lendo.
                    final List<String> currentLineBatch = List.copyOf(lineBatch);

                    // Submete a execução do processamento do lote para uma thread virtual assíncrona
                    executor.submit(() -> {
                        try {
                            // Executa o parsing das linhas e o envio ao consumidor no contexto da thread virtual
                            executeBatch(currentLineBatch, batchConsumer);
                        } catch (Exception e) {
                            // Captura e imprime eventuais exceções não tratadas na thread virtual para evitar falhas silenciosas
                            e.printStackTrace();
                        }
                    });

                    // Limpa o buffer local da thread leitora para acumular as próximas 2.500 linhas do arquivo
                    lineBatch.clear();
                }

            }

            // Tratamento do lote residual: caso o total de linhas do arquivo não seja múltiplo exato de 2.500,
            // processa as linhas restantes que ficaram no buffer
            if (!lineBatch.isEmpty()) {
                IO.println("Executando batch final ingestor...");
                final List<String> currentLineBatch = List.copyOf(lineBatch);
                executor.submit(() -> {
                    try {
                        executeBatch(currentLineBatch, batchConsumer);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }

        } catch (Exception ex) {
            // Empacota quaisquer falhas de I/O ou sistema em uma RuntimeException com mensagem clara e causa original
            throw new RuntimeException("Erro ao ler o arquivo: " + filename, ex);
        }
    }

    /**
     * Método auxiliar privado executado concorrentemente pelas Threads Virtuais.
     *
     * <p><b>Propósito do Método:</b></p>
     * Transformar a lista de linhas textuais em instâncias do modelo de domínio {@link Transaction}
     * utilizando a Stream API, e repassar o lote para o {@code batchConsumer} sob a proteção
     * do semáforo de concorrência {@link #dbPermits}.
     *
     * <p><b>Lógica Passo a Passo e Concorrência:</b></p>
     * <ol>
     *   <li><b>Pipeline Funcional (CPU-bound):</b> Converte cada linha em objeto de domínio, filtrando falhas com {@link Optional};</li>
     *   <li><b>Aquisição de Permissão ({@code dbPermits.acquire()}):</b> Antes de chamar o consumidor que acessará o banco de dados,
     *       solicita um alvará ao semáforo. Se as 10 permissões estiverem ocupadas, a Thread Virtual é suspensa sem onerar a CPU;</li>
     *   <li><b>Execução do Consumidor:</b> Executa {@code batchConsumer.accept(transactionBatch)} (ex: inserção JDBC em lote no MySQL);</li>
     *   <li><b>Liberação Garantida ({@code finally { dbPermits.release(); }}):</b> O bloco {@code finally} assegura que a permissão
     *       seja impreterivelmente devolvida ao semáforo, mesmo se ocorrer erro ou exceção no banco;</li>
     *   <li><b>Tratamento de Interrupção:</b> Se a thread for interrompida durante a espera do semáforo, preserva o status
     *       de interrupção através de {@code Thread.currentThread().interrupt()}.</li>
     * </ol>
     *
     * @param lineBatch Lista imutável contendo as linhas de texto bruto em formato CSV. Tipo: {@link List}&lt;{@link String}&gt;.
     * @param batchConsumer Consumidor de destino que processará o lote de transações (ex: persistência SQL).
     *                      Tipo: {@link Consumer}&lt;{@link List}&lt;{@link Transaction}&gt;&gt;.
     */
    private void executeBatch(List<String> lineBatch, Consumer<List<Transaction>> batchConsumer) {
        // transactionBatch: Lista imutável de transações convertidas com sucesso a partir do lote de linhas
        List<Transaction> transactionBatch = lineBatch
                .stream()
                .map(this::parseTransaction) // Aplica parseTransaction para cada String da lista
                .filter(Optional::isPresent) // Descarta registros inválidos que retornaram Optional.empty()
                .map(Optional::get)          // Desembrulha o objeto Transaction de dentro do Optional
                .toList();                   // Coleta em uma List imutável (Java 16+)

        try {
            // acquire(): Solicita uma permissão ao semáforo.
            // Se já houver 10 operações concorrentes em andamento, esta thread virtual fica bloqueada
            // (unmounted do carrier thread do sistema operacional) até que uma permissão fique disponível.
            dbPermits.acquire();
            try {
                // Invoca o callback consumidor fornecido passando o lote processado (ex: inserção em batch no MySQL)
                batchConsumer.accept(transactionBatch);
            } finally {
                // release(): Devolve obrigatoriamente a permissão ao semáforo dentro do bloco 'finally',
                // garantindo que nunca ocorra vazamento de permissões (permit leak) ou deadlock.
                dbPermits.release();
            }
        } catch (InterruptedException e) {
            // Boa prática de concorrência em Java: quando InterruptedException é capturada, a flag de interrupção
            // da thread é limpa pelo runtime. Chamar interrupt() restaura o status da flag para o chamador saber que houve cancelamento.
            Thread.currentThread().interrupt();
        }
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
