package br.com.zenon;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Classe principal e ponto de entrada da aplicação de detecção de fraudes (Zenon Fraud Detector).
 *
 * <p><b>Propósito e Responsabilidade no Projeto:</b></p>
 * Atua como classe orquestradora (Driver Class) que integra, executa e valida todos os componentes
 * do sistema antifraude. Seu objetivo educacional é exercitar de ponta a ponta:
 * <ol>
 *   <li><b>Modelagem de Domínio:</b> Criação e manipulação de entidades com {@link Record} e {@link Enum};</li>
 *   <li><b>Ingestão de Dados (ETL):</b> Leitura de arquivos CSV com resiliência a dados corrompidos;</li>
 *   <li><b>Processamento Analítico Funcional:</b> Uso avançado da <i>Stream API</i> para consultas estatísticas;</li>
 *   <li><b>Padrão de Repositório (Repository Pattern):</b> Abstração de acesso a dados com polimorfismo;</li>
 *   <li><b>Benchmark de Algoritmos:</b> Medição prática de desempenho entre busca linear O(N) e busca hash O(1).</li>
 * </ol>
 */
public class Main {

    /**
     * Ponto de entrada da aplicação.
     *
     * <p><b>Conceito Educacional - Métodos Main Simplificados (Java 21+ / JEP 445 / JEP 463 / JEP 477):</b></p>
     * No Java moderno, a assinatura tradicional e verbosa {@code public static void main(String[] args)}
     * foi simplificada. O compilador permite declarar o método de inicialização sem modificador {@code public}
     * e sem a obrigatoriedade do array de argumentos {@code String[] args} quando este não for utilizado,
     * reduzindo a cerimônia de código boilerplate.
     *
     * <p>Também faz uso de {@code IO.println(...)}, recurso de I/O em console simplificado introduzido
     * nas versões recentes do Java para tornar a saída no console mais direta.</p>
     */
    static void main() {

        // ====================================================================================
        // ETAPA 1: TESTE DE MODELAGEM E INSTANCIAÇÃO DIRETA DOS RECORDS
        // ====================================================================================

        // transaction1: Variável do tipo Transaction.
        // Representa uma operação financeira cotidiana, legítima e sem fraude (isFraud = false, isFlaggedFraud = false).
        // Serve para verificar a validação de parâmetros e a correta inicialização dos records aninhados.
        Transaction transaction1 = new Transaction(
                1,
                TransactionType.PAYMENT,
                new BigDecimal("9839.64"),
                new TransactionCustomer("C1231006815", new BigDecimal("170136.0"), new BigDecimal("160296.36")),
                new TransactionCustomer("M1979787155", new BigDecimal("0.0"), new BigDecimal("0.0")),
                false,
                false
        );

        // transaction2: Variável do tipo Transaction.
        // Representa uma transação fraudulenta confirmada de alto valor (CASH_OUT, R$ 850.002,52, isFraud = true).
        // Permite validar a criação de instâncias com características críticas de risco.
        Transaction transaction2 = new Transaction(
                743,
                TransactionType.CASH_OUT,
                new BigDecimal("850002.52"),
                new TransactionCustomer("C1280323807", new BigDecimal("850002.52"), new BigDecimal("0.0")),
                new TransactionCustomer("C873221189", new BigDecimal("6510099.11"), new BigDecimal("7360101.63")),
                true,
                false
        );

        // Exibe as transações no console padrão.
        // O Java invoca automaticamente o método toString() gerado pelo compilador para cada Record,
        // exibindo todos os campos e valores de forma legível e estruturada.
        System.out.println("Imprimindo transação 1: " + transaction1);
        System.out.println("Imprimindo transação 2: " + transaction2);

        // ====================================================================================
        // ETAPA 2: INGESTÃO DE ARQUIVO CSV COM DADOS REAIS (DATASET PAYSIM)
        // ====================================================================================

        IO.println("---------- Usando a classe TransactionIngestor---------------------------------------");

        // Instancia o componente responsável pela leitura, limpeza e conversão do arquivo CSV
        TransactionIngestor transactionIngestor = new TransactionIngestor();

        // transactions: Lista imutável (List<Transaction>) contendo as transações válidas carregadas.
        // Processa o arquivo CSV com dados reais da simulação PaySim, aplicando o teto de registros definidos no Ingestor.
        List<Transaction> transactions = transactionIngestor.readTransactionsNew(
                "/home/alexandre/Projetos-Pratica-UNIPDS/zenon-fraud-detector/data/PS_20174392719_1491204439457_log.csv"
        );
        IO.println(transactions.size()); // Imprime a quantidade de transações carregadas com sucesso

        // Exibe no console uma amostragem das 10 primeiras transações através de uma Stream
        // .limit(10): intercepta o fluxo após os 10 primeiros elementos
        // .forEach(System.out::println): consome e imprime cada elemento usando Method Reference
        transactions.stream()
                .limit(10)
                .forEach(System.out::println);

        // ====================================================================================
        // ETAPA 3: TESTE DE RESILIÊNCIA E TOLERÂNCIA A DADOS CORROMPIDOS (BAD DATA)
        // ====================================================================================

        IO.println("---------- Usando a classe TransactionIngestor2 com tratamento de erros-------------------");

        // transactionsBadData: Coleção resultante da leitura de um arquivo propositalmente defeituoso.
        // Contém anomalias como: steps negativos ou zerados, valores de amount negativos, tipos inválidos, etc.
        // Demonstra a robustez da aplicação: as linhas inválidas geram Optional.empty() e são descartadas
        // suavemente pela Stream, sem que a execução seja interrompida por exceções fatais.
        List<Transaction> transactionsBadData = transactionIngestor.readTransactionsNew(
                "/home/alexandre/Projetos-Pratica-UNIPDS/zenon-fraud-detector/data/paysim_with_bad_data.csv"
        );
        IO.println(transactionsBadData.size()); // Imprime a quantidade total de linhas válidas que foram aceitas
        transactionsBadData.forEach(IO::println); // Exibe cada registro válido aceito

        // ====================================================================================
        // ETAPA 4: ANÁLISES ESTATÍSTICAS E HEURÍSTICAS COM STREAM API (TAREFA 5)
        // ====================================================================================

        IO.println("---------- Tarefa 5 - Streams  -------------------");

        // fraudAnalyzer: Instância do serviço analítico, encapsulando as operações de Stream sobre a lista de transações
        var fraudAnalyzer = new FraudAnalyzer(transactions);

        // Tarefa 5.1: Contagem total de fraudes confirmadas
        // fraudCont: Variável do tipo primitivo 'long' que armazena a quantidade de transações fraudulentas
        long fraudCont = fraudAnalyzer.countFrauds();
        IO.println("1. Total de fraudes: " + fraudCont);

        // Tarefa 5.2: Identificação das 3 fraudes de maior valor financeiro (Top 3 Fraudes)
        // highestFraudsAmounts: Lista contendo os montantes monetários (List<BigDecimal>) em ordem decrescente
        List<BigDecimal> highestFraudsAmounts = fraudAnalyzer.findHighestValueFraudsAmounts(3);
        IO.println("2. Top 3 Fraudes de Maior Valor: ");
        // String.formatted(...) com especificador '%.2f' formata o valor numérico com exatamente duas casas decimais
        highestFraudsAmounts.forEach(amount -> IO.println("- %.2f".formatted(amount)));

        // Tarefa 5.3: Identificação dos 5 maiores clientes emissores associados a fraudes (sem duplicações)
        // suspiciousClients: Lista de identificadores de clientes (List<String>) distintos
        List<String> suspiciousClients = fraudAnalyzer.findTopSuspiciousClients(5);
        IO.println("3. Clientes Suspeitos:");
        suspiciousClients.forEach(IO::println);

        // Tarefa 5.4: Apuração do impacto financeiro total (soma cumulativa dos montantes das fraudes)
        // totalFraudLoss: Instância de BigDecimal com a soma precisa de todos os valores de fraude
        BigDecimal totalFraudLoss = fraudAnalyzer.calculateTotalFraudsLoss();
        IO.println("4. Prejuízo Total: " + totalFraudLoss);

        // Tarefa 5.5: Distribuição e contagem de fraudes agrupadas por categoria/tipo de operação
        // fraudCountByType: Estrutura Map<TransactionType, Long> associando cada categoria ao total de ocorrências
        Map<TransactionType, Long> fraudCountByType = fraudAnalyzer.countFraudsByType();
        IO.println("5. Fraudes por Tipo: ");
        // Map.forEach aceita um BiConsumer (chave, valor)
        // itera sobre o Map formatando e exibindo cada tipo de transação com seu respectivo número de fraudes
        fraudCountByType.forEach((type, count) -> IO.println("- %s: %d".formatted(type, count)) );
        /*
        * itera sobre um Map contendo a contagem de transações fraudulentas agrupadas por tipo de transação
        * e imprime cada entrada no console em uma string formatada de forma clara.
        * */

        // ====================================================================================
        // ETAPA 5: BENCHMARK DE PERFORMANCE - LISTA O(N) VS TABELA HASH O(1) (TAREFA 6)
        // ====================================================================================

        IO.println("---------- Tarefa 6 - benchmark  -------------------");

        // transactionListRepository: Referência declarada com o tipo da interface (TransactionRepository),
        // demonstrando o princípio de Inversão de Dependência (DIP) e polimorfismo.
        TransactionRepository transactionListRepository = new TransactionListRepository(transactions);

        // notFoundOriginName: Chave de teste inexistente ("C12345") para testar o comportamento de falha na busca
        String notFoundOriginName = "C12345";

        // ifPresentOrElse (Java 9+): Executa a primeira ação (Consumer) se o Optional contiver um valor;
        // caso contrário (se estiver vazio), executa a segunda ação (Runnable), evitando ifs e checks manuais de null.
        transactionListRepository.findByOriginName(notFoundOriginName)
                .ifPresentOrElse(IO::println, () -> IO.println("Transação não encontrada para o cliente: " + notFoundOriginName));

        // exitingOriginName: Chave de teste existente no dataset ("C1868032458") para medição de latência de busca
        String exitingOriginName = "C1868032458";

        // --- SUBETAPA 6.1: Medição com Busca Linear O(N) em List ---
        IO.println("---------- Busca com List - observe o tempo do retorno da pesquisa -------------------");

        // System.nanoTime(): Retorna a leitura atual do relógio monotônico de alta precisão da JVM em nanossegundos.
        // É a função recomendada para benchmarks (em vez de System.currentTimeMillis()), pois não é afetada
        // por sincronizações do relógio do sistema operacional (NTP) e possui resolução de nanossegundos.
        long startTimeList = System.nanoTime();

        // Executa a busca linear percorrendo sequencialmente a lista até encontrar o cliente
        transactionListRepository.findByOriginName(exitingOriginName)
                .ifPresentOrElse(IO::println, () -> IO.println("Transação não encontrada para o cliente: " + exitingOriginName));

        long endTimeList = System.nanoTime();

        // Converte o delta de nanossegundos para milissegundos dividindo por 1_000_000.0 (ponto flutuante)
        IO.println("Tempo de busa - List (ms): " + (endTimeList - startTimeList) / 1_000_000.0 + " ms");

        // --- SUBETAPA 6.2: Medição com Busca Indexada O(1) em Tabela Hash (Map) ---
        IO.println("---------- Busca com map - observe o tempo do retorno da pesquisa  -------------------");

        // Reatribui a mesma variável de interface para a implementação baseada em Map (Polimorfismo em ação)
        transactionListRepository = new TransactionMapRepository(transactions);

        startTimeList = System.nanoTime();

        // Executa a busca em tempo constante O(1) calculando o hashCode da chave
        transactionListRepository.findByOriginName(exitingOriginName)
                .ifPresentOrElse(IO::println, () -> IO.println("Transação não encontrada para o cliente: " + exitingOriginName));

        endTimeList = System.nanoTime();

        // Converte a duração de nanossegundos para milissegundos e exibe a drástica redução de tempo
        IO.println("Tempo de busa - Map (ms): " + (endTimeList - startTimeList) / 1_000_000.0 + " ms");
    }
}
