package br.com.zenon;

// Importa BigDecimal para cálculos monetários com precisão exata, evitando erros de arredondamento de ponto flutuante (double/float)
import java.math.BigDecimal;
// Importa Comparator para definir critérios de ordenação customizados de objetos em Streams
import java.util.Comparator;
// Importa a interface List para manipulação de coleções ordenadas de elementos
import java.util.List;
// Importa a interface Map para trabalhar com estruturas de chave-valor (ex: agrupamento por tipo de transação)
import java.util.Map;
// Importa a classe utilitária Objects para validações de integridade e checagem de nulidade em tempo de execução
import java.util.Objects;
// Importa Collectors, que fornece operações terminais avançadas para transformar fluxos em coleções, mapas ou valores agregados
import java.util.stream.Collectors;

/**
 * Serviço analítico responsável por processar e extrair inteligência estatística sobre transações financeiras.
 *
 * <p><b>Objetivo Educacional e Arquitetura:</b></p>
 * Atua na camada de serviço/análise (Analytical Service Layer) do sistema antifraude Zenon.
 * Esta classe encapsula as consultas estatísticas e heurísticas de detecção de fraudes
 * demonstrando o potencial expressivo, declarativo e imutável da <b>Stream API</b> introduzida no Java 8+.
 *
 * <p><b>Conceitos Fundamentais da Stream API Aplicados:</b></p>
 * <ul>
 *   <li><b>Imutabilidade da Fonte:</b> Nenhuma operação em Streams altera a coleção original ({@code transactions}).
 *       Elas apenas criam novos fluxos e novas coleções como saída.</li>
 *   <li><b>Operações Intermediárias (Lazy / Preguiçosas):</b> Métodos como {@code filter}, {@code map},
 *       {@code sorted}, {@code limit} e {@code distinct} não executam processamento até que uma operação terminal seja invocada.</li>
 *   <li><b>Operações Terminais (Eager):</b> Métodos como {@code toList}, {@code reduce} e {@code collect}
 *       disparam a computação de todos os elementos e produzem o resultado final.</li>
 * </ul>
 */
public class FraudAnalyzer {

    //TODO
    //Apenas transações onde isFraud == true, imprima o tamanho da lista.
    //Imprima as 3 fraudes de maior valor (amount).
    //Obter apenas os nomes dos clientes de origem (nameOrig) dessas fraudes e depois gere uma lista sem repetições (Set ou distinct) com os 5 maiores clientes suspeitos.
    //Calcule o prejuízo total causado pelas fraudes (soma dos amount).
    //Conte quantas fraudes ocorreram por tipo de transação (CASH_OUT, TRANSFER, etc...).

    /**
     * Coleção original e imutável de transações financeiras a ser analisada.
     * <p>Tipo de dado: {@code List<Transaction>}.</p>
     * <p>O modificador {@code final} impede que a referência seja alterada após a construção do objeto,
     * garantindo a segurança de threads (thread safety) e consistência analítica.</p>
     */
    private final List<Transaction> transactions;

    /**
     * Construtor da classe {@code FraudAnalyzer}.
     *
     * <p>Recebe a coleção de transações que servirá como conjunto de dados para todas as métricas analíticas.</p>
     *
     * @param transactions Coleção completa de transações ingeridas.
     *                     Não pode ser nula (garantido por validação Fail-Fast).
     * @throws NullPointerException Se o parâmetro {@code transactions} for nulo.
     */
    public FraudAnalyzer(List<Transaction> transactions) {
        // Objects.requireNonNull verifica se o parâmetro fornecido é nulo.
        // Se for null, interrompe imediatamente a execução lançando uma NullPointerException com a mensagem indicada.
        // Se for válido (não nulo), retorna a própria referência da lista para ser atribuída ao atributo 'this.transactions'.
        this.transactions = Objects.requireNonNull(transactions, "A lista de transações não pode ser nula.");
    }

    /**
     * Tarefa 1: Identifica todas as transações confirmadas como fraude ({@code isFraud == true}),
     * imprime a quantidade no console e retorna o total apurado.
     *
     * <p><b>Detalhamento do Pipeline Funcional:</b></p>
     * <ol>
     *   <li>{@code this.transactions.stream()}: Inicializa o fluxo sequencial sobre os dados.</li>
     *   <li>{@code .filter(Transaction::isFraud)}: Operação intermediária que recebe um {@code Predicate}.
     *       Utiliza Method Reference para {@link Transaction#isFraud()}, equivalente a {@code t -> t.isFraud()}.
     *       Apenas registros onde {@code isFraud == true} continuam no fluxo.</li>
     *   <li>{@code .toList()}: Operação terminal (Java 16+) que materializa o fluxo em uma lista imutável.</li>
     *   <li>{@code fraudTransactions.size()}: Recupera a quantidade de elementos contidos na lista filtrada.</li>
     * </ol>
     *
     * @return Quantidade total de transações fraudulentas localizadas (tipo primitivo {@code long}).
     */
    public long countFrauds() {
        // Inicia o fluxo a partir da lista original de transações
        List<Transaction> fraudTransactions = this.transactions.stream()
                // filter: recebe um Predicate (função booleana).
                // Usamos Method Reference (Transaction::isFraud), que equivale à expressão lambda:
                // (transacao) -> transacao.isFraud() == true
                .filter(Transaction::isFraud)
                // toList: operação terminal que materializa os elementos filtrados em uma List imutável
                .toList();

        // Imprime no console o tamanho da lista resultante através do método size()
        System.out.println("Tamanho da lista de fraudes: " + fraudTransactions.size());

        // Retorna o tamanho da lista como 'long' para satisfazer o contrato esperado pelo chamador em Main.java
        return fraudTransactions.size();
    }

    /**
     * Tarefa 2: Identifica os N maiores valores financeiros movimentados em operações fraudulentas.
     *
     * <p><b>Detalhamento do Pipeline Funcional:</b></p>
     * <ol>
     *   <li>{@code .filter(Transaction::isFraud)}: Isola apenas transações fraudulentas.</li>
     *   <li>{@code .map(Transaction::amount)}: Projeta o fluxo de objetos {@link Transaction}
     *       para um fluxo de valores monetários ({@link BigDecimal}), descartando outros atributos.</li>
     *   <li>{@code .sorted(Comparator.reverseOrder())}: Ordena os valores em ordem decrescente (do maior para o menor).
     *       {@link Comparator#reverseOrder()} inverte a ordem natural do método {@link BigDecimal#compareTo(BigDecimal)}.</li>
     *   <li>{@code .limit(limit)}: Operação intermediária de curto-circuito que restringe o fluxo aos primeiros N elementos.</li>
     *   <li>{@code .toList()}: Coleta os valores selecionados em uma lista imutável.</li>
     * </ol>
     *
     * @param limit Quantidade máxima de valores de topo a serem retornados (ex: 3 para Top 3). Tipo primitivo: {@code int}.
     * @return Lista imutável contendo os maiores montantes monetários de fraudes em ordem decrescente ({@code List<BigDecimal>}).
     */
    public List<BigDecimal> findHighestValueFraudsAmounts(int limit) {
        // Inicia o processamento com Stream API
        return this.transactions.stream()
                // filter: seleciona apenas as transações fraudulentas
                .filter(Transaction::isFraud)
                // map: transforma cada objeto 'Transaction' no seu respectivo valor 'amount' (BigDecimal)
                .map(Transaction::amount)
                // sorted(Comparator.reverseOrder()): ordena os valores de BigDecimal em ordem decrescente (do maior para o menor)
                // reverseOrder() inverte a ordem natural do método compareTo de BigDecimal
                .sorted(Comparator.reverseOrder())
                // limit(N): operação intermediária de curto-circuito (short-circuiting) que trunca o fluxo aos primeiros N elementos
                .limit(limit)
                // toList(): consolida os elementos do fluxo resultante em uma lista imutável
                .toList();
    }

    /**
     * Tarefa 3: Localiza os clientes de origem ({@code nameOrig}) associados às fraudes de maior valor financeiro,
     * eliminando repetições e restringindo aos N principais clientes suspeitos.
     *
     * <p><b>Detalhamento do Pipeline Funcional:</b></p>
     * <ol>
     *   <li>{@code .filter(Transaction::isFraud)}: Isola eventos fraudulentos.</li>
     *   <li>{@code .sorted(Comparator.comparing(Transaction::amount).reversed())}: Ordena as transações
     *       com base no valor movimentado ({@code amount}) de forma decrescente, garantindo que as maiores fraudes apareçam primeiro.</li>
     *   <li>{@code .map(t -> t.transactionCustomerOrigin().name())}: Navega pelo objeto e extrai a String
     *       com o identificador do cliente emissor (ex: "C1280323807").</li>
     *   <li>{@code .distinct()}: Operação intermediária com estado (stateful) que remove duplicatas no fluxo
     *       comparando os elementos através de {@link String#equals(Object)} e {@link String#hashCode()}.
     *       Como as fraudes já vêm ordenadas pelo maior valor, o cliente preservado no fluxo é associado à fraude de maior valor.</li>
     *   <li>{@code .limit(limit)}: Trunca a lista nos primeiros N clientes mais suspeitos.</li>
     *   <li>{@code .toList()}: Coleta o resultado em lista imutável.</li>
     * </ol>
     *
     * @param limit Quantidade máxima de clientes a retornar (ex: 5). Tipo primitivo: {@code int}.
     * @return Lista de identificadores únicos de clientes em ordem de periculosidade financeira ({@code List<String>}).
     */
    public List<String> findTopSuspiciousClients(int limit) {
        // Inicia a Stream funcional
        return this.transactions.stream()
                // filter: considera somente transações confirmadas como fraude
                .filter(Transaction::isFraud)
                // sorted: ordena as fraudes com base no valor (amount) em ordem decrescente
                // Comparator.comparing(Transaction::amount).reversed(): extrai a chave 'amount' e inverte a ordenação
                .sorted(Comparator.comparing(Transaction::amount).reversed())
                // map: navega da transação para o cliente de origem (transactionCustomerOrigin) e obtém o nome (name)
                // Equivalente a: transacao -> transacao.transactionCustomerOrigin().name()
                .map(t -> t.transactionCustomerOrigin().name())
                // distinct: operação intermediária com estado (stateful) que descarta elementos duplicados
                // utilizando os métodos equals() e hashCode() de String
                .distinct()
                // limit: restringe o fluxo aos primeiros N clientes distintos
                .limit(limit)
                // toList: coleta os nomes únicos em uma lista imutável
                .toList();
    }

    /**
     * Tarefa 4: Calcula o prejuízo financeiro consolidado acumulado causado por todas as fraudes.
     *
     * <p><b>Conceito de Programação Funcional - Redução (Fold / Reduce):</b></p>
     * O método {@link java.util.stream.Stream#reduce(Object, java.util.function.BinaryOperator)} combina
     * repetidamente todos os elementos do fluxo em um único valor resultante através de:
     * <ul>
     *   <li><b>Elemento de Identidade:</b> {@link BigDecimal#ZERO}, que atua como ponto de partida da soma
     *       e o valor padrão retornado caso a lista esteja vazia (pois {@code x + 0 == x}).</li>
     *   <li><b>Operador Binário (Acumulador):</b> {@link BigDecimal#add(BigDecimal)}, invocado como
     *       Method Reference para acumular cada valor ao subtotal corrente.</li>
     * </ul>
     *
     * @return Soma total de todos os valores das transações fraudulentas como {@link BigDecimal}.
     */
    public BigDecimal calculateTotalFraudsLoss() {
        // Inicia a Stream para redução agregada dos valores
        return this.transactions.stream()
                // filter: filtra apenas as transações fraudulentas
                .filter(Transaction::isFraud)
                // map: projeta o fluxo de transações para um fluxo apenas de valores monetários (amount)
                .map(Transaction::amount)
                // reduce(identidade, acumulador): operação terminal de redução (folding)
                // BigDecimal.ZERO: valor inicial neutro (identidade da soma)
                // BigDecimal::add: função de combinação que soma cada elemento ao total acumulado
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    /**
     * Tarefa 5: Agrupa e totaliza as ocorrências de fraudes para cada categoria de transação financeira.
     *
     * <p><b>Detalhamento do Coletor de Agrupamento (GroupingBy & Counting):</b></p>
     * A operação {@link Collectors#groupingBy(java.util.function.Function, java.util.stream.Collector)}
     * funciona de forma análoga à cláusula SQL {@code GROUP BY}:
     * <ul>
     *   <li><b>Função Classificadora (Classifier):</b> {@code Transaction::type} extrai a chave
     *       (a constante de {@link TransactionType}, ex: {@code CASH_OUT}, {@code TRANSFER}).</li>
     *   <li><b>Coletor Secundário (Downstream Collector):</b> {@link Collectors#counting()} contabiliza
     *       quantos registros pertencem a cada uma dessas categorias e retorna o total como {@link Long}.</li>
     * </ul>
     *
     * @return Um {@link Map} onde a chave é o tipo de transação ({@link TransactionType})
     *         e o valor é o total de ocorrências de fraudes para esse tipo ({@link Long}).
     */
    public Map<TransactionType, Long> countFraudsByType() {
        // Inicia a Stream para agregação e particionamento dos dados
        return this.transactions.stream()
                // filter: seleciona apenas as fraudes confirmadas
                .filter(Transaction::isFraud)
                // collect: operação terminal que executa uma redução mutável no fluxo
                // Collectors.groupingBy: agrupa elementos pela chave classificada (Transaction::type)
                // Collectors.counting(): downstream collector que conta o número de elementos associados a cada chave no mapa
                .collect(Collectors.groupingBy(
                        Transaction::type,
                        Collectors.counting()
                ));
    }
}
