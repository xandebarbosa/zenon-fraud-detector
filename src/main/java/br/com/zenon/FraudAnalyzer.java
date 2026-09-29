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
 * Classe responsável por analisar e extrair métricas estatísticas sobre transações financeiras,
 * com foco na identificação, quantificação e categorização de fraudes utilizando a Stream API do Java.
 */
public class FraudAnalyzer {

    //TODO
    //Apenas transações onde isFraud == true, imprima o tamanho da lista.
    //Imprima as 3 fraudes de maior valor (amount).
    //Obter apenas os nomes dos clientes de origem (nameOrig) dessas fraudes e depois gere uma lista sem repetições (Set ou distinct) com os 5 maiores clientes suspeitos.
    //Calcule o prejuízo total causado pelas fraudes (soma dos amount).
    //Conte quantas fraudes ocorreram por tipo de transação (CASH_OUT, TRANSFER, etc...).

    // Atributo privado e imutável (final) que armazena a coleção original de transações a ser analisada.
    // O uso de 'final' garante que a referência da lista não poderá ser reatribuída após a criação da instância.
    private final List<Transaction> transactions;

    /**
     * Construtor da classe FraudAnalyzer.
     *
     * @param transactions Lista com todas as transações carregadas que serão analisadas.
     *                     Não pode ser nula para garantir a segurança em tempo de execução (Fail-Fast).
     */
    public FraudAnalyzer(List<Transaction> transactions) {
        // Objects.requireNonNull verifica se o parâmetro fornecido é nulo.
        // Se for null, interrompe imediatamente a execução lançando uma NullPointerException com a mensagem indicada.
        // Se for válido (não nulo), retorna a própria referência da lista para ser atribuída ao atributo 'this.transactions'.
        this.transactions = Objects.requireNonNull(transactions, "A lista de transações não pode ser nula.");
    }

    /**
     * 1. Apenas transações onde isFraud == true, imprima o tamanho da lista e retorne a contagem.
     *
     * Pipeline funcional:
     * - stream(): inicia um fluxo sequencial a partir da coleção 'transactions'.
     * - filter(Transaction::isFraud): operação intermediária que avalia cada elemento,
     *   mantendo apenas as transações confirmadas como fraude.
     * - toList(): operação terminal (Java 16+) que coleta o fluxo em uma lista imutável.
     *
     * @return Quantidade total de transações fraudulentas (long).
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
     * 2. Identifica e retorna os N maiores valores financeiros (amount) entre as transações fraudulentas.
     *
     * @param limit Quantidade máxima de registros a retornar (ex: Top 3).
     * @return Lista contendo os maiores valores de fraude em ordem decrescente.
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
     * 3. Obtém os nomes dos clientes de origem (nameOrig) associados às maiores fraudes,
     * eliminando repetições (distinct) e limitando aos N principais clientes suspeitos.
     *
     * @param limit Quantidade máxima de clientes a retornar (ex: 5).
     * @return Lista de Strings contendo identificadores únicos dos clientes mais suspeitos.
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
     * 4. Calcula o prejuízo financeiro acumulado total decorrente de todas as transações fraudulentas.
     *
     * @return Soma total de todos os valores de fraude como BigDecimal.
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
     * 5. Agrupa e contabiliza a quantidade de ocorrências de fraudes para cada tipo de transação (ex: CASH_OUT, TRANSFER).
     *
     * @return Mapa associando cada TransactionType à contagem de fraudes ocorridas (Map<TransactionType, Long>).
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
