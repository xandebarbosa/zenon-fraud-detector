package br.com.zenon;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Implementação do repositório {@link TransactionRepository} baseada em lista em memória ({@link List}).
 *
 * <p><b>Objetivo Educacional e Análise de Complexidade:</b></p>
 * Esta classe armazena as transações sequencialmente em uma lista ordenada. Para realizar buscas,
 * ela percorre os elementos um a um a partir do início da coleção (busca linear / varredura sequencial).
 * <ul>
 *   <li><b>Complexidade de Tempo (Busca):</b> O(N) no pior caso (onde N é o total de transações),
 *       pois caso o registro procurado esteja no final da lista ou não exista, todos os N elementos
 *       precisarão ser inspecionados.</li>
 *   <li><b>Complexidade de Espaço:</b> O(N), pois armazena apenas a referência aos elementos já existentes
 *       na lista fornecida, sem sobrecarga de tabelas hash ou estruturas de índices adicionais.</li>
 * </ul>
 *
 * <p>É utilizada no projeto para fins comparativos (benchmark) em relação à implementação
 * {@link TransactionMapRepository}, demonstrando na prática o impacto de diferentes estruturas de dados
 * no desempenho de consultas.</p>
 */
public class TransactionListRepository implements TransactionRepository {

    /*
     * Crie uma classe TransactionListRepository que define um método que busca uma transação por nome
     * do cliente de origem e retorna Optional<Transaction>, indicando que o valor pode não existir.
     * Teste a busca na Main com o arquivo PaySim completo / sem erros, com valores existentes (ex. C1231006815)
     * e não existentes (ex. C12345).
     * */

    /**
     * Coleção interna de transações mantida em formato de lista ({@link List}).
     * <p>Tipo: {@code List<Transaction>}. Armazena a sequência de transações na mesma ordem
     * em que foram ingeridas a partir da fonte de dados.</p>
     */
    private List<Transaction> transactions;

    /**
     * Construtor da classe {@code TransactionListRepository}.
     *
     * <p>Recebe a lista de transações a ser gerenciada pelo repositório e aplica validação defensiva.</p>
     *
     * @param transactions Coleção de transações que servirá como fonte de dados em memória.
     *                     Não pode ser nula (garantido por {@link Objects#requireNonNull(Object)}).
     * @throws NullPointerException Se a lista {@code transactions} informada for nula (padrão Fail-Fast).
     */
    public TransactionListRepository(List<Transaction> transactions) {
        // Validação Fail-Fast: interrompe a criação imediatamente se o parâmetro for nulo,
        // evitando comportamentos imprevisíveis durante futuras operações de busca.
        Objects.requireNonNull(transactions);
        this.transactions = transactions;
    }

    /**
     * Busca a primeira transação realizada pelo cliente de origem especificado utilizando a Stream API.
     *
     * <p><b>Lógica do Pipeline de Execução:</b></p>
     * <ol>
     *   <li>{@code transactions.stream()}: Inicializa uma sequência de elementos (Stream) a partir da lista interna.</li>
     *   <li>{@code .filter(transaction -> ...)}: Operação intermediária que recebe um predicado (função lambda).
     *       Navega no objeto {@link Transaction}, obtém o cliente de origem via {@link Transaction#transactionCustomerOrigin()},
     *       recupera o seu nome com {@link TransactionCustomer#name()} e compara com {@code originName} usando {@link String#equals(Object)}.
     *       Apenas as transações que correspondem exatamente ao nome informado passam pelo filtro.</li>
     *   <li>{@code .findFirst()}: Operação terminal de curto-circuito (short-circuiting). Retorna um {@link Optional}
     *       contendo o primeiro elemento correspondente encontrado e interrompe o restante da varredura na lista.
     *       Se nenhum elemento corresponder ao critério após examinar toda a lista, retorna {@link Optional#empty()}.</li>
     * </ol>
     *
     * @param originName Nome ou identificador do cliente emissor da transação (ex: "C1231006815").
     * @return {@link Optional} com a primeira {@link Transaction} encontrada para a origem informada,
     *         ou {@link Optional#empty()} se nenhuma transação com essa origem existir.
     */
    @Override
    public Optional<Transaction> findByOriginName(String originName) {
        return transactions
                .stream()
                // Avalia se o cliente de origem da transação é idêntico ao originName buscado
                .filter(transaction -> transaction.transactionCustomerOrigin().name().equals(originName))
                // Retorna o primeiro elemento correspondente encontrado ou Optional vazio
                .findFirst();
    }


}
