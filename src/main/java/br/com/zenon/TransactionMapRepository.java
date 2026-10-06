package br.com.zenon;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implementação do repositório {@link TransactionRepository} baseada em tabela hash ({@link Map}).
 *
 * <p><b>Propósito e Responsabilidade:</b></p>
 * Fornecer armazenamento e recuperação de alta performance em memória para transações financeiras,
 * indexando cada transação pelo identificador do cliente de origem ({@code originName}).
 *
 * <p><b>Integração com o Projeto:</b></p>
 * Integra-se ao ecossistema do <i>Zenon Fraud Detector</i> implementando a interface comum {@link TransactionRepository}.
 * É utilizada tanto para cenários onde consultas instantâneas O(1) são críticas quanto no módulo de benchmark
 * (em {@link Main}), permitindo contrastar empiricamente seu tempo de resposta contra a busca linear O(N)
 * da classe {@link TransactionListRepository}.
 *
 * <p><b>Objetivo Educacional e Análise de Complexidade (Trade-off de Estruturas de Dados):</b></p>
 * Diferente da abordagem em lista sequencial ({@link TransactionListRepository}), esta classe cria
 * um índice em memória indexando cada transação pelo identificador do cliente de origem ({@code originName}).
 *
 * <ul>
 *   <li><b>Complexidade de Tempo (Construção / Indexação):</b> O(N) no momento da instanciação, pois precisa
 *       percorrer todos os N elementos da lista para montar o mapa hash.</li>
 *   <li><b>Complexidade de Tempo (Busca / Query):</b> O(1) em média (tempo constante), pois tabelas hash
 *       calculam o código de hash ({@link Object#hashCode()}) da chave e acessam diretamente o bucket
 *       de memória correspondente, dispensando a varredura linear de todos os registros.</li>
 *   <li><b>Complexidade de Espaço:</b> O(N), porém com consumo de memória significativamente maior
 *       que uma {@link List}, devido à sobrecarga interna dos nós (Node/Entry) da tabela hash e ponteiros.</li>
 * </ul>
 *
 * <p>Essa abordagem demonstra o clássico trade-off da ciência da computação:
 * <i>"gastar mais memória e tempo inicial de processamento para obter consultas praticamente instantâneas"</i>.</p>
 */
public class TransactionMapRepository implements TransactionRepository {

    /**
     * Tabela hash imutável que mapeia o nome do cliente de origem (Chave: {@link String})
     * para a sua respectiva transação financeira (Valor: {@link Transaction}).
     *
     * <p><b>Tipo de Dado:</b> {@link Map Map&lt;String, Transaction&gt;}.</p>
     * <p><b>Propósito:</b> Atua como o índice primário de busca rápida em memória do repositório.</p>
     * <p><b>Uso e Imutabilidade:</b> O modificador {@code final} garante que a referência do mapa não possa ser
     * reatribuída após a inicialização no construtor, promovendo imutabilidade referencial e consistência estrutural.</p>
     */
    private final Map<String, Transaction> transactionByOriginName;

    /**
     * Construtor da classe {@code TransactionMapRepository}.
     *
     * <p>Recebe a lista de transações e a transforma em um mapa hash indexado por {@code originName}.</p>
     *
     * <p><b>Detalhamento do Pipeline Funcional de Coleta:</b></p>
     * <ol>
     *   <li>{@code transactions.stream()}: Abre o fluxo sequencial sobre a lista fornecida.</li>
     *   <li>{@code Collectors.toMap(...)}: Coletor terminal que transforma a Stream em um {@link Map}.
     *       <ul>
     *         <li><b>Key Mapper:</b> {@code transaction -> transaction.transactionCustomerOrigin().name()}
     *             extrai o nome do cliente de origem para ser a chave única do mapa.</li>
     *         <li><b>Value Mapper:</b> {@link Function#identity()} é uma função canônica do Java funcional
     *             que simplesmente retorna o próprio elemento que recebeu (neste caso, a instância de {@link Transaction}).
     *             É equivalente a escrever {@code t -> t}, porém com maior clareza semântica e reutilização de instância.</li>
     *       </ul>
     *   </li>
     * </ol>
     *
     * @param transactions Coleção de transações a ser indexada. Não pode ser nula.
     * @throws NullPointerException Se o parâmetro {@code transactions} for nulo (Fail-Fast).
     */
    public TransactionMapRepository(List<Transaction> transactions) {
        // Validação defensiva: impede que referências nulas sejam processadas
        Objects.requireNonNull(transactions);

        // Converte a lista em um mapa utilizando a Stream API
        this.transactionByOriginName =
                transactions
                        .stream()
                        // Agrupa os dados onde: Chave = Nome do Cliente de Origem, Valor = Transação Completa
                        .collect(Collectors.toMap(
                                transaction -> transaction.transactionCustomerOrigin().name(),
                                Function.identity()));
    }

    /**
     * Recupera uma transação em tempo constante O(1) a partir do nome do cliente de origem.
     *
     * <p><b>Por que utilizar {@link Optional#ofNullable(Object)}?</b></p>
     * O método {@link Map#get(Object)} retorna o valor associado à chave ou {@code null} caso a chave
     * não esteja presente na tabela hash. O método utilitário {@link Optional#ofNullable(Object)} encapsula
     * esse retorno com elegância:
     * <ul>
     *   <li>Se {@code get()} retornar um objeto {@link Transaction}, produz {@code Optional.of(transaction)}.</li>
     *   <li>Se {@code get()} retornar {@code null}, produz {@link Optional#empty()}.</li>
     * </ul>
     * Isso elimina a necessidade de blocos condicionais {@code if-else} com verificações explícitas de {@code != null}.
     *
     * @param originName Nome do cliente de origem a ser consultado (ex: "C1868032458").
     * @return {@link Optional} contendo a {@link Transaction} localizada ou {@link Optional#empty()} se inexistente.
     */
    @Override
    public Optional<Transaction> findByOriginName(String originName) {
        // Consulta instantânea na tabela hash O(1) e empacotamento seguro em Optional
        return Optional.ofNullable(transactionByOriginName.get(originName));
    }

    /**
     * Armazena uma nova transação no mapa em memória indexada pelo identificador do cliente emissor.
     *
     * <p><b>Propósito do Método:</b></p>
     * Adicionar uma nova transação à estrutura hash sem sobrescrever dados pré-existentes de um mesmo cliente.
     *
     * <p><b>Por que utilizar {@link Map#putIfAbsent(Object, Object)}?</b></p>
     * Diferente do método tradicional {@link Map#put(Object, Object)}, que substitui incondicionalmente
     * qualquer valor associado à chave existente, o {@code putIfAbsent}:
     * <ul>
     *   <li>Verifica se a chave ({@code originName}) já está mapeada para algum valor não nulo na tabela hash;</li>
     *   <li>Caso <b>já exista</b>: preserva a transação original existente e ignora a nova inserção;</li>
     *   <li>Caso <b>não exista</b>: insere o novo par (chave, transação) na tabela hash.</li>
     * </ul>
     * Isso garante a idempotência da inserção e evita checagens manuais redundantes com blocos
     * condicionais como {@code if (!map.containsKey(key))}.
     *
     * <p><b>Lógica Passo a Passo:</b></p>
     * <ol>
     *   <li>{@code transaction.transactionCustomerOrigin().name()}: Extrai o identificador único do cliente
     *       emissor (ex: "C1231006815"), que servirá como chave de indexação hash.</li>
     *   <li>{@code transaction}: Instância completa da transação que será o valor associado à chave.</li>
     *   <li>{@code putIfAbsent}: Executa a inserção atômica na tabela hash sob a chave informada.</li>
     * </ol>
     *
     * @param transaction Objeto {@link Transaction} contendo os dados da transação a ser salva.
     */
    @Override
    public void save(Transaction transaction) {
        // Insere a transação na tabela hash apenas se o cliente de origem ainda não estiver cadastrado
        this.transactionByOriginName.putIfAbsent(transaction.transactionCustomerOrigin().name(), transaction);
    }
}
