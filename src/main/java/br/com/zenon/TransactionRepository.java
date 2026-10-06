package br.com.zenon;

import java.util.Optional;

/**
 * Interface que define o contrato de repositório para acesso e persistência a dados de transações financeiras.
 *
 * <p><b>Propósito e Responsabilidade:</b></p>
 * Centralizar e padronizar todas as operações de acesso a dados (leitura e gravação) relativas à entidade
 * {@link Transaction}. Esta interface define as operações essenciais que qualquer mecanismo de armazenamento
 * deve prover, ocultando os detalhes de baixo nível de como e onde os dados são efetivamente guardados.
 *
 * <p><b>Integração com o Projeto:</b></p>
 * No ecossistema do <i>Zenon Fraud Detector</i>, esta interface funciona como o contrato comum compartilhado
 * entre as classes controladoras/orquestradoras (como {@link Main} e {@link DBMain}) e as múltiplas estratégias
 * de armazenamento:
 * <ul>
 *   <li>{@link TransactionListRepository}: Armazenamento volátil em lista sequencial em memória ({@link java.util.List});</li>
 *   <li>{@link TransactionMapRepository}: Armazenamento volátil indexado por chave em tabela hash ({@link java.util.Map});</li>
 *   <li>{@link TransactionSQLRepository}: Armazenamento persistente e transacional em banco de dados relacional MySQL via JDBC.</li>
 * </ul>
 *
 * <p><b>Padrão de Projeto - Repository Pattern:</b></p>
 * O padrão Repository atua como uma camada de mediação entre a camada de domínio/regras de negócio e a camada
 * de mapeamento/infraestrutura de dados. Ele simula uma coleção de objetos de domínio em memória, permitindo
 * que o código cliente realize operações de busca e persistência sem acoplamento a tecnologias de banco de dados
 * ou estruturas de dados específicas.
 *
 * <p><b>Princípio de Inversão de Dependência (SOLID - DIP):</b></p>
 * Ao definir esta interface, as classes que consomem operações de busca e salvamento dependem desta abstração
 * ({@code TransactionRepository}) e não de implementações concretas específicas.
 * Isso permite substituir a estratégia de armazenamento (por exemplo, migrar de um mapa em memória para um banco SQL)
 * sem a necessidade de alterar ou recompilar o código que consome os dados.
 */
public interface TransactionRepository {

    /**
     * Localiza uma transação no repositório a partir do identificador do cliente emissor (origem).
     *
     * <p><b>Propósito do Método:</b></p>
     * Realizar a consulta pontual de uma transação pelo nome/código do cliente emissor ({@code originName}),
     * permitindo verificar se determinado cliente já registrou movimentações financeiras no sistema.
     *
     * <p><b>Por que retornar {@link Optional}?</b></p>
     * O uso de {@code Optional<Transaction>} em vez de retornar diretamente {@code Transaction} ou {@code null}
     * expressa explicitamente no contrato da API que a transação procurada pode não existir no repositório.
     * Isso obriga o chamador a tratar conscientemente a ausência do valor (ex: via {@code isPresent()},
     * {@code ifPresentOrElse()}, {@code orElseThrow()}), prevenindo as temidas exceções do tipo
     * {@link NullPointerException}.
     *
     * @param originName Identificador único da conta/cliente de origem (ex: "C1231006815").
     *                   Tipo: {@link String}.
     * @return {@link Optional} contendo a {@link Transaction} se encontrada, ou {@link Optional#empty()}
     *         caso não exista nenhuma transação associada ao cliente informado.
     */
    Optional<Transaction> findByOriginName(String originName);

    /**
     * Persiste uma nova transação financeira no mecanismo de armazenamento subjacente.
     *
     * <p><b>Propósito do Método:</b></p>
     * Registrar e armazenar os dados de uma {@link Transaction}. Dependendo da implementação concreta
     * utilizada, a operação pode adicionar a transação a uma lista em memória, associá-la a uma chave
     * em um mapa hash ou executar uma instrução SQL {@code INSERT} em um banco de dados relacional.
     *
     * <p><b>Por que este método existe?</b></p>
     * Estabelece a capacidade de mutação/gravação essencial de um repositório (operação de Create/Save),
     * permitindo que novas transações ingeridas via arquivos (como logs CSV) ou geradas pelo sistema
     * sejam armazenadas de forma padronizada e polimórfica.
     *
     * @param transaction Objeto de domínio {@link Transaction} a ser persistido.
     *                    Contém os atributos da movimentação financeira (step, type, amount,
     *                    dados do cliente emissor, dados do cliente recebedor e sinalizadores de fraude).
     */
    void save(Transaction transaction);
}
