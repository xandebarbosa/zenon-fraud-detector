package br.com.zenon;

import java.util.Optional;

/**
 * Interface que define o contrato de repositório para acesso a dados de transações financeiras.
 *
 * <p><b>Padrão de Projeto - Repository Pattern:</b></p>
 * O padrão Repository atua como uma camada de abstração entre a camada de domínio/regras
 * de negócio e a camada de acesso/armazenamento a dados (seja em memória, banco de dados ou arquivos).
 * Ele encapsula a lógica necessária para recuperar dados, expondo uma interface orientada a coleções.
 *
 * <p><b>Princípio de Inversão de Dependência (SOLID - DIP):</b></p>
 * Ao definir esta interface, as classes que consomem operações de busca dependem desta abstração
 * ({@code TransactionRepository}) e não de implementações concretas específicas (como listas ou mapas).
 * Isso permite substituir a estratégia de armazenamento (ex: {@link TransactionListRepository} por
 * {@link TransactionMapRepository}) sem necessidade de alterar o código cliente.
 */
public interface TransactionRepository {

    /**
     * Localiza uma transação no repositório a partir do identificador do cliente emissor (origem).
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
}
