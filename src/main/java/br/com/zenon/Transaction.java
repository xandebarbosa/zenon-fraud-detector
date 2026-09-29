package br.com.zenon;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Record que representa uma transação financeira completa no sistema antifraude Zenon.
 * 
 * Modela os eventos do dataset PaySim, contendo informações temporais, tipo de operação,
 * valor transferido, dados das contas envolvidas (origem e destino) e marcações de suspeita de fraude.
 *
 * @param step Etapa temporal da simulação (cada 'step' equivale a 1 hora de atividade no mundo real).
 * @param type Categoria da operação financeira (representada pelas constantes do enum TransactionType).
 * @param amount Quantia financeira movimentada nesta transação.
 * @param transactionCustomerOrigin Instância de TransactionCustomer com os dados do cliente emissor (origem).
 * @param transactionCustomerRecipient Instância de TransactionCustomer com os dados do cliente recebedor (destino).
 * @param isFraud Booleano indicando se a transação foi confirmada como fraude real no histórico.
 * @param isFlaggedFraud Booleano sinalizado pelo sistema de regras de negócio internas (ex: operações acima de R$ 200.000).
 */
public record Transaction(
        int step,
        TransactionType type,
        BigDecimal amount,
        TransactionCustomer transactionCustomerOrigin,
        TransactionCustomer transactionCustomerRecipient,
        boolean isFraud,
        boolean isFlaggedFraud
) {

    /**
     * Construtor Compacto (Compact Constructor) do Record Transaction.
     * 
     * Executado automaticamente antes da atribuição dos campos.
     * Garante a integridade dos dados, rejeitando valores nulos ou estados inválidos (invariantes de classe).
     */
    public Transaction {

        // Valida que nenhuma referência obrigatória seja nula, evitando NullPointerException posterior (Fail-Fast)
        Objects.requireNonNull(type, "O tipo de transação (type) não pode ser nulo.");
        Objects.requireNonNull(amount, "O valor da transação (amount) não pode ser nulo.");
        Objects.requireNonNull(transactionCustomerOrigin, "O cliente de origem não pode ser nulo.");
        Objects.requireNonNull(transactionCustomerRecipient, "O cliente de destino não pode ser nulo.");

        // O 'step' precisa representar um tempo válido no simulador, devendo ser estritamente maior que zero (> 0)
        if (step <= 0) {
            throw new IllegalArgumentException("O valor de step deve ser positivo: " + step);
        }

        // O valor financeiro da transação (amount) não pode ser negativo.
        // O método signum() retorna -1 se o BigDecimal for menor que zero.
        if (amount.signum() < 0) {
            throw new IllegalArgumentException("O valor de amount deve ser positivo: " + amount);
        }
    }
}
