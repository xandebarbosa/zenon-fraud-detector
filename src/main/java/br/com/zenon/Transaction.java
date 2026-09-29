package br.com.zenon;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Record que modela uma transação financeira completa dentro do sistema antifraude Zenon.
 *
 * <p><b>Contexto de Domínio - Dataset Sintético PaySim:</b></p>
 * O PaySim é um simulador baseado em agentes que replica o comportamento financeiro real de serviços
 * de dinheiro móvel (Mobile Money), gerando dados sintéticos para pesquisa em detecção de fraudes.
 * Cada instância desta classe representa um evento atômico de transferência monetária entre dois agentes.
 *
 * <p><b>Composição de Objetos:</b></p>
 * Esta classe aplica o princípio de composição de objetos da Orientação a Objetos, agrupando
 * entidades menores e especializadas ({@link TransactionCustomer} para remetente e destinatário,
 * e {@link TransactionType} para categorização da operação).
 *
 * @param step                         Unidade temporal da simulação. Cada unidade (passo) equivale a
 *                                     1 hora de relógio no mundo real (ex: totalizando 744 passos em 30 dias de simulação).
 *                                     Tipo primitivo: {@code int}.
 * @param type                         Categoria da operação financeira realizada (ex: {@link TransactionType#TRANSFER}).
 *                                     Tipo: {@link TransactionType}.
 * @param amount                       Quantia monetária movimentada na transação em moeda corrente.
 *                                     Tipo: {@link BigDecimal}.
 * @param transactionCustomerOrigin    Dados detalhados do cliente/conta emissora (origem), contendo identificador
 *                                     e saldos antes e depois do evento. Tipo: {@link TransactionCustomer}.
 * @param transactionCustomerRecipient Dados detalhados do cliente/conta recebedora (destino), contendo identificador
 *                                     e saldos antes e depois do evento. Tipo: {@link TransactionCustomer}.
 * @param isFraud                      Indicador de <i>Ground Truth</i> (verdade real histórica): {@code true} se
 *                                     a transação foi comprovadamente executada por agentes fraudulentos; {@code false} caso legítima.
 *                                     Tipo primitivo: {@code boolean}.
 * @param isFlaggedFraud               Sinalizador de alerta disparado por regras heurísticas do sistema de controle
 *                                     (no PaySim, sinaliza transferências únicas não autorizadas que excederam R$ 200.000).
 *                                     Tipo primitivo: {@code boolean}.
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
     * <p>Executado antes da inicialização definitiva dos campos imutáveis.
     * Garante o cumprimento das regras e invariantes de negócio do sistema antifraude,
     * impedindo a existência de objetos em estado inconsistente ou inválido na memória da JVM.</p>
     *
     * @throws NullPointerException     Se qualquer referência a objeto obrigatória for nula (Fail-Fast).
     * @throws IllegalArgumentException Se {@code step} for menor ou igual a zero, ou se {@code amount} for negativo.
     */
    public Transaction {

        // Validação Fail-Fast das referências obrigatórias para evitar NullPointerException tardio
        Objects.requireNonNull(type, "O tipo de transação (type) não pode ser nulo.");
        Objects.requireNonNull(amount, "O valor da transação (amount) não pode ser nulo.");
        Objects.requireNonNull(transactionCustomerOrigin, "O cliente de origem não pode ser nulo.");
        Objects.requireNonNull(transactionCustomerRecipient, "O cliente de destino não pode ser nulo.");

        // O 'step' representa a hora relativa na simulação e deve ser estritamente positivo (>= 1)
        if (step <= 0) {
            throw new IllegalArgumentException("O valor de step deve ser positivo: " + step);
        }

        // O montante financeiro da transação não pode ser negativo.
        // O método signum() do BigDecimal retorna -1 se o número for estritamente menor que zero.
        if (amount.signum() < 0) {
            throw new IllegalArgumentException("O valor de amount deve ser positivo: " + amount);
        }
    }
}
