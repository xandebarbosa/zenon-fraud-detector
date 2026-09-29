package br.com.zenon;

/**
 * Enumeração (Enum) que define os tipos válidos de operações financeiras no sistema antifraude.
 *
 * <p><b>Conceito Educacional - Enums no Java:</b></p>
 * Enums são tipos de dados especiais no Java que definem uma lista fixa, finita e pré-determinada
 * de constantes nomeadas. Cada constante do enum é tratada em tempo de compilação como uma instância
 * única (singleton) e imutável desse tipo.
 *
 * <p><b>Benefícios de Arquitetura:</b></p>
 * <ul>
 *   <li><b>Type-Safety (Segurança de Tipos):</b> Impede que strings arbitrárias ou incorretas (conhecidas
 *       como <i>"strings mágicas"</i>, como "PIX", "BOLETO_TESTE") sejam passadas inadvertidamente pelo sistema.
 *       O compilador detecta inconsistências em tempo de compilação.</li>
 *   <li><b>Refatoração e Manutenibilidade:</b> Alterações nos nomes das operações podem ser propagadas
 *       com segurança pela IDE por toda a base de código sem risco de quebras silenciosas.</li>
 *   <li><b>Otimização de Memória e Comparação:</b> Enums podem ser comparados com segurança usando o operador
 *       de identidade {@code ==} em vez de {@code equals()}, pois cada constante é uma instância única na JVM.</li>
 * </ul>
 *
 * <p><b>Contexto de Negócio (Dataset PaySim):</b></p>
 * No domínio de detecção de fraudes financeiras simulado pelo PaySim, fraudadores historicamente
 * utilizam combinações de {@code TRANSFER} (para desviar fundos de contas invadidas) seguidas
 * de {@code CASH_OUT} (para sacar dinheiro vivo e quebrar o rastreamento bancário).
 */
public enum TransactionType {

    /**
     * Depósito ou entrada de recursos em uma conta bancária.
     * Representa a injeção de capital (ex: depósito de dinheiro ou crédito externo).
     */
    CASH_IN,

    /**
     * Saque ou retirada de recursos em espécie da conta.
     * Vetor crítico frequentemente associado a etapas finais de lavagem e saque de fraudes.
     */
    CASH_OUT,

    /**
     * Operação de débito direto em conta corrente para liquidação imediata de obrigações.
     */
    DEBIT,

    /**
     * Pagamento convencional de compras e serviços para contas de comerciantes ou fornecedores.
     */
    PAYMENT,

    /**
     * Transferência eletrônica de valores entre contas bancárias de clientes.
     * Principal vetor de movimentação inicial em golpes e fraudes financeiras.
     */
    TRANSFER;

    /**
     * Método fábrica estático (Static Factory Method) para conversão segura e tolerante de String para TransactionType.
     *
     * <p><b>Por que utilizar uma Factory Method em vez de chamar {@code valueOf} diretamente?</b></p>
     * O método padrão {@link Enum#valueOf(Class, String)} do Java é estrito: se o texto fornecido
     * for nulo, contiver espaços em branco ou diferir no caso (maiúsculas/minúsculas), ele lança
     * {@link NullPointerException} ou {@link IllegalArgumentException} com mensagens genéricas.
     * Este método estático customizado encapsula:
     * <ol>
     *   <li>Validação prévia contra nulos ou strings vazias/em branco;</li>
     *   <li>Normalização com {@link String#trim()} (remoção de espaços) e {@link String#toUpperCase()} (insensibilidade a caixa);</li>
     *   <li>Captura e tradução de exceções com mensagens descritivas em português orientadas ao domínio.</li>
     * </ol>
     *
     * @param value Texto bruto extraído do arquivo CSV ou recebido de APIs (ex: "PAYMENT", " cash_out ").
     *              Tipo: {@link String}.
     * @return A constante correspondente do enum {@link TransactionType}.
     * @throws IllegalArgumentException Se a string informada for nula, em branco ou não corresponder a nenhum enum cadastrado.
     */
    public static TransactionType fromString(String value) {
        // Validação defensiva preliminar: verifica se o argumento é nulo ou se contém apenas espaços em branco
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("O tipo de transação não pode ser nulo nem vazio.");
        }

        try {
            // trim(): elimina espaços acidentais no início e no fim gerados na leitura do CSV
            // toUpperCase(): converte para letras maiúsculas para suportar "payment", "Payment" ou "PAYMENT"
            // valueOf(): busca a constante canônica do enum correspondente ao texto padronizado
            return TransactionType.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            // Se o texto não coincidir com nenhuma constante (ex: "BITCOIN", "DOC"), lançamos erro com contexto claro
            throw new IllegalArgumentException("Tipo de transação inválido: " + value);
        }
    }
}
