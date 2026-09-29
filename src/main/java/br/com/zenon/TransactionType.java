package br.com.zenon;

/**
 * Enumeração (Enum) que define os tipos válidos de operações financeiras no sistema.
 * 
 * Enums no Java são tipos especiais de classes que representam um conjunto finito,
 * imutável e seguro de constantes nomeadas, prevenindo o uso de "strings mágicas".
 */
public enum TransactionType {
    // CASH_IN: Depósito ou entrada de dinheiro na conta
    CASH_IN,

    // CASH_OUT: Saque ou retirada de dinheiro da conta
    CASH_OUT,

    // DEBIT: Operação de débito direto
    DEBIT,

    // PAYMENT: Pagamento de compras ou boletos para comerciantes
    PAYMENT,

    // TRANSFER: Transferência bancária entre contas de clientes
    TRANSFER;

    /**
     * Método fábrica estático (Static Factory Method) para conversão segura de String para TransactionType.
     * 
     * Converte o texto recebido de arquivos CSV/APIs para o enum correspondente,
     * aplicando tratamento de erros amigável caso a entrada seja nula, vazia ou inválida.
     *
     * @param value Texto bruto extraído do arquivo CSV (ex: "PAYMENT", "TRANSFER").
     * @return A constante correspondente do enum TransactionType.
     * @throws IllegalArgumentException Se o valor for nulo, vazio ou não for um enum válido.
     */
    public static TransactionType fromString(String value) {
        // Validação preventiva contra valores nulos ou compostos apenas por espaços
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("O tipo de transação não pode ser nulo nem vazio.");
        }

        try {
            // trim(): remove eventuais espaços antes e depois do texto
            // toUpperCase(): padroniza para letras maiúsculas, tornando o parse case-insensitive
            // valueOf(): método padrão do Java que busca a constante pelo nome exato
            return TransactionType.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            // Se o valor não existir no Enum (ex: "COMPRA_INVALIDA"), lançamos uma exceção clara em português
            throw new IllegalArgumentException("Tipo de transação inválido: " + value);
        }
    }
}
