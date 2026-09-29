package br.com.zenon;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Record que representa um cliente/conta participante de uma transação financeira (seja origem ou destino).
 * 
 * No Java moderno (Java 16+), um 'record' é uma estrutura de dados imutável voltada para transporte de dados (DTO).
 * O compilador gera automaticamente para nós:
 * - Campos privados e finais (private final) para cada componente declarado no cabeçalho;
 * - Métodos de acesso (getters) com o mesmo nome do componente (ex: cliente.name());
 * - Implementações consistentes dos métodos equals(), hashCode() e toString().
 *
 * @param name Identificador único do cliente ou comerciante (ex: "C123456789" para cliente, "M123456789" para comerciante).
 * @param oldBalance Saldo da conta antes da execução da transação (usamos BigDecimal para evitar erros de arredondamento financeiro).
 * @param newBalance Saldo da conta após a conclusão da transação.
 */
public record TransactionCustomer(
        String name,
        BigDecimal oldBalance,
        BigDecimal newBalance) {

    /**
     * Construtor Compacto (Compact Constructor), exclusivo de Records no Java.
     * 
     * Diferente de um construtor tradicional, o construtor compacto não repete a lista de parâmetros
     * e nem requer atribuições manuais como 'this.name = name'.
     * Sua principal função é validar e/ou normalizar os dados antes que o Java faça a atribuição
     * automática dos campos de forma segura e imutável.
     */
    public TransactionCustomer {

        // Objects.requireNonNull verifica se a referência passada é nula.
        // Se for null, lança imediatamente NullPointerException com a mensagem explicativa (prática de Fail-Fast).
        Objects.requireNonNull(name, "O parâmetro name não pode ser nulo.");
        Objects.requireNonNull(oldBalance, "O parâmetro oldBalance não pode ser nulo.");
        Objects.requireNonNull(newBalance, "O parâmetro newBalance não pode ser nulo.");

        // O método signum() do BigDecimal retorna o sinal numérico do valor:
        //  -1: se o número for negativo (< 0)
        //   0: se o número for exatamente zero (== 0)
        //   1: se o número for positivo (> 0)
        // Como um saldo bancário neste domínio não pode ser negativo, lançamos IllegalArgumentException.
        if (oldBalance.signum() < 0) {
            throw new IllegalArgumentException("O valor de oldBalance deve ser positivo ou zero: " + oldBalance);
        }

        // Validação idêntica para o novo saldo: não é permitido que newBalance seja negativo (< 0).
        if (newBalance.signum() < 0) {
            throw new IllegalArgumentException("O valor de newBalance deve ser positivo ou zero: " + newBalance);
        }

        // name.trim(): remove todos os espaços em branco no início e no final da String.
        // isEmpty(): verifica se o comprimento da String após o trim é zero ("").
        // Isso impede a criação de clientes com nome vazio ("") ou preenchido somente com espaços ("   ").
        if (name.trim().isEmpty()) {
            throw new IllegalArgumentException("O valor de name não pode ser vazio.");
        }
    }
}
