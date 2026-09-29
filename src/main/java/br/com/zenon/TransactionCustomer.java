package br.com.zenon;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Record que representa um participante (cliente ou comerciante) em uma transação financeira.
 *
 * <p><b>Conceito Educacional - Records no Java (Java 16+ / JEP 395):</b></p>
 * Records são tipos transparentes de portadores de dados imutáveis (Data Transfer Objects - DTOs).
 * Ao declarar um Record, o compilador Java gera automaticamente nos bastidores:
 * <ul>
 *   <li>Campos {@code private final} correspondentes a cada componente definido no cabeçalho;</li>
 *   <li>Métodos assessores de leitura públicos com o mesmo nome do componente (ex: {@link #name()}, {@link #oldBalance()});</li>
 *   <li>Implementação consistente de {@link Object#equals(Object)} baseada em valor de todos os campos;</li>
 *   <li>Implementação consistente de {@link Object#hashCode()} baseada em valor de todos os campos;</li>
 *   <li>Implementação legível de {@link Object#toString()} listando todos os atributos e valores;</li>
 *   <li>Um construtor canônico contendo todos os componentes.</li>
 * </ul>
 *
 * <p><b>Por que utilizar {@link BigDecimal} em aplicações financeiras?</b></p>
 * Tipos primitivos de ponto flutuante como {@code double} ou {@code float} utilizam a norma binária IEEE 754.
 * Essa representação binária não consegue representar frações decimais simples (como 0.1 ou 0.05) de maneira exata,
 * gerando erros acumulativos de arredondamento inaceitáveis no setor bancário. {@link BigDecimal}
 * opera com representação decimal de precisão arbitrária, garantindo cálculos monetários estritamente exatos.
 *
 * @param name       Identificador único da conta ou entidade participante.
 *                   No dataset PaySim, inicia com 'C' para cliente individual (ex: "C1231006815")
 *                   ou 'M' para comerciante (Merchant, ex: "M1979787155"). Tipo: {@link String}.
 * @param oldBalance Saldo da conta imediatamente antes da realização da transação. Tipo: {@link BigDecimal}.
 * @param newBalance Saldo da conta imediatamente após o processamento da transação. Tipo: {@link BigDecimal}.
 */
public record TransactionCustomer(
        String name,
        BigDecimal oldBalance,
        BigDecimal newBalance) {

    /**
     * Construtor Compacto (Compact Constructor), recurso exclusivo de Records no Java.
     *
     * <p><b>Diferencial do Construtor Compacto:</b></p>
     * No construtor compacto, os parâmetros não são redeclarados explicitamente e não há necessidade
     * de escrever comandos redundantes de atribuição (como {@code this.name = name;}).
     * O compilador insere o código deste bloco no início do construtor canônico e faz as atribuições
     * automaticamente ao final. Seu propósito principal é a validação de <b>invariantes de domínio</b>
     * e sanitização dos dados antes de criar a instância imutável.
     *
     * @throws NullPointerException     Se qualquer um dos parâmetros obrigatórios for nulo (Fail-Fast).
     * @throws IllegalArgumentException Se os saldos forem negativos ou o nome do cliente for vazio/em branco.
     */
    public TransactionCustomer {

        // Objects.requireNonNull verifica se a referência passada é nula.
        // Se for null, lança imediatamente NullPointerException com mensagem explicativa (Fail-Fast).
        Objects.requireNonNull(name, "O parâmetro name não pode ser nulo.");
        Objects.requireNonNull(oldBalance, "O parâmetro oldBalance não pode ser nulo.");
        Objects.requireNonNull(newBalance, "O parâmetro newBalance não pode ser nulo.");

        // O método signum() da classe BigDecimal retorna o sinal numérico do valor:
        //  -1: se o número for negativo (< 0)
        //   0: se o número for exatamente zero (== 0)
        //   1: se o número for positivo (> 0)
        // Regra de Negócio: saldos bancários no modelo deste dataset não podem ser negativos.
        if (oldBalance.signum() < 0) {
            throw new IllegalArgumentException("O valor de oldBalance deve ser positivo ou zero: " + oldBalance);
        }

        // Validação idêntica para o novo saldo: newBalance não pode ser negativo (< 0).
        if (newBalance.signum() < 0) {
            throw new IllegalArgumentException("O valor de newBalance deve ser positivo ou zero: " + newBalance);
        }

        // name.trim(): remove espaços em branco acidentais nas extremidades da string.
        // isEmpty(): retorna true se o comprimento resultante for zero ("").
        // Impede que nomes vazios ou compostos exclusivamente por espaços em branco ("   ") sejam aceitos.
        if (name.trim().isEmpty()) {
            throw new IllegalArgumentException("O valor de name não pode ser vazio.");
        }
    }
}
