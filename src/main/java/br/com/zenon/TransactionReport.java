package br.com.zenon;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Classe responsável pela geração de relatórios consolidados sobre grandes volumes de transações.
 *
 * <p><b>Objetivo Educacional e Arquitetura - Processamento de Big Data em Memória Limitada:</b></p>
 * O desafio central desta classe é processar arquivos extremamente grandes (como o dataset PaySim
 * com quase 500 MB e milhões de linhas) sob restrições severas de memória heap da JVM (ex: {@code -Xmx128M}).
 *
 * <p><b>Por que NÃO utilizar {@link Files#readAllLines(Path)} aqui?</b></p>
 * {@link Files#readAllLines(Path)} tenta carregar o arquivo inteiro de uma só vez para dentro de
 * uma {@code List<String>} na memória Heap. Em arquivos de centenas de megabytes ou gigabytes, isso
 * resulta inevitavelmente em um erro fatal de estouro de memória: {@link OutOfMemoryError} (OOM).
 *
 * <p><b>A Solução com Avaliação Preguiçosa (Lazy Evaluation) via {@link Files#lines(Path)}:</b></p>
 * {@link Files#lines(Path)} lê o arquivo linha por linha sob demanda (streaming I/O com {@link java.io.BufferedReader}).
 * Cada linha é lida do disco, transformada, acumulada nas estatísticas e imediatamente descartada para ser
 * recolhida pelo Garbage Collector (GC). Dessa forma, a complexidade de espaço na memória torna-se O(1)
 * (constante), consumindo apenas poucos megabytes de memória, independentemente de o arquivo ter 500 MB ou 50 GB.
 */
public class TransactionReport {

    /**
     * Record interno e imutável que atua como uma projeção leve de dados (Lightweight DTO).
     *
     * <p><b>Otimização de Performance e Redução de Pressão no Garbage Collector:</b></p>
     * Para gerar o relatório consolidado, o sistema precisa apenas do valor da operação ({@code amount})
     * e da flag de fraude ({@code isFraud}). Criar a entidade completa {@link Transaction} exigiria
     * instanciar objetos adicionais como {@link TransactionCustomer}, fazer parses de strings complexas,
     * validar saldos e criar identificadores de clientes.
     * Ao projetar apenas os 2 campos estritamente necessários neste Record enxuto, reduz-se
     * drasticamente a alocação de objetos efêmeros na Heap, acelerando a execução.
     *
     * @param amount  Quantia monetária movimentada na transação. Tipo: {@link BigDecimal}.
     * @param isFraud Booleano indicando se a transação foi confirmada como fraudulenta. Tipo primitivo: {@code boolean}.
     */
    private record ReportTransaction(BigDecimal amount, boolean isFraud) {}

    /**
     * Record imutável que acumula e encapsula as métricas consolidadas apuradas no relatório.
     *
     * <p><b>Padrão Imutável de Acumulação:</b></p>
     * Cada operação de adição não altera os valores internos (já que Records são estritamente imutáveis),
     * mas produz uma nova instância de {@code Statistics} com os dados agregados, garantindo total
     * segurança para execuções concorrentes ou em paralelo (Thread Safety).
     *
     * @param totalTransactions Quantidade total de transações processadas com sucesso. Tipo: {@code long}.
     * @param totalFrauds       Quantidade total de transações confirmadas como fraude. Tipo: {@code long}.
     * @param totalAmount       Valor financeiro cumulativo de todas as transações processadas. Tipo: {@link BigDecimal}.
     */
    public record Statistics(long totalTransactions, long totalFrauds, BigDecimal totalAmount) {

        /**
         * Elemento neutro / de identidade da acumulação de estatísticas.
         * <p>Representa o estado inicial onde nenhuma linha foi processada:
         * 0 transações, 0 fraudes e R$ 0.00 acumulados.</p>
         * <p>Tipo: {@code Statistics}. Constante estática e privada ({@code private final static}).</p>
         */
        private final static Statistics ZERO = new Statistics(0, 0, BigDecimal.ZERO);

        /**
         * Método acumulador (Accumulator): incorpora os dados de uma única transação ({@link ReportTransaction})
         * ao acumulador de estatísticas atual.
         *
         * <p><b>Lógica de agregação passo a passo:</b></p>
         * <ul>
         *   <li>{@code totalTransactions + 1}: incrementa em 1 a contagem total de transações;</li>
         *   <li>{@code totalFrauds + (reportTransaction.isFraud ? 1 : 0)}: utiliza operador ternário
         *       para somar 1 caso a transação seja fraude, ou 0 caso legítima;</li>
         *   <li>{@code totalAmount.add(reportTransaction.amount)}: soma com precisão monetária exata
         *       o montante da transação ao total acumulado via {@link BigDecimal#add(BigDecimal)}.</li>
         * </ul>
         *
         * @param reportTransaction Dados da transação a ser incorporada.
         * @return Uma nova instância imutável de {@link Statistics} contendo os totais recalculados.
         */
        private Statistics addReportTransaction(ReportTransaction reportTransaction) {
            return new Statistics(
                    totalTransactions + 1,
                    totalFrauds + (reportTransaction.isFraud ? 1 : 0),
                    totalAmount.add(reportTransaction.amount)
            );
        }

        /**
         * Método combinador (Combiner): funde dois objetos {@link Statistics} parciais em um único resultado.
         *
         * <p><b>Por que este método existe?</b></p>
         * É exigido pela sobrecarga de 3 argumentos do método {@link Stream#reduce(Object, java.util.function.BiFunction, java.util.function.BinaryOperator)}.
         * Caso a Stream seja convertida para processamento paralelo ({@code .parallel()}), o Java divide
         * o processamento entre vários núcleos de CPU, gerando estatísticas parciais independentes em cada thread.
         * Este método define exatamente como combinar duas instâncias de {@code Statistics} geradas por threads diferentes.
         *
         * @param othersStatistics Segunda instância de estatísticas a ser combinada com a atual.
         * @return Nova instância de {@link Statistics} com a soma de ambas as métricas.
         */
        private Statistics addOuthersStatistics(Statistics othersStatistics) {
            return new Statistics(
                    totalTransactions + othersStatistics.totalTransactions,
                    totalFrauds + othersStatistics.totalFrauds,
                    totalAmount.add(othersStatistics.totalAmount));
        }
    }

    /**
     * Gera o relatório estatístico consolidado a partir do arquivo CSV indicado, processando-o
     * via fluxo preguiçoso (lazy stream) com consumo constante de memória.
     *
     * <p><b>Detalhamento do Pipeline de Execução:</b></p>
     * <ol>
     *   <li>{@link Path#of(String, String...)}: Fábrica estática introduzida no Java 11 que substitui
     *       {@code Paths.get(...)} para obter um caminho no sistema de arquivos.</li>
     *   <li>{@code try (Stream<String> lines = Files.lines(path))}: Bloco <i>try-with-resources</i>.
     *       <b>Crítico:</b> Streams retornadas por {@link Files#lines(Path)} mantêm um descritor de arquivo aberto.
     *       Como {@link Stream} implementa {@link AutoCloseable}, o bloco try-with-resources garante o
     *       fechamento seguro do arquivo após o processamento, prevenindo vazamentos de recursos no S.O.</li>
     *   <li>{@code .skip(1)}: Descarta a primeira linha (cabeçalho com os nomes das colunas).</li>
     *   <li>{@code .map(this::parseReportTransaction)}: Transforma cada linha de texto em um
     *       {@code Optional<ReportTransaction>}, isolando apenas os dados necessários.</li>
     *   <li>{@code .filter(Optional::isPresent)}: Descarta silenciosamente registros corrompidos ou com erro de formatação.</li>
     *   <li>{@code .map(Optional::get)}: Extrai o objeto {@link ReportTransaction} de dentro do {@link Optional}.</li>
     *   <li>{@code .reduce(...)}: Executa a redução final sobre o fluxo com 3 parâmetros:
     *       <ul>
     *         <li><b>Identidade:</b> {@code Statistics.ZERO} como estado inicial neutro;</li>
     *         <li><b>Acumulador (BiFunction):</b> {@code Statistics::addReportTransaction} que incorpora
     *             cada elemento {@code ReportTransaction} no acumulador {@code Statistics};</li>
     *         <li><b>Combinador (BinaryOperator):</b> {@code Statistics::addOuthersStatistics} que combina
     *             duas instâncias de {@code Statistics} (suporte a execução paralela).</li>
     *       </ul>
     *   </li>
     * </ol>
     *
     * @param fileName Caminho absoluto ou relativo do arquivo CSV a ser processado. Tipo: {@link String}.
     * @return Instância de {@link Statistics} consolidando o total de transações, fraudes e valor financeiro.
     * @throws RuntimeException Se ocorrer erro de I/O na abertura ou leitura do arquivo físico.
     */
    public Statistics generateReport(String fileName) {
        // Path.of(fileName): forma idiomática no Java moderno (11+) para criar instâncias de Path
        Path path = Path.of(fileName);

        // try-with-resources garante que o fluxo I/O de disco subjacente seja fechado com segurança
        try (Stream<String> lines = Files.lines(path)) {
            return lines
                    // Pula a linha do cabeçalho CSV
                    .skip(1)
                    // Converte a linha de texto para Optional<ReportTransaction>
                    .map(this::parseReportTransaction)
                    // Filtra apenas as linhas processadas com sucesso
                    .filter(Optional::isPresent)
                    // Desempacota o ReportTransaction de dentro do Optional
                    .map(Optional::get)
                    // Reduz todo o fluxo ao resultado estatístico consolidado
                    .reduce(
                            Statistics.ZERO,
                            Statistics::addReportTransaction,
                            Statistics::addOuthersStatistics
                    );

        } catch (IOException e) {
            // Caso ocorra falha de leitura ou arquivo inacessível, encapsula a exceção verificada em RuntimeException
            throw new RuntimeException("Erro ao ler o arquivo" + fileName, e);
        }
    }

    /**
     * Converte uma linha individual de texto CSV no registro enxuto {@link ReportTransaction}.
     *
     * <p><b>Estratégia Seletiva de Parsing:</b></p>
     * Em vez de processar todos os 11 campos da linha, este método examina e converte somente:
     * <ul>
     *   <li>{@code chunks[2]}: {@code amount} (quantia transacionada);</li>
     *   <li>{@code chunks[9]}: {@code isFraud} (indicador de fraude real: 1 para true, 0 para false);</li>
     *   <li>{@code chunks[10]}: {@code isFlaggedFraud} (sinalizador de heurística interna: 1 para true, 0 para false).</li>
     * </ul>
     *
     * <p><b>Tolerância a Falhas:</b></p>
     * Qualquer exceção resultante de dados inválidos (como formatos numéricos incompatíveis ou colunas faltantes)
     * é capturada pelo bloco {@code catch}, que imprime um alerta e retorna {@link Optional#empty()},
     * permitindo que o pipeline continue processando os milhões de registros restantes sem travar.
     *
     * @param line Linha bruta em texto contendo campos delimitados por vírgula. Tipo: {@link String}.
     * @return {@link Optional} contendo a {@link ReportTransaction} ou {@link Optional#empty()} se inválida.
     */
    private Optional<ReportTransaction> parseReportTransaction(String line) {
        try {
            // split(","): particiona a linha de texto delimitada por vírgulas em um array de Strings
            String[] chunks = line.split(",");

            // chunks[2]: validação prévia de campo vazio/nulo antes de converter para BigDecimal
            if (chunks[2] == null || chunks[2].trim().isEmpty()) {
                throw new IllegalArgumentException("O valor de amount não pode ser nulo nem vazio");
            }
            // Converte a String de montante financeiro para BigDecimal com precisão decimal exata
            BigDecimal amount = new BigDecimal(chunks[2]);

            // chunks[9] e chunks[10]: colunas booleanas representadas como inteiros no CSV (0 ou 1)
            int fraudValue = Integer.parseInt(chunks[9]);
            int flaggedFraudValue = Integer.parseInt(chunks[10]);

            // Conversão lógica dos inteiros para tipos booleanos primitivos
            boolean isFraud = fraudValue == 1;
            boolean isFlaggedFraud = flaggedFraudValue == 1;

            // Optional.of: encapsula a instância de ReportTransaction criada em um container Optional não-nulo
            return Optional.of(new ReportTransaction(amount, isFraud));

        } catch (Exception e) {
            // Captura qualquer exceção (IllegalArgumentException, NumberFormatException, ArrayIndexOutOfBoundsException, etc.)
            // Exibe mensagem no System.err para depuração sem interromper o fluxo da aplicação
            System.err.println("Error ao fazer parse: " + line + " | " + e);
            //e.printStackTrace(); // Imprime a pilha de execução (útil em testes e depuração)

            // Retorna um Optional vazio, permitindo que a Stream descarte esta linha inválida suavemente
            return Optional.empty();
        }
    }
}
