package br.com.zenon;

import br.com.zenon.TransactionReport.Statistics;

/**
 * Ponto de entrada (Main Class) para teste e validação de processamento massivo de dados (Big Data).
 *
 * <p><b>Propósito e Cenário de Execução:</b></p>
 * Esta classe demonstra a execução prática do processamento de um arquivo CSV de aproximadamente 493 MB
 * (contendo milhões de registros da simulação PaySim) sob limites intencionalmente reduzidos de memória
 * da JVM (exemplo: executando com a flag {@code -Xmx128M}, que limita o heap da JVM a apenas 128 MB).
 *
 * <p><b>Conceito Educacional - Processamento Reativo / Sob Demanda:</b></p>
 * Se o arquivo de 493 MB fosse lido convencionalmente em memória (com listas completas), o programa
 * abortaria com erro de {@link OutOfMemoryError}, já que os objetos criados ultrapassariam a capacidade do heap.
 * Graças ao pipeline com avaliação preguiçosa (Lazy Streams) em {@link TransactionReport}, a execução
 * é concluída com sucesso e consumo de memória quase imperceptível.
 */
public class ReportMain {

    /**
     * Método principal da aplicação de relatório.
     *
     * <p><b>Recursos de Java Moderno Aplicados:</b></p>
     * <ul>
     *   <li>Declaração simplificada de método principal sem necessidade de {@code public static void main(String[] args)};</li>
     *   <li>Uso de {@code IO.println(...)} para operações de saída de terminal limpas e diretas;</li>
     *   <li>Inferência de tipos com palavra-chave {@code var} para reduzir verbosidade local;</li>
     *   <li>Blocos de texto multi-linha (Text Blocks: {@code """..."""}) com interpolação via {@link String#formatted(Object...)};</li>
     *   <li>Medição temporal de alta precisão com relógio monotônico via {@link System#nanoTime()}.</li>
     * </ul>
     */
    static void main() {

        // Exibe banners informativos detalhando as características do teste de estresse
        IO.println("=========================================================");
        IO.println("Iniciando processamento massivo (Lazy) de 493MB...");
        IO.println("Monitorando uso da JVM (Max Heap: 128MB)");
        IO.println("=======================================================");

        // transactionReport: Instância do serviço de relatório responsável pelo pipeline lazy
        var transactionReport = new TransactionReport();

        // arquivoPaySim: Caminho completo para o arquivo CSV volumoso de log do PaySim
        // Tipo: String
        String arquivoPaySim = "/home/alexandre/Projetos-Pratica-UNIPDS/zenon-fraud-detector/data/PS_20174392719_1491204439457_log.csv";

        // startTime: Marca o instante inicial da execução em nanossegundos utilizando o relógio monotônico da JVM.
        // nanoTime() é a API indicada para medição de intervalos de tempo decorrido (elapsed time).
        // Tipo de dado: long (inteiro de 64 bits)
        long startTime = System.nanoTime();

        // statistics: Objeto do tipo TransactionReport.Statistics que acumula os resultados calculados
        // durante o streaming de todo o arquivo CSV
        Statistics statistics = transactionReport.generateReport(arquivoPaySim);

        // Text Block (Java 15+): Bloco de texto delimitado por três aspas ("""), preservando quebras de linha.
        // O método .formatted(...) substitui os marcadores:
        //  - %d: número inteiro (long)
        //  - %.2f: número decimal (BigDecimal/double) formatado com duas casas decimais
        IO.println("""
                Total de linhas: %d
                Total de fraudes: %d
                Valor total transacionado: %.2f
                """.formatted(statistics.totalTransactions(), statistics.totalFrauds(), statistics.totalAmount()));

        // endTime: Marca o instante final do processamento em nanossegundos
        // Tipo de dado: long
        long endTime = System.nanoTime();

        // tempoEmSegundos: Diferença de tempo convertida de nanossegundos para segundos.
        // A divisão por 1_000_000_000.0 (literal de ponto flutuante em double) assegura precisão decimal no cálculo.
        // Tipo de dado: double (número de ponto flutuante de dupla precisão)
        double tempoEmSegundos = (endTime - startTime) / 1000000000.0;

        // printf: Exibe o tempo total com 2 casas decimais.
        // %n é o formatador portátil de quebra de linha (newline independente de SO: \n no Linux, \r\n no Windows).
        System.out.printf("\nTempo de processamento: %.2f segundos%n", tempoEmSegundos);
        System.out.println("=========================================================");
    }
}
