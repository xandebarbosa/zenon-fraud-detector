package br.com.zenon;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Classe responsável pela ingestão, extração e transformação (ETL) de arquivos de dados brutos no formato CSV.
 *
 * <p><b>Objetivo Educacional e Arquitetura de Software:</b></p>
 * Atua como a camada de entrada de dados (Data Ingestion Layer) da aplicação.
 * Seu propósito é ler arquivos externos contendo transações não estruturadas/semiestruturadas em texto puro,
 * aplicar higienização (data cleansing), validar integridade e transformar cada registro em instâncias ricas
 * do modelo de domínio ({@link Transaction}).
 *
 * <p>Esta classe também serve como um estudo comparativo entre dois paradigmas de I/O em Java:</p>
 * <ol>
 *   <li><b>Abordagem Moderna (Declarativa / Funcional):</b> {@link #readTransactionsNew(String)}
 *       utiliza <i>Java NIO.2</i> ({@code java.nio.file}) combinado com a <i>Stream API</i>,
 *       promovendo código expressivo, conciso e com encadeamento de transformações.</li>
 *   <li><b>Abordagem Clássica (Imperativa / Procedural):</b> {@link #readTransactionsOld(String)}
 *       utiliza fluxos tradicionais de I/O ({@link FileInputStream}, {@link Scanner}) e laços explícitos,
 *       ilustrando o gerenciamento manual de ponteiros, recursos e contadores.</li>
 * </ol>
 */
public class TransactionIngestor {

    /**
     * Limite máximo padrão de registros a serem processados na leitura de arquivos.
     *
     * <p><b>Conceito de Sintaxe:</b> O caractere underscore ({@code _}) em literais numéricos
     * (recurso introduzido no Java 7) serve exclusivamente para melhorar a legibilidade humana
     * de números grandes (neste caso, 100 mil registros), sem alterar o valor final compilado.</p>
     * <p>Tipo: {@code int}. Constante estática e pública ({@code public static final}).</p>
     */
    public static final int FRAUDE_LIMIT = 100_000;

    /**
     * Processa o arquivo CSV utilizando a abordagem moderna e funcional com Java NIO.2 e Stream API.
     *
     * <p><b>Detalhamento do Pipeline de Execução:</b></p>
     * <ol>
     *   <li>{@link Paths#get(String, String...)}: Cria um identificador abstrato {@link Path}
     *       que localiza o arquivo de forma independente do sistema operacional (Windows, Linux, macOS).</li>
     *   <li>{@link Files#readAllLines(Path)}: Lê todas as linhas do arquivo de uma só vez para uma {@code List<String>}.
     *       <i>Nota didática:</i> Para arquivos massivos de dezenas de gigabytes, preferir-se-ia {@link Files#lines(Path)},
     *       que realiza leitura preguiçosa (lazy evaluation) sob demanda via buffer de disco sem esgotar a memória heap.</li>
     *   <li>{@code .stream()}: Converte a lista de linhas em um fluxo de dados sequencial.</li>
     *   <li>{@code .skip(1)}: Pula o primeiro elemento do fluxo, correspondente à linha de cabeçalho
     *       com nomes de colunas ("step,type,amount,nameOrig,...").</li>
     *   <li>{@code .limit(FRAUDE_LIMIT)}: Trunca o fluxo no limite especificado (100.000 registros),
     *       evitando consumo excessivo de memória durante análises laboratoriais.</li>
     *   <li>{@code .map(this::parseTransaction)}: Aplica o método de conversão {@link #parseTransaction(String)}
     *       a cada linha de texto, gerando um {@code Optional<Transaction>}.</li>
     *   <li>{@code .filter(Optional::isPresent)}: Mantém no fluxo apenas os registros que foram parseados com sucesso,
     *       descartando automaticamente linhas defeituosas ou corrompidas.</li>
     *   <li>{@code .map(Optional::get)}: Desembrulha o objeto {@link Transaction} de dentro do {@link Optional}.</li>
     *   <li>{@code .toList()}: Operação terminal introduzida no Java 16 que coleta os elementos em uma lista
     *       não modificável (imutável), garantindo integridade referencial.</li>
     * </ol>
     *
     * @param fileName Caminho relativo ou absoluto do arquivo CSV a ser processado. Tipo: {@link String}.
     * @return Lista imutável contendo apenas as transações válidas extraídas com sucesso.
     * @throws RuntimeException Se ocorrerem erros de I/O de baixo nível (arquivo ausente, permissão negada, etc.).
     */
    public List<Transaction> readTransactionsNew(String fileName) {
        // Paths.get(fileName): obtém a representação abstrata do caminho no sistema de arquivos
        Path path = Paths.get(fileName);

        try {
            // Files.readAllLines(path): carrega as linhas de texto do arquivo em memória
            List<String> lines = Files.readAllLines(path);

            return lines.stream()
                    // skip(1): descarta a primeira linha (cabeçalho com nomes das colunas: step, type, amount, ...)
                    .skip(1)
                    // limit(FRAUDE_LIMIT): restringe o processamento ao teto máximo de registros
                    .limit(FRAUDE_LIMIT)
                    // map(this::parseTransaction): aplica o método parseTransaction a cada linha, retornando Optional<Transaction>
                    .map(this::parseTransaction)
                    // filter(Optional::isPresent): descarta os Optionals vazios (linhas que geraram erro no parse)
                    .filter(Optional::isPresent)
                    // map(Optional::get): extrai o objeto Transaction de dentro de cada Optional preenchido
                    .map(Optional::get)
                    // toList(): coleta todos os elementos em uma lista imutável (Java 16+)
                    .toList();
        } catch (Exception e) {
            // Caso ocorra erro de I/O na abertura ou leitura do arquivo físico, encapsula em RuntimeException (Unchecked)
            throw new RuntimeException("Erro ao ler o arquivo: " + fileName, e);
        }
    }

    /**
     * Processa o arquivo CSV utilizando a abordagem clássica e imperativa com {@link FileInputStream} e {@link Scanner}.
     *
     * <p><b>Objetivo Educacional:</b></p>
     * Permite comparar o estilo de programação estruturada/procedural com o modelo funcional contemporâneo.
     * Ilustra o uso manual de laços {@code while}, contadores de linha e desvios de fluxo com {@code continue} e {@code break}.
     *
     * <p><b>Padrão Try-With-Resources (Java 7+):</b></p>
     * A cláusula {@code try (FileInputStream fis = new FileInputStream(filename))} assegura que o descritor
     * de arquivo no sistema operacional seja liberado e fechado automaticamente ao término do bloco,
     * mesmo na ocorrência de exceções não esperadas, prevenindo vazamentos de recursos (Resource Leaks).
     *
     * @param filename Caminho do arquivo CSV a ser lido. Tipo: {@link String}.
     * @return Lista mutável contendo instâncias de {@code Optional<Transaction>}, preservando o histórico de sucessos e falhas.
     * @throws RuntimeException Se ocorrer erro ao abrir ou ler o arquivo físico.
     */
    public List<Optional<Transaction>> readTransactionsOld(String filename) {

        // Cria a lista mutável (ArrayList) que acumulará os resultados processados linha a linha
        List<Optional<Transaction>> transactions = new ArrayList<>();

        // try-with-resources: garante o fechamento automático do recurso (FileInputStream)
        try (FileInputStream fis = new FileInputStream(filename)) {
            // Scanner lê o fluxo de bytes do arquivo de forma sequencial utilizando quebras de linha
            Scanner scanner = new Scanner(fis);

            int lineCount = 0; // Variável primitiva de controle: contador sequencial de linhas processadas

            // Enquanto houver uma próxima linha disponível para leitura no buffer do arquivo
            while (scanner.hasNextLine()) {

                String line = scanner.nextLine(); // Obtém a linha de texto atual

                lineCount++; // Incrementa o contador de linhas

                // Pula a linha 1 correspondente ao cabeçalho das colunas do CSV
                if (lineCount == 1) {
                    continue; // Pula imediatamente para a próxima iteração do laço
                }

                // Interrompe a execução após ler 1000 linhas de dados (linha 1002 em diante)
                if (lineCount > 1001) {
                    break; // Sai prematuramente do laço de repetição
                }

                // Efetua a conversão da linha e adiciona o Optional retornado à lista acumuladora
                var transaction = parseTransaction(line);
                transactions.add(transaction);
            }

        } catch (Exception e) {
            // Trata falhas de acesso ao sistema de arquivos encapsulando-as
            throw new RuntimeException("Error reading the file: " + filename, e);
        }

        return transactions;
    }

    /**
     * Método auxiliar privado de transformação (Parse) responsável por converter uma linha bruta em texto CSV
     * em uma instância imutável de {@link Transaction}.
     *
     * <p><b>Estratégia de Resiliência e Tolerância a Falhas:</b></p>
     * Se os dados da linha atenderem a todas as validações de tipos e regras de negócio, o método retorna
     * {@code Optional.of(transaction)}. Se a linha contiver anomalias (dados corrompidos, campos vazios, tipos
     * incompatíveis, valores negativos), a exceção é interceptada e o método retorna {@link Optional#empty()}.
     * Isso impede que uma única linha defeituosa aborte o processamento de um lote inteiro de milhões de registros.
     *
     * <p><b>Mapeamento das Colunas do CSV PaySim:</b></p>
     * <ul>
     *   <li>chunks[0]: {@code step} (Hora da simulação - inteiro)</li>
     *   <li>chunks[1]: {@code type} (Tipo da operação - enum {@link TransactionType})</li>
     *   <li>chunks[2]: {@code amount} (Valor movimentado - {@link BigDecimal})</li>
     *   <li>chunks[3]: {@code nameOrig} (Identificador do emissor)</li>
     *   <li>chunks[4]: {@code oldbalanceOrg} (Saldo de origem anterior)</li>
     *   <li>chunks[5]: {@code newbalanceOrig} (Saldo de origem posterior)</li>
     *   <li>chunks[6]: {@code nameDest} (Identificador do recebedor)</li>
     *   <li>chunks[7]: {@code oldbalanceDest} (Saldo de destino anterior)</li>
     *   <li>chunks[8]: {@code newbalanceDest} (Saldo de destino posterior)</li>
     *   <li>chunks[9]: {@code isFraud} (Flag de fraude confirmada: 1 para true, 0 para false)</li>
     *   <li>chunks[10]: {@code isFlaggedFraud} (Flag de fraude sinalizada pelo sistema: 1 para true, 0 para false)</li>
     * </ul>
     *
     * @param line Linha única contendo os valores separados por vírgula. Tipo: {@link String}.
     * @return {@link Optional} contendo a {@link Transaction} se os dados forem íntegros, ou {@link Optional#empty()} em caso de falha.
     */
    private Optional<Transaction> parseTransaction(String line) {
        try {
            // split(","): particiona a linha de texto delimitada por vírgulas em um array de Strings
            String[] chunks = line.split(",");

            // chunks[0]: número da hora/etapa (step) convertido de String para int primitivo
            int step = Integer.parseInt(chunks[0]);

            // chunks[1]: tipo da transação (ex: PAYMENT, TRANSFER) validado pelo método seguro do enum
            TransactionType type = TransactionType.fromString(chunks[1]);

            // chunks[2]: validação prévia de campo vazio/nulo antes de converter para BigDecimal
            if (chunks[2] == null || chunks[2].trim().isEmpty()) {
                throw new IllegalArgumentException("O valor de amount não pode ser nulo nem vazio");
            }
            BigDecimal amount = new BigDecimal(chunks[2]);

            // chunks[3], chunks[4], chunks[5]: identificador e saldos de origem (TransactionCustomer)
            var origin = new TransactionCustomer(chunks[3], new BigDecimal(chunks[4]), new BigDecimal(chunks[5]));

            // chunks[6], chunks[7], chunks[8]: identificador e saldos de destino (TransactionCustomer)
            var recipient = new TransactionCustomer(chunks[6], new BigDecimal(chunks[7]), new BigDecimal(chunks[8]));

            // chunks[9] e chunks[10]: colunas booleanas representadas como inteiros no CSV (0 ou 1)
            int fraudValue = Integer.parseInt(chunks[9]);
            int flaggedFraudValue = Integer.parseInt(chunks[10]);

            // Conversão lógica dos inteiros para tipos booleanos primitivos
            boolean isFraud = fraudValue == 1;
            boolean isFlaggedFraud = flaggedFraudValue == 1;

            // Optional.of: encapsula a instância Transaction criada em um container Optional não-nulo
            var transaction = Optional.of(new Transaction(step, type, amount, origin, recipient, isFraud, isFlaggedFraud));
            return transaction;
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
