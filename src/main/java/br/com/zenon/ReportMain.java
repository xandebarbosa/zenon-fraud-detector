package br.com.zenon;

// Importa a classe record Statistics interna de TransactionReport,
// que atua como Value Object / DTO para consolidar as métricas calculadas (transações, fraudes e valor total).
import br.com.zenon.TransactionReport.Statistics;

// Importa DecimalFormat e NumberFormat do pacote java.text.
// Estas classes são o padrão do ecossistema Java para Internacionalização (i18n) e Localização (l10n) de números.
// Permitem formatar números e valores monetários respeitando convenções culturais (separadores de milhar, decimal e símbolos).
import java.text.DecimalFormat;
import java.text.NumberFormat;

// Importa Currency do pacote java.util para manipulação de moedas padronizadas pela ISO 4217.
// Permite especificar ou sobrepor a unidade monetária (ex: "USD", "BRL", "EUR") independentemente da região cultural.
import java.util.Currency;

// Importa Locale do pacote java.util, que encapsula uma região geográfica, política ou cultural (ex: Brasil, EUA).
// É o elemento central que orienta todos os componentes dependentes de localização (formatadores de data, hora, números e bundles).
import java.util.Locale;

// Importa ResourceBundle do pacote java.util, mecanismo nativo do Java para carregar textos internacionalizados.
// Lê arquivos '.properties' específicos por idioma e país (ex: report_pt.properties, report_en.properties), desacoplando textos do código.
import java.util.ResourceBundle;

/**
 * Ponto de entrada (Main Class) para teste, validação e exibição internacionalizada do processamento massivo de dados (Big Data).
 *
 * <p><b>Propósito da Classe:</b></p>
 * Demonstrar a orquestração ponta a ponta do detector de fraudes:
 * <ol>
 *   <li>Receber parâmetros de inicialização do usuário (como o idioma preferido via linha de comando);</li>
 *   <li>Configurar o ambiente de Internacionalização (i18n) com {@link java.util.Locale} e {@link java.util.ResourceBundle};</li>
 *   <li>Disparar o processamento em fluxo preguiçoso (lazy stream) de um arquivo de ~493 MB sem sobrecarregar a memória;</li>
 *   <li>Formatar métricas numéricas e financeiras segundo as convenções culturais da localidade ativa;</li>
 *   <li>Apresentar o relatório traduzido no console e registrar métricas precisas de tempo de processamento.</li>
 * </ol>
 *
 * <p><b>Responsabilidade Arquitetural:</b></p>
 * Atua como camada de apresentação / ponto de partida executável (Console CLI Application).
 * Não implementa regras de negócio diretamente; sua função é integrar o serviço de domínio {@link TransactionReport}
 * com a interface de saída (terminal) e com os recursos de localização.
 *
 * <p><b>Cenário de Execução e Memória Restrita:</b></p>
 * O arquivo PaySim analisado possui quase 500 MB e milhões de transações simuladas.
 * Sob limites estritos de heap (como {@code -Xmx128M}), a leitura ingênua geraria um {@link OutOfMemoryError}.
 * A combinação de Streams Lazy com agregação em tempo de leitura permite concluir o processamento com pico ínfimo de memória.
 */
public class ReportMain {

    /**
     * Método principal (entry point) que inicializa e executa a aplicação de geração de relatórios.
     *
     * <p><b>O que este método faz:</b></p>
     * 1. Determina o idioma da aplicação com base nos argumentos de execução (ou adota "pt" como padrão).<br>
     * 2. Inicializa instâncias de formatação cultural para números inteiros ({@link NumberFormat}) e moedas ({@link DecimalFormat}).<br>
     * 3. Carrega o pacote de mensagens traduzidas através de {@link ResourceBundle}.<br>
     * 4. Dispara o processamento do arquivo PaySim via {@link TransactionReport#generateReport(String)}.<br>
     * 5. Formata os valores calculados e busca os rótulos textuais traduzidos.<br>
     * 6. Imprime o relatório final usando blocos de texto modernos e calcula o tempo total decorrido.
     *
     * <p><b>Parâmetros:</b></p>
     * @param args Array de strings contendo argumentos passados na linha de comando.
     *             Opcionalmente, o primeiro elemento (args[0]) define a sigla do idioma desejado
     *             (ex: "pt" para Português, "en" para Inglês).
     *
     * <p><b>Retorno:</b></p>
     * {@code void} - Este método não retorna nenhum valor; os resultados são exibidos diretamente no console.
     *
     * <p><b>Por que este método existe:</b></p>
     * É o ponto de entrada padrão exigido pela Máquina Virtual Java (JVM) para iniciar a execução do programa a partir do terminal.
     *
     * <p><b>Recursos de Java Moderno Aplicados:</b></p>
     * <ul>
     *   <li>{@link Locale#of(String)}: Factory method moderno (Java 19+) seguro e sem uso de construtores depreciados;</li>
     *   <li>Inferência de tipo local com {@code var} (Java 10+): Reduz o ruído sintático sem perder a segurança de tipagem estática;</li>
     *   <li>Blocos de texto multi-linha (Text Blocks: {@code """..."""}) introduzidos no Java 15;</li>
     *   <li>Interpolação de Strings simplificada via {@link String#formatted(Object...)};</li>
     *   <li>Medição temporal de alta precisão com relógio monotônico via {@link System#nanoTime()};</li>
     *   <li>Saída limpa de terminal através da classe utilitária {@link IO#println(String)}.</li>
     * </ul>
     */
    static void main(String[] args) {
        // =========================================================================
        // SEÇÃO 1: CONFIGURAÇÃO DE INTERNACIONALIZAÇÃO (i18n) E LOCALIZAÇÃO (l10n)
        // =========================================================================

        // language: Define o código de idioma (ISO 639) a ser utilizado na aplicação.
        // Tipo de dado: String
        // Lógica da Operação Ternária:
        // - (args.length > 0): Avalia se o usuário forneceu ao menos um argumento via terminal.
        // - Se verdadeiro: Utiliza o primeiro argumento (args[0]), permitindo executar "java ReportMain en".
        // - Se falso: Adota "pt" (Português) como idioma padrão da aplicação.
        // Por que usar o ternário: Evita a exceção ArrayIndexOutOfBoundsException que ocorreria caso
        // tentássemos acessar args[0] em um array vazio, fornecendo simultaneamente flexibilidade e um valor default seguro.
        String language = (args.length > 0 ? args[0] : "pt");

        // locale: Instância que encapsula as regras culturais e linguísticas da região escolhida.
        // Tipo de dado: java.util.Locale (inferido pelo compilador com 'var').
        // Método Locale.of(language):
        // Introduzido no Java 19 como método estático de fábrica (Factory Method).
        // Foi escolhido porque substitui os construtores tradicionais 'new Locale(...)', que foram
        // formalmente depreciados nas versões recentes do Java devido a limitações de validação sintática (IETF BCP 47).
        var locale = Locale.of(language);

        // integerInstance: Formatador numérico configurado especificamente para números inteiros (sem casas decimais).
        // Tipo de dado: java.text.NumberFormat (inferido com 'var').
        // Método NumberFormat.getIntegerInstance(locale):
        // Retorna uma instância ajustada ao Locale especificado. Por exemplo:
        // - Em "pt" (Brasil/Portugal): Utiliza ponto como agrupador de milhar (ex: 1.000.000).
        // - Em "en" (EUA/Reino Unido): Utiliza vírgula como agrupador de milhar (ex: 1,000,000).
        // Foi escolhido para garantir que contagens de registros e fraudes sejam legíveis e culturalmente corretas.
        var integerInstance = NumberFormat.getIntegerInstance(locale);

        // currencyFormatter: Formatador especializado na representação visual de quantias financeiras/monetárias.
        // Tipo de dado: java.text.NumberFormat (subtipo java.text.DecimalFormat, inferido com 'var').
        // Método DecimalFormat.getCurrencyInstance(locale):
        // Aplica automaticamente:
        // 1. O símbolo da moeda associado ao Locale (ex: "R$" para pt_BR, "$" para en_US);
        // 2. A quantidade padrão de dígitos fracionários daquela moeda (duas casas decimais);
        // 3. Os separadores de milhar e decimal pertinentes (ex: R$ 1.234,56 vs $1,234.56).
        var currencyFormatter = DecimalFormat.getCurrencyInstance(locale);

        // currencyFormatter.setCurrency(Currency.getInstance("USD")):
        // Linha didática comentada: Demonstra como seria possível desacoplar a formatação linguística da moeda física.
        // Por padrão, getCurrencyInstance deriva a moeda a partir do país do Locale. Se quiséssemos exibir os valores
        // sempre em Dólares Americanos ("USD") conforme a norma ISO 4217, mesmo com a interface traduzida em Português,
        // bastaria invocar este método.
        //currencyFormatter.setCurrency(Currency.getInstance("USD"));

        // resourceBundle: Repositório que gerencia as mensagens traduzidas da aplicação.
        // Tipo de dado: java.util.ResourceBundle (inferido com 'var').
        // Método ResourceBundle.getBundle("report", locale):
        // Carrega o arquivo '.properties' correspondente ao nome base "report" e ao Locale selecionado.
        // O mecanismo de resolução do Java procura os arquivos no classpath na seguinte ordem de fallback:
        // 1. report_<language>_<country>.properties (ex: report_pt_BR.properties)
        // 2. report_<language>.properties (ex: report_pt.properties ou report_en.properties)
        // 3. report.properties (arquivo padrão base de contingência)
        // Por que foi escolhido: Desacopla 100% os textos do código-fonte compilado, viabilizando adicionar
        // novos idiomas sem necessidade de recompilar as classes do sistema.
        var resourceBundle = ResourceBundle.getBundle("report", locale);

        // =========================================================================
        // SEÇÃO 2: EXECUÇÃO DO PROCESSAMENTO EM STREAMING (LAZY BIG DATA)
        // =========================================================================

        // Exibe banners informativos detalhando as características do teste de estresse
        IO.println("=========================================================");
        IO.println("Iniciando processamento massivo (Lazy) de 493MB...");
        IO.println("Monitorando uso da JVM (Max Heap: 128MB)");
        IO.println("=======================================================");

        // transactionReport: Instância do serviço de relatório responsável pelo pipeline lazy
        // Tipo de dado: br.com.zenon.TransactionReport (inferido com 'var')
        // Responsabilidade: Contém a lógica de processamento de fluxo que lê o CSV linha a linha sob demanda.
        var transactionReport = new TransactionReport();

        // arquivoPaySim: Caminho absoluto para o arquivo CSV de teste massivo com transações PaySim.
        // Tipo de dado: String
        // Propósito: Fornece o arquivo físico em disco a ser consumido pelo motor de relatório.
        String arquivoPaySim = "/home/alexandre/Projetos-Pratica-UNIPDS/zenon-fraud-detector/data/PS_20174392719_1491204439457_log.csv";

        // startTime: Marca o instante exato de início do processamento do arquivo.
        // Tipo de dado: long (inteiro de 64 bits)
        // Método System.nanoTime():
        // Retorna o valor atual do relógio monotônico da CPU da JVM com resolução em nanossegundos.
        // Por que foi escolhido: Ao contrário de System.currentTimeMillis() (relógio de parede), nanoTime()
        // não está sujeito a saltos temporais causados por ajustes manuais de relógio ou sincronização NTP,
        // sendo a API padrão recomendada pelo Java para medição de intervalos de tempo (benchmark/elapsed time).
        long startTime = System.nanoTime();

        // statistics: Objeto que armazena o resultado consolidado após o consumo integral do CSV.
        // Tipo de dado: br.com.zenon.TransactionReport.Statistics (Record imutável)
        // Propósito: Contém os três totais agregados:
        // - totalTransactions(): contagem de linhas válidas processadas;
        // - totalFrauds(): contagem de transações identificadas como fraudulentas (isFraud == 1);
        // - totalAmount(): somatório de todos os valores transacionados (BigDecimal com alta precisão monetária).
        Statistics statistics = transactionReport.generateReport(arquivoPaySim);

        // =========================================================================
        // SEÇÃO 3: FORMATAÇÃO CULTURAL E LOCALIZAÇÃO DOS RESULTADOS
        // =========================================================================

        // fmtTotalTransactions: Total de transações formatado culturalmente como texto.
        // Tipo de dado: String
        // Aplica o formatador de inteiros 'integerInstance' sobre o valor primitivo long 'statistics.totalTransactions()'.
        String fmtTotalTransactions = integerInstance.format(statistics.totalTransactions());

        // fmtTotalFrauds: Total de fraudes formatado culturalmente como texto.
        // Tipo de dado: String
        // Transforma o número de fraudes detectadas em uma representação com separadores de milhar apropriados.
        String fmtTotalFrauds = integerInstance.format(statistics.totalFrauds());

        // fmtTotalAmount: Valor financeiro total formatado como representação monetária.
        // Tipo de dado: String
        // Aplica o formatador 'currencyFormatter' sobre o BigDecimal 'statistics.totalAmount()',
        // incluindo o símbolo monetário da região e a formatação decimal correta.
        String fmtTotalAmount = currencyFormatter.format(statistics.totalAmount());

        // msgTotalTransactions: Rótulo textual traduzido para o total de transações.
        // Tipo de dado: String
        // Busca a chave 'label.total.transactions' no ResourceBundle correspondente ao idioma ativo.
        String msgTotalTransactions = resourceBundle.getString("label.total.transactions");

        // msgTotalFrauds: Rótulo textual traduzido para o total de fraudes.
        // Tipo de dado: String
        // Busca a chave 'label.total.frauds' no ResourceBundle correspondente ao idioma ativo.
        String msgTotalFrauds = resourceBundle.getString("label.total.frauds");

        // msgTotalAmount: Rótulo textual traduzido para o valor financeiro total.
        // Tipo de dado: String
        // Busca a chave 'label.total.amount' no ResourceBundle correspondente ao idioma ativo.
        String msgTotalAmount = resourceBundle.getString("label.total.amount");

        // =========================================================================
        // SEÇÃO 4: EXIBIÇÃO NO CONSOLE E MÉTRICAS DE DESEMPENHO
        // =========================================================================

        // Text Block (Java 15+): Bloco de texto multi-linha delimitado por três aspas (""").
        // Preserva a formatação visual e quebras de linha naturais sem poluição com caracteres de escape '\n' repetidos.
        // O método .formatted(...) substitui os marcadores de formato sequencialmente:
        // - Cada par '%s: %s' recebe um rótulo traduzido (ex: msgTotalTransactions) e seu valor numérico formatado (ex: fmtTotalTransactions).
        // Por que usar '%s' em vez de '%d' ou '%.2f': Como os números já foram convertidos para Strings formatadas
        // pelos formatadores NumberFormat e DecimalFormat respeitando as convenções locais, eles são interpolados como strings puras.
        IO.println("""
                %s: %s
                %s: %s
                %s: %s
                """.formatted(
                        msgTotalTransactions, fmtTotalTransactions,
                        msgTotalFrauds, fmtTotalFrauds,
                        msgTotalAmount, fmtTotalAmount
                )
        );

        // endTime: Marca o instante final do processamento em nanossegundos logo após o término do cálculo e exibição.
        // Tipo de dado: long
        long endTime = System.nanoTime();

        // tempoEmSegundos: Diferença de tempo convertida de nanossegundos para segundos.
        // Tipo de dado: double (número de ponto flutuante de dupla precisão de 64 bits)
        // Passo a passo do cálculo:
        // 1. (endTime - startTime): Subtrai o instante inicial do final, obtendo o tempo total decorrido em nanossegundos (10^-9 segundos).
        // 2. Divisão por 1000000000.0: O sufixo decimal '.0' garante que o divisor seja interpretado como double,
        //    forçando a promoção aritmética da operação para ponto flutuante e evitando truncamento por divisão inteira.
        double tempoEmSegundos = (endTime - startTime) / 1000000000.0;

        // printf: Exibe o tempo total formatado no terminal com 2 casas decimais (%.2f).
        // '%n' é o especificador de quebra de linha portátil do Java, gerando '\n' no Linux/macOS e '\r\n' no Windows.
        System.out.printf("\nTempo de processamento: %.2f segundos%n", tempoEmSegundos);
        System.out.println("=========================================================");
    }
}
