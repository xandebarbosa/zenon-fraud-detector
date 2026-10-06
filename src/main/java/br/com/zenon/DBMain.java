package br.com.zenon;

import java.math.BigDecimal;
import java.util.List;

/**
 * Classe executável (Driver Class) para validação e benchmark da persistência em banco de dados relacional.
 *
 * <p><b>Propósito e Responsabilidade:</b></p>
 * Atuar como ponto de entrada para testes de integração com o banco de dados MySQL.
 * Esta classe orquestra o ciclo completo de persistência de dados:
 * <ol>
 *   <li>Verificação de conectividade com a base de dados relacional;</li>
 *   <li>Instanciação do repositório baseado em SQL ({@link TransactionSQLRepository});</li>
 *   <li>Ingestão e parsing de transações a partir do dataset CSV real (PaySim) via {@link TransactionIngestor};</li>
 *   <li>Persistência em lote/iteração de cada transação diretamente no banco de dados via JDBC;</li>
 *   <li>Medição e exibição do tempo total decorrido durante o processo de carga;</li>
 *   <li>Consulta pontual de transações persistidas via {@link TransactionRepository#findByOriginName(String)}.</li>
 * </ol>
 *
 * <p><b>Integração com o Projeto:</b></p>
 * Complementa a classe {@link Main}. Enquanto a {@link Main} foca em estruturas de dados puramente
 * em memória (como {@link TransactionListRepository} e {@link TransactionMapRepository}) e na
 * análise de fraudes via Stream API, a classe {@link DBMain} valida a transição do sistema para uma
 * arquitetura persistente, demonstrando a intercambialidade provida pela interface {@link TransactionRepository}.
 */
public class DBMain {

    /**
     * Ponto de entrada da aplicação para teste do repositório SQL.
     *
     * <p><b>Conceito Educacional - Método Main Simplificado (Java 21+):</b></p>
     * Utiliza o recurso moderno de métodos main de instância sem modificador de acesso {@code public},
     * sem a palavra-chave {@code static} e sem o parâmetro opcional {@code String[] args},
     * reduzindo a verbosidade inicial em pequenos programas utilitários ou testes.
     */
    void main() {
        // ====================================================================================
        // ETAPA 1: TESTE DE CONECTIVIDADE COM O BANCO DE DADOS
        // ====================================================================================

        // Invoca a ConnectionFactory para abrir uma conexão de teste com o MySQL.
        // Se houver falha de rede, credenciais erradas ou banco offline, uma exceção será lançada imediatamente.
        ConnectionFactory.getConnection();
        IO.println("Conectado com o Banco de Dados criada! ");

        // ====================================================================================
        // ETAPA 2: INSTANCIAÇÃO DOS COMPONENTES DE PERSISTÊNCIA E INGESTÃO
        // ====================================================================================

        // repository: Variável que armazena a instância de TransactionSQLRepository.
        // O uso da palavra-chave 'var' (Local Variable Type Inference - Java 10+) infere automaticamente
        // o tipo da variável em tempo de compilação sem perder a segurança de tipos (type-safety).
        var repository = new TransactionSQLRepository();

        // transactionIngestor: Instância do leitor/parser responsável por abrir o CSV, validar colunas
        // e instanciar os Records de Transaction correspondentes.
        TransactionIngestor transactionIngestor = new TransactionIngestor();

        // ====================================================================================
        // ETAPA 3: INÍCIO DA MEDIÇÃO DE TEMPO E LEITURA DO ARQUIVO CSV
        // ====================================================================================

        // startTimeSQL: Variável do tipo primitivo 'long' que armazena a marcação de tempo inicial em nanossegundos.
        // Obtida através de System.nanoTime(), permitindo medir com alta precisão o tempo gasto na ingestão e inserção.
        long startTimeSQL = System.nanoTime();

        // transactions: Coleção List<Transaction> populada com os dados extraídos do arquivo CSV PaySim.
        // O método readTransactionsNew lê linha por linha e converte apenas os registros válidos.
        List<Transaction> transactions = transactionIngestor.readTransactionsNew(
                "/home/alexandre/Projetos-Pratica-UNIPDS/zenon-fraud-detector/data/PS_20174392719_1491204439457_log.csv"
        );
        // Exibe no console o número total de transações válidas lidas do arquivo CSV
        IO.println(transactions.size());

        // ====================================================================================
        // ETAPA 4: PERSISTÊNCIA DAS TRANSAÇÕES NO BANCO DE DADOS RELACIONAL
        // ====================================================================================

        IO.println("Iniciando adição das transações no Banco de Dados...");

        // Itera sobre cada transação da lista e envia para o método save do repositório SQL.
        // O uso de Method Reference (repository::save) é equivalente à expressão lambda:
        // transactions.forEach(transaction -> repository.save(transaction));
        transactions.forEach(repository::save);

        // ====================================================================================
        // ETAPA 5: FINALIZAÇÃO DO BENCHMARK E EXIBIÇÃO DO TEMPO DE PROCESSAMENTO
        // ====================================================================================

        // endTimeSQL: Marcação de tempo em nanossegundos logo após o término da gravação de todos os registros.
        long endTimeSQL = System.nanoTime();

        // Converte o delta de nanossegundos para milissegundos dividindo por 1_000_000.0 (ponto flutuante)
        IO.println("Tempo de busca - List (ms): " + (endTimeSQL - startTimeSQL) / 1_000_000.0 + " ms");

        // ====================================================================================
        // ETAPA 6: TESTE DE CONSULTA DE TRANSAÇÃO POR CLIENTE DE ORIGEM
        // ====================================================================================

        // Executa a busca parametrizada no banco pelo cliente "C1231006815".
        // O método findByOriginName retorna um Optional<Transaction>.
        // O método ifPresentOrElse (Java 9+) aceita duas ações:
        //   1. Consumer (IO::println): executado se a transação for encontrada no banco;
        //   2. Runnable (() -> ...): executado se o Optional estiver vazio (registro inexistente).
        repository.findByOriginName("C1231006815")
                .ifPresentOrElse(IO::println, () -> IO.println("Transação não encontrada para: C1000001 "));
    }
}
//Parei no video mininuto 31;05
