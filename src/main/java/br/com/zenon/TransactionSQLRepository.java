package br.com.zenon;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

/**
 * Implementação do repositório {@link TransactionRepository} baseada em banco de dados relacional SQL via JDBC.
 *
 * <p><b>Propósito e Responsabilidade:</b></p>
 * Gerenciar a persistência duradoura e a recuperação de dados das transações financeiras utilizando
 * o padrão JDBC (<i>Java Database Connectivity</i>) diretamente contra uma base de dados relacional MySQL.
 * Esta classe é responsável por realizar a ponte entre o modelo orientado a objetos do sistema
 * (entidades {@link Transaction}, {@link TransactionCustomer} e o enum {@link TransactionType}) e o modelo
 * relacional (tabela {@code transactions} no esquema {@code zenon_frauds}).
 *
 * <p><b>Integração com o Projeto:</b></p>
 * Enquanto as classes {@link TransactionListRepository} e {@link TransactionMapRepository} operam exclusivamente
 * em memória volátil (perdendo os dados ao encerrar a JVM), esta classe integra o sistema a um banco de dados
 * externo, garantindo persistência duradoura (princípio de Durabilidade do ACID) e permitindo que o sistema
 * manipule volumes massivos de dados que excedem a capacidade de memória RAM disponível.
 *
 * <p><b>Conceitos Educacionais Aplicados:</b></p>
 * <ul>
 *   <li><b>Padrão de Repositório (Repository Pattern):</b> Encapsula todas as consultas SQL e mapeamentos relacionais,
 *       isolando a aplicação de detalhes de infraestrutura de banco de dados;</li>
 *   <li><b>Prevenção de SQL Injection:</b> Utilização rigorosa de {@link PreparedStatement} com consultas parametrizadas (placeholders {@code ?});</li>
 *   <li><b>Gerenciamento Seguro de Recursos:</b> Uso da estrutura {@code try-with-resources} (Java 7+) para fechar
 *       conexões, statements e result sets automaticamente, evitando vazamentos de recursos (resource leaks);</li>
 *   <li><b>Tradução de Exceções:</b> Captura de {@link SQLException} (checked exception) e relançamento como
 *       {@link RuntimeException} (unchecked exception) com encadeamento da causa original;</li>
 *   <li><b>Processamento em Lote (JDBC Batch Processing):</b> Agrupamento de instruções SQL em blocos para reduzir drasticamente
 *       latência de rede e acessos a disco durante inserções volumosas;</li>
 *   <li><b>Controle Transacional Explícito:</b> Desativação de {@code autoCommit} com controle manual de {@code commit()}
 *       e {@code rollback()} para assegurar a propriedade de Atomicidade do padrão ACID.</li>
 * </ul>
 */
public class TransactionSQLRepository implements TransactionRepository {

    /**
     * Tamanho padrão do lote (batch) para envio agrupado de instruções de inserção ao banco via JDBC.
     *
     * <p><b>Objetivo Educacional e de Engenharia de Dados:</b></p>
     * O valor {@code 1_000} representa um ponto ideal de equilíbrio (trade-off) entre:
     * <ul>
     *   <li><b>Uso de Memória RAM na JVM:</b> Manter batches pequenos o suficiente para não consumir memória heap excessiva com statements pendentes;</li>
     *   <li><b>Eficiência de Rede:</b> Enviar um número significativo de linhas em cada pacote de rede para amortizar a latência de transmissão;</li>
     *   <li><b>Limites do MySQL:</b> Permanecer bem abaixo do limite de tamanho máximo de pacote (parâmetro {@code max_allowed_packet}) e
     *       da janela de logs do mecanismo transacional (InnoDB redo/undo logs).</li>
     * </ul>
     * <p>Tipo: {@code int}. Constante estática e imutável ({@code public static final}).</p>
     */
    public static final int JDBC_BATCH_SIZE = 1_000;

    /**
     * Localiza a primeira transação associada a um cliente emissor específico no banco de dados.
     *
     * <p><b>Propósito do Método:</b></p>
     * Consultar a tabela {@code transactions} buscando pelo identificador do cliente de origem ({@code originName}),
     * ordenando pelo passo temporal ({@code step}) e limitando o resultado a 1 registro.
     *
     * <p><b>Lógica Passo a Passo:</b></p>
     * <ol>
     *   <li><b>Definição da Query SQL:</b> Utiliza um <i>Text Block</i> do Java 15+ (delimitado por {@code """..."""})
     *       para escrever a instrução SQL multilinhas com clareza, contendo o parâmetro {@code WHERE name_origin = ?}.</li>
     *   <li><b>Abertura de Recursos (try-with-resources):</b> Obtém uma conexão com o banco via {@link ConnectionFactory#getConnection()}
     *       e compila a instrução através de {@link Connection#prepareStatement(String)}. Ambos os recursos
     *       são declarados no cabeçalho do {@code try} para fechamento automático ao término do bloco.</li>
     *   <li><b>Parametrização:</b> Define o valor do primeiro placeholder (índice 1-based no JDBC) com o valor de {@code originName}.</li>
     *   <li><b>Execução da Query:</b> Executa a consulta via {@link PreparedStatement#executeQuery()}, abrindo um {@link ResultSet}
     *       aninhado também gerenciado por {@code try-with-resources}.</li>
     *   <li><b>Avaliação do Resultado:</b>
     *       <ul>
     *         <li>Se {@code rs.next()} for verdadeiro: há ao menos uma linha retornada. O registro é convertido
     *             em objeto de domínio pelo método {@link #mapResultSetTransaction(ResultSet)}, o nome é impresso
     *             no console e o resultado é retornado empacotado em {@link Optional#of(Object)}.</li>
     *         <li>Se {@code rs.next()} for falso: nenhum registro corresponde ao critério. Uma mensagem informativa
     *             é impressa e retorna {@link Optional#empty()}.</li>
     *       </ul>
     *   </li>
     * </ol>
     *
     * @param originName Identificador único da conta/cliente de origem (ex: "C1231006815").
     *                   Tipo: {@link String}.
     * @return {@link Optional} contendo a {@link Transaction} localizada no banco, ou {@link Optional#empty()} se inexistente.
     * @throws RuntimeException Caso ocorra qualquer erro de comunicação ou sintaxe SQL (encapsulando a {@link SQLException}).
     */
    @Override
    public Optional<Transaction> findByOriginName(String originName) {

        // sql: Variável do tipo String contendo o comando DQL (Data Query Language) parametrizado.
        // O uso do placeholder '?' previne SQL Injection e permite cache do plano de execução pelo SGBD.
        String sql = """
                SELECT id, step, `type`, amount, name_origin, old_balance_origin, new_balance_origin,
                name_recipient, old_balance_recipient, new_balance_recipient, is_fraud, is_flagged_fraud
                FROM zenon_frauds.transactions
                WHERE name_origin = ?
                ORDER BY step
                LIMIT 1;
                """;

        // Bloco try-with-resources: garante que a conexão (conn) e o statement (ps) sejam fechados
        // automaticamente ao término do bloco, mesmo em caso de erro ou exceção inesperada.
        try (Connection conn = ConnectionFactory.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            // Substitui o primeiro placeholder (?) pelo nome do cliente de origem passado como argumento.
            // Os índices de parâmetros no JDBC iniciam em 1 (1-based index).
            ps.setString(1, originName);

            // try-with-resources aninhado: garante o fechamento automático do cursor do ResultSet (rs)
            try (ResultSet rs = ps.executeQuery()) {

                // rs.next(): move o cursor da tabela de resultados para a próxima linha (inicialmente aponta antes da 1ª linha).
                // Retorna 'true' se houver um registro válido disponível ou 'false' se o resultado estiver vazio.
                if (rs.next()) {
                    // Converte a linha atual do ResultSet no objeto de domínio Transaction
                    Transaction transaction = mapResultSetTransaction(rs);
                    IO.println(rs.getString("name_origin"));
                    return Optional.of(transaction);
                } else {
                    // Informa no console que a transação para o cliente informado não foi localizada no banco
                    IO.println("Transaction not found: " + originName);
                    return Optional.empty();
                }

            }

        } catch (SQLException e) {
            // Converte a checked exception SQLException em RuntimeException (unchecked),
            // preservando a rastreabilidade do erro através do encadeamento de causas (e)
            throw new RuntimeException("Erro ao buscar transação da origin" + originName, e);
        }
    }

    /**
     * Insere e persiste os dados de uma nova transação financeira na tabela do banco de dados.
     *
     * <p><b>Propósito do Método:</b></p>
     * Gravar permanentemente uma instância de {@link Transaction} na tabela {@code transactions}
     * do banco de dados relacional.
     *
     * <p><b>Lógica Passo a Passo:</b></p>
     * <ol>
     *   <li><b>Comando SQL INSERT:</b> Define a instrução DML com os 11 campos da tabela e seus respectivos
     *       placeholders posicionais ({@code ?}). Note o escape da coluna `` `type` `` com crases, pois
     *       {@code TYPE} pode ser uma palavra reservada em determinadas versões e dialetos SQL.</li>
     *   <li><b>Abertura de Conexão e Statement:</b> Instancia a conexão e prepara a query com {@code try-with-resources}.</li>
     *   <li><b>Mapeamento Objeto-Relacional (Decomposição):</b> Desmonta os atributos do objeto {@link Transaction}
     *       e dos records aninhados ({@link TransactionCustomer}) preenchendo os tipos de dados correspondentes no SQL:
     *       <ul>
     *         <li>Inteiros: {@link PreparedStatement#setInt(int, int)};</li>
     *         <li>Strings / Enums: {@link PreparedStatement#setString(int, String)};</li>
     *         <li>Valores Monetários: {@link PreparedStatement#setBigDecimal(int, BigDecimal)} para preservar precisão absoluta;</li>
     *         <li>Booleanos: {@link PreparedStatement#setBoolean(int, boolean)}.</li>
     *       </ul>
     *   </li>
     *   <li><b>Execução:</b> Invoca {@link PreparedStatement#execute()} para enviar a instrução de inserção ao SGBD.</li>
     * </ol>
     *
     * @param transaction Instância do record {@link Transaction} contendo todas as informações da movimentação.
     * @throws RuntimeException Caso ocorra falha de comunicação, integridade ou sintaxe no banco de dados.
     */
    @Override
    public void save(Transaction transaction) {
        // sql: Instrução de inserção (DML - Data Manipulation Language) com 11 parâmetros posicionais (?)
        String sql = """
                insert into transactions
                (step, `type`, amount, name_origin, old_balance_origin, new_balance_origin, name_recipient, old_balance_recipient, new_balance_recipient, is_fraud, is_flagged_fraud)
                values
                    (?,?,?,?,?,?,?,?,?,?,?);
                """;

        // Gerenciamento seguro de conexão e PreparedStatement via try-with-resources
        try (Connection conn = ConnectionFactory.getConnection();
            PreparedStatement ps = conn.prepareStatement(sql)) {

            // Mapeamento dos campos da transação principal:
            ps.setInt(1, transaction.step()); // 1. Passo de tempo da simulação (step)
            ps.setString(2, transaction.type().name()); // 2. Nome do enum da transação (ex: "PAYMENT", "CASH_OUT")
            ps.setBigDecimal(3, transaction.amount()); // 3. Valor monetário exato da operação

            // Mapeamento dos campos do cliente emissor (Origem):
            ps.setString(4, transaction.transactionCustomerOrigin().name()); // 4. Nome/ID do cliente de origem
            ps.setBigDecimal(5, transaction.transactionCustomerOrigin().oldBalance()); // 5. Saldo anterior à transação
            ps.setBigDecimal(6, transaction.transactionCustomerOrigin().newBalance()); // 6. Novo saldo pós-transação

            // Mapeamento dos campos do cliente recebedor (Destino):
            ps.setString(7, transaction.transactionCustomerRecipient().name()); // 7. Nome/ID do cliente destinatário
            ps.setBigDecimal(8, transaction.transactionCustomerRecipient().oldBalance()); // 8. Saldo do destinatário antes
            ps.setBigDecimal(9, transaction.transactionCustomerRecipient().newBalance()); // 9. Saldo do destinatário depois

            // Mapeamento dos sinalizadores booleanos de fraude:
            ps.setBoolean(10, transaction.isFraud()); // 10. Indicador de fraude real confirmada
            ps.setBoolean(11, transaction.isFlaggedFraud()); // 11. Indicador de fraude sinalizada pelo sistema de heurística

            IO.println("Salvando transação: " + transaction.step() + " / " + transaction.amount());

            // Executa a instrução INSERT no banco de dados MySQL
            ps.execute();

        } catch (SQLException e) {
            // Em caso de falha de conexão, violação de constraint ou erro de rede, encapsula a exceção
            throw new RuntimeException("Erro ao salvar a nova transação: " + transaction, e);
        }
    }

    /**
     * Método auxiliar privado de Mapeamento Objeto-Relacional (Manual ORM).
     *
     * <p><b>Propósito do Método:</b></p>
     * Transformar os dados relacionais brutos contidos na linha atual do cursor {@link ResultSet}
     * em instâncias ricas dos objetos de domínio Java ({@link Transaction}, {@link TransactionCustomer}
     * e {@link TransactionType}).
     *
     * <p><b>Por que este método é privado e separado?</b></p>
     * Aplica o princípio da Responsabilidade Única (SRP) e o princípio DRY (<i>Don't Repeat Yourself</i>).
     * Ao isolar a lógica de conversão do {@link ResultSet} em um método específico, desacoplamos a execução
     * da consulta da mecânica de montagem dos objetos, mantendo o código dos métodos de busca conciso e limpo.
     *
     * <p><b>Detalhamento da Extração e Conversão:</b></p>
     * <ul>
     *   <li>{@link ResultSet#getInt(String)}: Recupera colunas numéricas inteiras (ex: {@code step});</li>
     *   <li>{@link TransactionType#valueOf(String)}: Converte a {@link String} armazenada no banco de volta
     *       para a respectiva constante do enum tipado;</li>
     *   <li>{@link ResultSet#getBigDecimal(String)}: Recupera valores monetários exatos de colunas {@code DECIMAL},
     *       essencial para evitar imprecisões de arredondamento em sistemas financeiros;</li>
     *   <li>{@link ResultSet#getBoolean(String)}: Converte colunas booleanas/tinyint do MySQL em tipos {@code boolean}.</li>
     * </ul>
     *
     * @param rs Cursor {@link ResultSet} posicionado na linha contendo os dados da transação a serem lidos.
     * @return Nova instância imutável de {@link Transaction} populada com os dados da linha atual.
     * @throws RuntimeException Caso ocorra algum erro durante a leitura das colunas pelo driver JDBC.
     */
    private Transaction mapResultSetTransaction(ResultSet rs) {
        try {
            // Extração dos atributos básicos da transação
            int step = rs.getInt("step");
            // Converte a String textual da coluna 'type' para a constante tipada do Enum correspondente
            TransactionType type = TransactionType.valueOf(rs.getString("type"));
            // Mantém a precisão monetária de ponto fixo através de BigDecimal
            BigDecimal amount = rs.getBigDecimal("amount");

            // Reconstrução do objeto de domínio do cliente de origem (Customer Origin)
            String originName = rs.getString("name_origin");
            BigDecimal originOldBalance = rs.getBigDecimal("old_balance_origin");
            BigDecimal originNewBalance = rs.getBigDecimal("new_balance_origin");
            TransactionCustomer origin = new TransactionCustomer(originName, originOldBalance, originNewBalance);

            // Reconstrução do objeto de domínio do cliente destinatário (Customer Recipient)
            String recipientName = rs.getString("name_recipient");
            BigDecimal recipientOldBalance =  rs.getBigDecimal("old_balance_recipient");
            BigDecimal recipientNewBalance =  rs.getBigDecimal("new_balance_recipient");
            TransactionCustomer recipient = new TransactionCustomer(recipientName, recipientOldBalance, recipientNewBalance);

            // Extração dos indicadores lógicos de fraude
            boolean isFraud = rs.getBoolean("is_fraud");
            boolean isFlaggedFraud = rs.getBoolean("is_flagged_fraud");

            // Instanciação e retorno do record imutável Transaction com todos os seus atributos compostos
            return new Transaction(step, type, amount, origin, recipient, isFraud, isFlaggedFraud);
        } catch (SQLException e) {
            // Relança a exceção de acesso aos dados como RuntimeException para simplificar o tratamento pelo chamador
            throw new RuntimeException(e);
        }
    }

    /**
     * Persiste uma lista de transações em massa (Bulk / Batch Insert) no banco de dados com alto desempenho.
     *
     * <p><b>Propósito e Responsabilidade:</b></p>
     * Salvar dezenas ou centenas de milhares de instâncias de {@link Transaction} de maneira eficiente,
     * utilizando as capacidades nativas de <i>Batch Processing</i> do JDBC combinadas com controle
     * manual de transação e commits periódicos.
     *
     * <p><b>Por que esta abordagem é ordens de grandeza mais rápida que {@link #save(Transaction)}?</b></p>
     * <ol>
     *   <li><b>Desativação do AutoCommit ({@code conn.setAutoCommit(false)}):</b>
     *       Por padrão, o JDBC opera com autocommit ligado, o que força o MySQL a realizar uma operação
     *       de gravação física em disco (<i>fsync</i> no transaction log) para CADA instrução individual.
     *       Ao desativar o autocommit, agrupamos centenas de inserções em uma única transação, economizando
     *       tempo de CPU e operações de I/O de disco.</li>
     *   <li><b>JDBC Batching ({@link PreparedStatement#addBatch()} e {@link PreparedStatement#executeBatch()}):</b>
     *       Em vez de enviar os parâmetros pela rede linha a linha, o driver JDBC armazena os valores na memória local.
     *       Ao chamar {@code executeBatch()}, o lote acumulado é transmitido de uma vez só. Combinado com o parâmetro
     *       {@code rewriteBatchedStatements=true} da {@link ConnectionFactory}, o MySQL Connector/J transforma as instruções
     *       em um único comando {@code INSERT ... VALUES (...), (...)}, reduzindo drasticamente os <i>round-trips</i> de rede.</li>
     *   <li><b>Despejo Periódico (Flush em lotes de {@link #JDBC_BATCH_SIZE}):</b>
     *       Enviar dezenas de milhares de registros em um único lote gigantesco poderia estourar a memória heap da JVM
     *       ou exceder o parâmetro {@code max_allowed_packet} do MySQL. Por isso, a cada {@code JDBC_BATCH_SIZE} (1.000)
     *       registros adicionados, o lote atual é executado e comitado, liberando os buffers de memória.</li>
     *   <li><b>Garantia de Atomicidade (Rollback):</b>
     *       Caso qualquer registro cause falha durante a execução do batch, o bloco {@code catch} invoca
     *       {@link Connection#rollback()}, desfazendo as alterações pendentes daquele lote e garantindo que
     *       dados inconsistentes ou pela metade não permaneçam no banco de dados.</li>
     * </ol>
     *
     * @param transactions Lista de objetos {@link Transaction} a serem inseridos no banco. Tipo: {@link List}.
     * @throws RuntimeException Caso ocorra qualquer falha durante a conexão, execução em lote ou execução do rollback.
     */
    public void saveAll(List<Transaction> transactions) {

        // sql: Comando SQL INSERT parametrizado com os 11 campos da tabela transactions
        String sql = """
        insert into transactions
        (step, `type`, amount, name_origin , old_balance_origin , new_balance_origin, name_recipient , old_balance_recipient , new_balance_recipient , is_fraud , is_flagged_fraud  )
        values (?,?,?,?,?,?,?,?,?,?,?);
        """;

        // Abre a conexão física com o banco de dados dentro do try-with-resources para fechamento automático
        try (Connection conn = ConnectionFactory.getConnection()) {
            // Desativa a confirmação automática de transação (auto-commit).
            // Isso permite que múltiplos comandos INSERT pertençam a uma mesma transação lógica,
            // evitando que o banco de dados sincronize com o disco a cada linha inserida.
            conn.setAutoCommit(false);

            // count: Variável inteira usada como contador de iterações para controlar o tamanho de cada lote despachado.
            int count = 0;

            // Prepara a instrução SQL parametrizada dentro de um try-with-resources para garantir seu fechamento
            try (PreparedStatement ps = conn.prepareStatement(sql)) {

                // Percorre iterativamente cada transação da lista recebida
                for (Transaction transaction : transactions) {

                    // Vincula os valores de cada atributo aos seus respectivos placeholders posicionais (1 a 11):
                    ps.setInt(1, transaction.step()); // 1. Passo temporal
                    ps.setString(2, transaction.type().name()); // 2. Tipo da transação em texto
                    ps.setBigDecimal(3, transaction.amount()); // 3. Valor monetário preciso

                    ps.setString(4, transaction.transactionCustomerOrigin().name()); // 4. Cliente de origem
                    ps.setBigDecimal(5, transaction.transactionCustomerOrigin().oldBalance()); // 5. Saldo anterior da origem
                    ps.setBigDecimal(6, transaction.transactionCustomerOrigin().newBalance()); // 6. Novo saldo da origem

                    ps.setString(7, transaction.transactionCustomerRecipient().name()); // 7. Cliente de destino
                    ps.setBigDecimal(8, transaction.transactionCustomerRecipient().oldBalance()); // 8. Saldo anterior do destino
                    ps.setBigDecimal(9, transaction.transactionCustomerRecipient().newBalance()); // 9. Novo saldo do destino

                    ps.setBoolean(10, transaction.isFraud()); // 10. Flag booleano de fraude real
                    ps.setBoolean(11, transaction.isFlaggedFraud()); // 11. Flag booleano de suspeita de fraude

                    // addBatch(): Adiciona o conjunto atual de parâmetros ao buffer de comandos em memória do PreparedStatement,
                    // sem enviá-lo imediatamente pela rede ao banco de dados.
                    ps.addBatch();
                    // Incrementa o contador de registros acumulados
                    count++;

                    // Operação de módulo (%): Verifica se a quantidade acumulada atingiu o tamanho definido por JDBC_BATCH_SIZE (ex: 1.000).
                    // Quando count for múltiplo de 1.000 (1000, 2000, 3000...), a condição é verdadeira.
                    if (count % JDBC_BATCH_SIZE == 0) {
                        IO.println("Executando batch JDBC...");

                        // executeBatch(): Envia o lote acumulado de 1.000 registros para o MySQL executar de uma única vez.
                        ps.executeBatch();
                        // commit(): Confirma e efetiva a persistência duradoura das alterações no banco de dados.
                        conn.commit();
                    }
                }

                // Após o loop, podem ter sobrado registros que não completaram um múltiplo exato de JDBC_BATCH_SIZE
                // (por exemplo: se a lista tiver 2.500 registros, 2.000 foram enviados no loop e sobraram 500 no buffer).
                IO.println("Executando batch final JDBC...");

                // Despacha os registros residuais restantes no buffer do PreparedStatement
                ps.executeBatch();
                // Confirma a transação final
                conn.commit();
                // Restaura o estado de auto-commit da conexão para o valor padrão (true) antes de devolver/fechar
                conn.setAutoCommit(true);

            } catch (SQLException e) {
                // Caso ocorra qualquer erro de banco de dados durante o processamento do lote,
                // realizamos o rollback para reverter as alterações não confirmadas, preservando a consistência dos dados.
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    // Se até mesmo o rollback falhar (ex: conexão perdida abruptamente), relança com explicação
                    throw new RuntimeException("Erro ao executar rollback", ex);
                }
                // Envelopa a SQLException original em RuntimeException informando a falha no salvamento
                throw new RuntimeException("Erro ao salvar nova transação ", e);
            }

        } catch (SQLException e) {
            // Captura falhas de obtenção de conexão com o banco de dados
            throw new RuntimeException("Erro na conexão comm o BD...", e);
        }
    }
}
