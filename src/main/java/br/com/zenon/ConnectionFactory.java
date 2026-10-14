package br.com.zenon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Fábrica de conexões com o banco de dados (Padrão <i>Factory</i>).
 *
 * <p><b>Propósito e Responsabilidade:</b></p>
 * Centralizar, encapsular e gerenciar a criação de conexões ativas com o banco de dados relacional MySQL
 * utilizando a API JDBC (<i>Java Database Connectivity</i>). Esta classe abstrai todos os detalhes técnicos
 * de infraestrutura — como URL de conexão JDBC, portas de rede, credenciais de acesso e parâmetros do driver —
 * para que as demais camadas do sistema não precisem conhecer como a conexão física é aberta.
 *
 * <p><b>Integração com o Projeto:</b></p>
 * É utilizada diretamente pelas classes de repositório, como {@link TransactionSQLRepository}, sempre que uma
 * operação de leitura (SELECT) ou escrita/persistência (INSERT) precisa interagir com a base de dados
 * {@code zenon_frauds}.
 *
 * <p><b>Conceitos Educacionais e de Performance:</b></p>
 * <ul>
 *   <li><b>Padrão Factory (Simple Factory):</b> Separa o código que usa o recurso (repositório) do código que
 *       instancia e configura o recurso (esta fábrica), promovendo baixo acoplamento e facilidade de manutenção.</li>
 *   <li><b>Parâmetro {@code rewriteBatchedStatements=true}:</b> Configuração essencial do driver MySQL Connector/J.
 *       Por padrão, o driver envia cada instrução de um lote ({@code batch}) de forma individual para o servidor MySQL.
 *       Ao habilitar {@code rewriteBatchedStatements=true}, o driver reescreve automaticamente múltiplos comandos
 *       {@code INSERT INTO tabela VALUES (...)} em um único comando composto:
 *       {@code INSERT INTO tabela VALUES (...), (...), (...)}. Isso reduz drasticamente o tráfego de rede (round-trips)
 *       e a quantidade de gravações no log de transações do banco, aumentando a performance de inserção em lote em até dezenas de vezes.</li>
 *   <li><b>Tradução de Exceções:</b> Converte {@link SQLException} (uma <i>checked exception</i> obrigatória)
 *       em {@link RuntimeException} (uma <i>unchecked exception</i>), permitindo que erros de infraestrutura
 *       sejam propagados de forma limpa sem poluir as assinaturas dos métodos chamadores.</li>
 * </ul>
 */
public class ConnectionFactory {

    /**
     * Construtor padrão da classe {@link ConnectionFactory}.
     *
     * <p><b>Nota Didática:</b> Como o método {@link #getConnection()} é estático ({@code static}),
     * esta classe atua primordialmente como uma classe utilitária/fábrica estática. Em projetos corporativos,
     * é comum declarar o construtor como {@code private} para prevenir instanciações desnecessárias.</p>
     */
    public ConnectionFactory() {}

    /**
     * Estabelece e retorna uma nova conexão física com o banco de dados MySQL via JDBC.
     *
     * <p><b>Detalhamento da Conexão:</b></p>
     * <ul>
     *   <li><b>Protocolo JDBC:</b> {@code jdbc:mysql://} identifica o driver apropriado registrado no classpath.</li>
     *   <li><b>Host e Porta:</b> {@code localhost:3307} — a porta 3307 é frequentemente mapeada via Docker para
     *       evitar conflitos com instâncias locais de MySQL rodando na porta padrão (3306).</li>
     *   <li><b>Esquema / Base:</b> {@code zenon_frauds} é o banco de dados onde a tabela {@code transactions} reside.</li>
     *   <li><b>Otimização de Lote:</b> {@code ?rewriteBatchedStatements=true} habilita a reescrita de batches para máximo desempenho.</li>
     *   <li><b>Credenciais:</b> Usuário {@code "root"} e senha {@code "senha123"}.</li>
     * </ul>
     *
     * @return {@link Connection} objeto que representa a sessão de comunicação aberta com o banco de dados MySQL.
     * @throws RuntimeException se ocorrer falha na autenticação, banco indisponível ou erro de rede (encapsula {@link SQLException}).
     */
    public static Connection getConnection() {
        try {
            // DriverManager.getConnection:
            // Solicita ao gerenciador de drivers JDBC uma conexão ativa para a URL informada com o usuário e senha fornecidos.
            // O parâmetro 'rewriteBatchedStatements=true' otimiza inserções em lote executadas via PreparedStatement#executeBatch().
            return DriverManager.getConnection("jdbc:mysql://localhost:3307/zenon_frauds?rewriteBatchedStatements=true", "root", "senha123");
        } catch (SQLException e) {
            // Captura a exceção checada SQLException (típica de problemas de conexão ou credenciais)
            // e a relança envelopada em RuntimeException para preservar o stack trace original (e).
            throw new RuntimeException("Erro ao conectar com o Banco de Dados: ",e);
        }
    }

}
