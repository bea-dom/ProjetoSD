import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;

import com.google.gson.Gson;

// [Secao 8] Classe Mensagem: objeto transferido em TODA comunicacao
// cliente <-> servidor e servidor <-> servidor (Secoes 4 e 5).
// Cada objeto e convertido para JSON (Gson) em uma unica linha para ir pelo socket TCP.
public class Mensagem {

    // Tipos de mensagem possiveis (valores do atributo "tipo")
    public static final String PUT = "PUT";                             // cliente pede para inserir/atualizar key-value
    public static final String PUT_OK = "PUT_OK";                       // confirmacao do PUT, com o timestamp da key
    public static final String GET = "GET";                             // cliente pede o value de uma key
    public static final String GET_RESPONSE = "GET_RESPONSE";           // resposta do GET com value e timestamp
    public static final String WAIT_FOR_RESPONSE = "WAIT_FOR_RESPONSE"; // servidor ainda nao tem a key atualizada
    public static final String REPLICATION = "REPLICATION";             // lider envia key, value e timestamp aos outros servidores
    public static final String REPLICATION_OK = "REPLICATION_OK";       // servidor confirma ao lider que gravou a replicacao
    public static final String SYNC_REQUEST = "SYNC_REQUEST";           // nao lider se registra no lider e pede a tabela inteira
    public static final String SYNC_RESPONSE = "SYNC_RESPONSE";         // lider devolve a tabela inteira ao nao lider

    // Objeto Gson usado para converter Mensagem <-> JSON
    private static Gson gson = new Gson();

    // Tipo da mensagem (uma das constantes acima)
    private String tipo;
    // Chave do par key-value
    private String key;
    // Valor do par key-value (no GET de key inexistente vai "NULL")
    private String value;
    // Timestamp da key no servidor (gerado pelo lider no PUT)
    private long timestamp;
    // Ultimo timestamp que o cliente conhece para a key (enviado no GET)
    private long timestampCliente;
    // IP do cliente original (para prints e para a resposta assincrona)
    private String ipCliente;
    // Porta de escuta do cliente (ServerSocket aberto no INIT, recebe a resposta assincrona)
    private int portaCliente;
    // IP do servidor (ex.: servidor que se registra no lider no SYNC_REQUEST)
    private String ipServidor;
    // Porta do servidor (ex.: servidor que se registra no lider no SYNC_REQUEST)
    private int portaServidor;
    // No SYNC_RESPONSE: true se o servidor ja estava registrado no lider (ou seja, caiu e voltou)
    private boolean jaRegistrado;
    // No SYNC_RESPONSE: copia da tabela de valores do lider (key -> value)
    private HashMap<String, String> tabelaValores;
    // No SYNC_RESPONSE: copia da tabela de timestamps do lider (key -> timestamp)
    private HashMap<String, Long> tabelaTimestamps;

    // Construtor vazio: os atributos sao preenchidos pelos setters
    public Mensagem() {
    }

    // Converte a mensagem em JSON (uma linha) e envia pelo socket, no padrao de aula
    public static void enviar(Mensagem m, DataOutputStream writer) throws IOException {
        String json = gson.toJson(m);
        writer.write((json + "\n").getBytes("UTF-8"));
        writer.flush();
    }

    // Le uma linha JSON do socket e converte de volta em Mensagem (null se a conexao fechou)
    public static Mensagem receber(BufferedReader reader) throws IOException {
        String json = reader.readLine();
        if (json == null) {
            return null;
        }
        return gson.fromJson(json, Mensagem.class);
    }

    // Getters e setters de cada atributo

    public String getTipo() {
        return tipo;
    }

    public void setTipo(String tipo) {
        this.tipo = tipo;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestampCliente() {
        return timestampCliente;
    }

    public void setTimestampCliente(long timestampCliente) {
        this.timestampCliente = timestampCliente;
    }

    public String getIpCliente() {
        return ipCliente;
    }

    public void setIpCliente(String ipCliente) {
        this.ipCliente = ipCliente;
    }

    public int getPortaCliente() {
        return portaCliente;
    }

    public void setPortaCliente(int portaCliente) {
        this.portaCliente = portaCliente;
    }

    public String getIpServidor() {
        return ipServidor;
    }

    public void setIpServidor(String ipServidor) {
        this.ipServidor = ipServidor;
    }

    public int getPortaServidor() {
        return portaServidor;
    }

    public void setPortaServidor(int portaServidor) {
        this.portaServidor = portaServidor;
    }

    public boolean isJaRegistrado() {
        return jaRegistrado;
    }

    public void setJaRegistrado(boolean jaRegistrado) {
        this.jaRegistrado = jaRegistrado;
    }

    public HashMap<String, String> getTabelaValores() {
        return tabelaValores;
    }

    public void setTabelaValores(HashMap<String, String> tabelaValores) {
        this.tabelaValores = tabelaValores;
    }

    public HashMap<String, Long> getTabelaTimestamps() {
        return tabelaTimestamps;
    }

    public void setTabelaTimestamps(HashMap<String, Long> tabelaTimestamps) {
        this.tabelaTimestamps = tabelaTimestamps;
    }
}
