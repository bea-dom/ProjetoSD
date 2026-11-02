import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

// Servidor do KV Store: atende clientes e outros servidores por TCP.
// Um dos tres servidores e o lider (fixo); so ele executa o PUT e replica para os outros.
public class Servidor {

    // ===================== Configuracoes =====================

    // Tempo maximo (ms) para conectar em outro servidor; se passar, a conexao e dada como falha
    static final int TIMEOUT_CONEXAO_MS = 1000;
    // Intervalo (ms) entre as tentativas de SYNC_REQUEST enquanto o lider ainda nao esta no ar
    static final int INTERVALO_SYNC_MS = 1000;
    // Atraso (ms) que os nao lideres aplicam antes de gravar uma REPLICATION (para demonstrar o WAIT_FOR_RESPONSE)
    static final int ATRASO_REPLICACAO_MS = 15000;

    // ===================== Enderecos =====================

    // IP e porta deste servidor (lidos do teclado na inicializacao)
    static String meuIp;
    static int minhaPorta;
    // IP e porta do lider (lidos do teclado na inicializacao)
    static String ipLider;
    static int portaLider;
    // true se este servidor e o lider (seu endereco e igual ao do lider)
    static boolean souLider;

    // ===================== Estruturas de dados =====================

    // [Secao 5] Tabela de hash local com os valores: key -> value
    static HashMap<String, String> tabelaValores = new HashMap<String, String>();
    // [Secao 5] Tabela de hash local com os timestamps: key -> timestamp da key (contador logico por key)
    static HashMap<String, Long> tabelaTimestamps = new HashMap<String, Long>();

    // Usado so no lider: registro dos outros servidores, "ip:porta" -> true (ATIVO) ou false (INATIVO).
    // Um servidor nunca e apagado daqui; so muda de estado.
    static HashMap<String, Boolean> registroServidores = new HashMap<String, Boolean>();
    // Usado so no lider: quantas vezes cada servidor ja se registrou (SYNC_REQUEST), "ip:porta" -> numero.
    // Serve para uma replicacao que falhou ANTES de o servidor voltar nao marca-lo como INATIVO DEPOIS da volta.
    static HashMap<String, Integer> numeroRegistros = new HashMap<String, Integer>();

    // GETs que receberam WAIT_FOR_RESPONSE e aguardam a key chegar com timestamp suficiente.
    // Cada Mensagem guarda a key, o timestamp do cliente e o IP/porta de escuta do cliente.
    static ArrayList<Mensagem> getsPendentes = new ArrayList<Mensagem>();

    // Trava UNICA que protege, contra acessos concorrentes, todo o estado compartilhado acima:
    // tabelaValores, tabelaTimestamps, registroServidores, numeroRegistros e getsPendentes.
    // Dentro dela nunca ha I/O de rede nem print (os trechos protegidos sao curtos).
    static final Object travaEstado = new Object();

    public static void main(String[] args) throws Exception {
        // [Secao 5] Inicializacao: le do teclado o IP e a porta deste servidor e do lider
        Scanner teclado = new Scanner(System.in);
        System.out.print("IP deste servidor: ");
        meuIp = teclado.nextLine().trim();
        System.out.print("Porta deste servidor: ");
        minhaPorta = Integer.parseInt(teclado.nextLine().trim());
        System.out.print("IP do l\u00edder: ");
        ipLider = teclado.nextLine().trim();
        System.out.print("Porta do l\u00edder: ");
        portaLider = Integer.parseInt(teclado.nextLine().trim());
        souLider = meuIp.equals(ipLider) && minhaPorta == portaLider;

        // A porta e reservada (bind) ANTES da sincronizacao: uma REPLICATION que o lider mande logo
        // depois do registro espera na fila do sistema operacional em vez de falhar
        ServerSocket serverSocket = new ServerSocket(minhaPorta);

        // [Secao 5] Nao lider: registra-se no lider (e recupera a tabela) ANTES de aceitar conexoes
        if (!souLider) {
            sincronizarComLider();
        }

        // [Secao 5] Recebe e responde simultaneamente: uma ThreadAtendimento para cada conexao aceita
        while (true) {
            Socket no = serverSocket.accept();
            new ThreadAtendimento(no).start();
        }
    }

    // Abre uma conexao TCP com outro servidor, desistindo depois de TIMEOUT_CONEXAO_MS
    static Socket conectar(String ip, int porta) throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress(ip, porta), TIMEOUT_CONEXAO_MS);
        return s;
    }

    // [Secao 5] Queda e retorno: registro no lider e recuperacao das informacoes.
    // Envia SYNC_REQUEST com o endereco deste servidor e espera o SYNC_RESPONSE, que traz a tabela
    // completa do lider. Se o lider ainda nao estiver no ar, tenta de novo em silencio a cada INTERVALO_SYNC_MS.
    // Roda ANTES do primeiro accept(): uma REPLICATION que o lider envie durante a sincronizacao fica na
    // fila do ServerSocket e so e tratada depois que a tabela recebida ja foi carregada.
    static void sincronizarComLider() {
        while (true) {
            try {
                Socket s = conectar(ipLider, portaLider);
                InputStreamReader is = new InputStreamReader(s.getInputStream(), "UTF-8");
                BufferedReader reader = new BufferedReader(is);
                OutputStream os = s.getOutputStream();
                DataOutputStream writer = new DataOutputStream(os);

                Mensagem pedido = new Mensagem();
                pedido.setTipo(Mensagem.SYNC_REQUEST);
                pedido.setIpServidor(meuIp);
                pedido.setPortaServidor(minhaPorta);
                Mensagem.enviar(pedido, writer);

                Mensagem resposta = Mensagem.receber(reader);
                s.close();
                if (resposta != null && Mensagem.SYNC_RESPONSE.equals(resposta.getTipo())) {
                    // [Secao 5] Queda e retorno: recupera todas as informacoes do lider
                    synchronized (travaEstado) {
                        tabelaValores.clear();
                        tabelaTimestamps.clear();
                        if (resposta.getTabelaValores() != null) {
                            tabelaValores.putAll(resposta.getTabelaValores());
                            tabelaTimestamps.putAll(resposta.getTabelaTimestamps());
                        }
                    }
                    // [Secao 6] So no retorno (o lider ja conhecia este endereco); no primeiro inicio nao imprime
                    if (resposta.isJaRegistrado()) {
                        System.out.println("Informa\u00e7\u00f5es recebidas do l\u00edder");
                    }
                    return;
                }
            } catch (Exception e) {
                // lider fora do ar: tenta de novo em silencio
            }
            try {
                Thread.sleep(INTERVALO_SYNC_MS);
            } catch (InterruptedException e) {
            }
        }
    }

    // [Secao 5] Thread que atende UMA conexao (de cliente ou de outro servidor):
    // le uma Mensagem, trata conforme o tipo e responde.
    static class ThreadAtendimento extends Thread {
        // Conexao aceita pelo ServerSocket (de um cliente ou de outro servidor) que esta thread atende
        private Socket no = null;

        public ThreadAtendimento(Socket node) {
            no = node;
        }

        public void run() {
            try {
                InputStreamReader is = new InputStreamReader(no.getInputStream(), "UTF-8");
                BufferedReader reader = new BufferedReader(is);
                OutputStream os = no.getOutputStream();
                DataOutputStream writer = new DataOutputStream(os);

                Mensagem pedido = Mensagem.receber(reader);
                if (pedido == null) {
                    no.close();
                    return;
                }

                Mensagem resposta = null;
                String tipo = pedido.getTipo();
                if (Mensagem.PUT.equals(tipo) && souLider) {
                    resposta = putNoLider(pedido);
                } else if (Mensagem.PUT.equals(tipo)) {
                    resposta = encaminharPutAoLider(pedido);
                } else if (Mensagem.REPLICATION.equals(tipo)) {
                    resposta = receberReplicacao(pedido);
                } else if (Mensagem.SYNC_REQUEST.equals(tipo) && souLider) {
                    resposta = registrarServidor(pedido);
                } else if (Mensagem.GET.equals(tipo)) {
                    resposta = receberGet(pedido);
                }
                if (resposta != null) {
                    Mensagem.enviar(resposta, writer);
                }
                no.close();
            } catch (Exception e) {
                // falha (ex.: lider fora do ar no encaminhamento): fecha a conexao sem resposta,
                // e o cliente tenta outro servidor
                try {
                    no.close();
                } catch (IOException e2) {
                }
            }
        }
    }

    // [Secao 5] Recebe PUT - servidor lider:
    // insere (ou atualiza) key e value na tabela local com um novo timestamp, replica para os outros
    // servidores e, depois de todos confirmarem, devolve o PUT_OK.
    // O timestamp e um contador por key: o primeiro PUT de uma key gera 1, o segundo 2, e assim por diante.
    static Mensagem putNoLider(Mensagem pedido) {
        String key = pedido.getKey();
        String value = pedido.getValue();
        // [Secao 6] IP e porta do cliente original (vindos dentro da Mensagem)
        System.out.println("Cliente " + pedido.getIpCliente() + ":" + pedido.getPortaCliente()
                + " PUT key:" + key + " value:" + value);

        long novoTimestamp;
        // Regiao critica: ler o timestamp atual, somar 1 e gravar precisa acontecer de uma vez so,
        // senao dois PUTs simultaneos poderiam ler o mesmo valor e gerar o mesmo timestamp
        synchronized (travaEstado) {
            long timestampAtual = 0;
            if (tabelaTimestamps.containsKey(key)) {
                timestampAtual = tabelaTimestamps.get(key);
            }
            novoTimestamp = timestampAtual + 1;
            // Se a key ja existir, o put sobrescreve tanto o value quanto o timestamp
            tabelaValores.put(key, value);
            tabelaTimestamps.put(key, novoTimestamp);
        }

        // [Secao 5] Replica a informacao (key, value, timestamp) nos outros servidores.
        // A trava ja foi solta: a espera pela rede nao bloqueia outros PUTs e GETs.
        replicarParaServidores(key, value, novoTimestamp);

        // [Secao 5] Recebe REPLICATION_OK de todos: so agora envia o PUT_OK ao cliente
        System.out.println("Enviando PUT_OK ao Cliente " + pedido.getIpCliente() + ":" + pedido.getPortaCliente()
                + " da key:" + key + " ts:" + novoTimestamp);
        Mensagem resposta = new Mensagem();
        resposta.setTipo(Mensagem.PUT_OK);
        resposta.setKey(key);
        resposta.setValue(value);
        resposta.setTimestamp(novoTimestamp);
        return resposta;
    }

    // [Secao 5] Envia a REPLICATION a cada servidor ATIVO do registro, cada envio numa ThreadReplicacao,
    // e espera todas terminarem (join). Quem nao confirmou e marcado INATIVO (mas continua no registro).
    static void replicarParaServidores(String key, String value, long timestamp) {
        // Mensagem REPLICATION (key, value, timestamp), a mesma enviada a todos os servidores
        Mensagem replicacao = new Mensagem();
        replicacao.setTipo(Mensagem.REPLICATION);
        replicacao.setKey(key);
        replicacao.setValue(value);
        replicacao.setTimestamp(timestamp);

        // Lista temporaria: uma ThreadReplicacao para cada servidor ATIVO (com o numero de registro dele).
        // E montada dentro da trava, porque o registro pode mudar durante o envio
        ArrayList<ThreadReplicacao> threads = new ArrayList<ThreadReplicacao>();
        synchronized (travaEstado) {
            for (String endereco : registroServidores.keySet()) {
                if (registroServidores.get(endereco)) {
                    threads.add(new ThreadReplicacao(endereco, numeroRegistros.get(endereco).intValue(), replicacao));
                }
            }
        }

        // Uma thread por servidor: as replicacoes acontecem em paralelo
        for (ThreadReplicacao t : threads) {
            t.start();
        }

        // Espera todas as threads; quem falhou vira INATIVO
        for (ThreadReplicacao t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
            }
            if (!t.isConfirmou()) {
                synchronized (travaEstado) {
                    // So marca INATIVO se o servidor NAO se registrou de novo durante a tentativa.
                    // Se ele caiu e ja voltou (numero de registro mudou), continua ATIVO.
                    if (numeroRegistros.get(t.getEndereco()).intValue() == t.getNumeroRegistro()) {
                        registroServidores.put(t.getEndereco(), false);
                    }
                }
            }
        }
    }

    // [Secao 5] Thread que envia UMA REPLICATION a UM servidor e espera o REPLICATION_OK dele
    static class ThreadReplicacao extends Thread {
        // Endereco "ip:porta" do servidor que vai receber a replicacao
        private String endereco = null;
        // Numero de registro do servidor no momento em que a replicacao comecou
        private int numeroRegistro = 0;
        // Mensagem REPLICATION com key, value e timestamp
        private Mensagem replicacao = null;
        // true se o servidor respondeu REPLICATION_OK
        private boolean confirmou = false;

        public ThreadReplicacao(String enderecoServidor, int numero, Mensagem mensagem) {
            endereco = enderecoServidor;
            numeroRegistro = numero;
            replicacao = mensagem;
        }

        public void run() {
            try {
                int separador = endereco.lastIndexOf(':');
                String ip = endereco.substring(0, separador);
                int porta = Integer.parseInt(endereco.substring(separador + 1));

                Socket s = conectar(ip, porta);
                InputStreamReader is = new InputStreamReader(s.getInputStream(), "UTF-8");
                BufferedReader reader = new BufferedReader(is);
                OutputStream os = s.getOutputStream();
                DataOutputStream writer = new DataOutputStream(os);

                Mensagem.enviar(replicacao, writer);
                Mensagem resposta = Mensagem.receber(reader);
                s.close();
                if (resposta != null && Mensagem.REPLICATION_OK.equals(resposta.getTipo())) {
                    confirmou = true;
                }
            } catch (Exception e) {
                // servidor fora do ar: confirmou continua false
            }
        }

        public String getEndereco() {
            return endereco;
        }

        public int getNumeroRegistro() {
            return numeroRegistro;
        }

        public boolean isConfirmou() {
            return confirmou;
        }
    }

    // [Secao 5] Recebe PUT - servidor nao lider: encaminha a requisicao ao lider.
    // O PUT vai como chegou (com IP e porta do cliente original); o PUT_OK do lider
    // e devolvido ao cliente pela conexao que o cliente abriu com este servidor.
    static Mensagem encaminharPutAoLider(Mensagem pedido) throws IOException {
        System.out.println("Encaminhando PUT key:" + pedido.getKey() + " value:" + pedido.getValue());

        Socket s = conectar(ipLider, portaLider);
        InputStreamReader is = new InputStreamReader(s.getInputStream(), "UTF-8");
        BufferedReader reader = new BufferedReader(is);
        OutputStream os = s.getOutputStream();
        DataOutputStream writer = new DataOutputStream(os);

        Mensagem.enviar(pedido, writer);
        Mensagem respostaDoLider = Mensagem.receber(reader);
        s.close();
        return respostaDoLider;
    }

    // [Secao 5] Recebe REPLICATION (nao lider): grava key, value e timestamp na tabela local
    // e responde REPLICATION_OK ao lider.
    static Mensagem receberReplicacao(Mensagem pedido) {
        String key = pedido.getKey();
        String value = pedido.getValue();
        long timestamp = pedido.getTimestamp();
        System.out.println("REPLICATION key:" + key + " value:" + value + " ts:" + timestamp);

        // Atraso opcional, so para conseguir demonstrar o WAIT_FOR_RESPONSE
        if (ATRASO_REPLICACAO_MS > 0) {
            try {
                Thread.sleep(ATRASO_REPLICACAO_MS);
            } catch (InterruptedException e) {
            }
        }

        // So grava se o timestamp recebido for MAIOR que o local: uma replicacao que chegou
        // atrasada (de um PUT mais antigo) nunca sobrescreve um value mais novo
        // Lista temporaria: respostas prontas para os GETs pendentes desta key que puderem ser atendidos
        ArrayList<Mensagem> entregas;
        synchronized (travaEstado) {
            long timestampLocal = 0;
            if (tabelaTimestamps.containsKey(key)) {
                timestampLocal = tabelaTimestamps.get(key);
            }
            if (timestamp > timestampLocal) {
                tabelaValores.put(key, value);
                tabelaTimestamps.put(key, timestamp);
            }
            // [Secao 5] GETs pendentes: ainda dentro da trava, retira da lista os GETs
            // desta key que agora podem ser atendidos (Tx <= timestamp local)
            entregas = retirarPendentesAtendidos(key);
        }

        // [Secao 5] Fora da trava: envia o value aos clientes que estavam esperando, numa thread
        // separada, para o REPLICATION_OK ir ao lider sem esperar essas entregas
        entregarPendentes(entregas);

        Mensagem resposta = new Mensagem();
        resposta.setTipo(Mensagem.REPLICATION_OK);
        resposta.setKey(key);
        resposta.setTimestamp(timestamp);
        return resposta;
    }

    // [Secao 5] GETs pendentes: retira de getsPendentes todos os pedidos da key cujo Tx ja foi alcancado
    // (Tx <= timestamp local) e devolve, para cada um, o GET_RESPONSE pronto com value e timestamp atuais.
    // DEVE ser chamado com a travaEstado ja adquirida (ele le a tabela e altera a lista de pendentes).
    static ArrayList<Mensagem> retirarPendentesAtendidos(String key) {
        // Lista temporaria devolvida: um GET_RESPONSE pronto para cada pedido retirado de getsPendentes
        ArrayList<Mensagem> entregas = new ArrayList<Mensagem>();
        if (!tabelaTimestamps.containsKey(key)) {
            return entregas;
        }
        long timestampLocal = tabelaTimestamps.get(key);
        String valueLocal = tabelaValores.get(key);

        // Percorre de tras para frente para poder remover durante o laco sem pular elementos
        for (int i = getsPendentes.size() - 1; i >= 0; i--) {
            Mensagem pendente = getsPendentes.get(i);
            if (pendente.getKey().equals(key) && pendente.getTimestampCliente() <= timestampLocal) {
                getsPendentes.remove(i);

                Mensagem resposta = new Mensagem();
                resposta.setTipo(Mensagem.GET_RESPONSE);
                resposta.setKey(key);
                resposta.setValue(valueLocal);
                resposta.setTimestamp(timestampLocal);
                resposta.setTimestampCliente(pendente.getTimestampCliente());
                resposta.setIpCliente(pendente.getIpCliente());
                resposta.setPortaCliente(pendente.getPortaCliente());
                resposta.setIpServidor(meuIp);
                resposta.setPortaServidor(minhaPorta);
                entregas.add(resposta);
            }
        }
        return entregas;
    }

    // Dispara a entrega de cada resposta pendente numa ThreadEntregaPendentes propria, sem esperar por elas:
    // um cliente que nao responde nao atrasa a entrega para os outros nem o REPLICATION_OK
    static void entregarPendentes(ArrayList<Mensagem> entregas) {
        for (Mensagem resposta : entregas) {
            new ThreadEntregaPendentes(resposta).start();
        }
    }

    // [Secao 5] Thread que entrega, de forma assincrona, o value a UM cliente que recebeu WAIT_FOR_RESPONSE:
    // abre uma conexao nova ate a porta de escuta do cliente e envia o GET_RESPONSE.
    // Roda fora da trava (I/O de rede nunca acontece dentro da trava).
    static class ThreadEntregaPendentes extends Thread {
        // Resposta pronta (key, value, timestamp do servidor, Tx original, endereco do cliente e do servidor)
        private Mensagem resposta = null;

        public ThreadEntregaPendentes(Mensagem respostaPronta) {
            resposta = respostaPronta;
        }

        public void run() {
            // [Secao 6] Mesmo print do GET, agora com o value que o cliente estava esperando
            System.out.println("Cliente " + resposta.getIpCliente() + ":" + resposta.getPortaCliente()
                    + " GET key:" + resposta.getKey() + " ts:" + resposta.getTimestampCliente()
                    + ". Meu ts \u00e9 " + resposta.getTimestamp()
                    + ", portanto devolvendo " + resposta.getValue());
            try {
                Socket s = conectar(resposta.getIpCliente(), resposta.getPortaCliente());
                OutputStream os = s.getOutputStream();
                DataOutputStream writer = new DataOutputStream(os);
                Mensagem.enviar(resposta, writer);
                s.close();
            } catch (Exception e) {
                // cliente nao respondeu: ignora em silencio
            }
        }
    }

    // [Secao 5] Queda e retorno, lado do lider: registra o servidor como ATIVO e envia a ele a tabela completa.
    // Se o endereco ja existia (ativo ou inativo), apenas volta a ATIVO (o HashMap nao duplica a chave)
    // e jaRegistrado = true, ou seja, o servidor caiu e voltou.
    //
    // Por que nenhuma atualizacao se perde: a copia da tabela e a marcacao como ATIVO acontecem juntas,
    // num unico trecho dentro da travaEstado. Um PUT grava na tabela (dentro da trava) e so depois le a
    // lista de ATIVOS (de novo dentro da trava). Entao todo PUT ou gravou ANTES da copia (e esta nela),
    // ou gravou DEPOIS (e entao ja ve o servidor como ATIVO e replica para ele). Se as duas coisas
    // acontecerem, o servidor recebe a replicacao com um timestamp que ja tem e a ignora (so grava maior).
    static Mensagem registrarServidor(Mensagem pedido) {
        String endereco = pedido.getIpServidor() + ":" + pedido.getPortaServidor();
        // true se o endereco ja constava no registro (o servidor caiu e voltou)
        boolean jaRegistrado;
        // Copias temporarias da tabela (key -> value e key -> timestamp) que vao no SYNC_RESPONSE
        HashMap<String, String> copiaValores;
        HashMap<String, Long> copiaTimestamps;

        synchronized (travaEstado) {
            jaRegistrado = registroServidores.containsKey(endereco);
            registroServidores.put(endereco, true);
            int numero = 0;
            if (numeroRegistros.containsKey(endereco)) {
                numero = numeroRegistros.get(endereco).intValue();
            }
            numeroRegistros.put(endereco, numero + 1);
            // Copias NOVAS: a resposta e enviada fora da trava, enquanto a tabela original continua mudando
            copiaValores = new HashMap<String, String>(tabelaValores);
            copiaTimestamps = new HashMap<String, Long>(tabelaTimestamps);
        }

        // Fora da trava: monta o SYNC_RESPONSE (o envio pela rede acontece na ThreadAtendimento)
        Mensagem resposta = new Mensagem();
        resposta.setTipo(Mensagem.SYNC_RESPONSE);
        resposta.setJaRegistrado(jaRegistrado);
        resposta.setTabelaValores(copiaValores);
        resposta.setTabelaTimestamps(copiaTimestamps);
        return resposta;
    }

    // [Secao 5] Recebe GET (cliente Cx com timestamp Tx pede a key Kx). Sempre atendido localmente,
    // por qualquer servidor (lider ou nao), nunca encaminhado. Tres casos:
    //   a) key inexistente e Tx == 0           -> value NULL, timestamp 0
    //   b) key existe e timestamp local >= Tx  -> GET_RESPONSE com value e timestamp local
    //   c) timestamp local < Tx (ou key inexistente com Tx > 0) -> WAIT_FOR_RESPONSE
    // Assim o cliente NUNCA recebe um value anterior ao que ja viu.
    static Mensagem receberGet(Mensagem pedido) {
        String key = pedido.getKey();
        long timestampCliente = pedido.getTimestampCliente();

        Mensagem resposta = new Mensagem();
        resposta.setKey(key);
        // Tx original vai na resposta (o cliente usa no print)
        resposta.setTimestampCliente(timestampCliente);
        long timestampLocal;

        // Leitura e decisao dentro da trava: value e timestamp precisam ser lidos juntos,
        // sem uma REPLICATION ou um PUT alterando a tabela no meio
        synchronized (travaEstado) {
            boolean existe = tabelaTimestamps.containsKey(key);
            if (existe) {
                timestampLocal = tabelaTimestamps.get(key);
            } else {
                timestampLocal = 0;
            }

            if (!existe && timestampCliente == 0) {
                // a) key inexistente
                resposta.setTipo(Mensagem.GET_RESPONSE);
                resposta.setValue("NULL");
                resposta.setTimestamp(0);
            } else if (existe && timestampLocal >= timestampCliente) {
                // b) servidor tem a key com timestamp igual ou mais novo que o do cliente
                resposta.setTipo(Mensagem.GET_RESPONSE);
                resposta.setValue(tabelaValores.get(key));
                resposta.setTimestamp(timestampLocal);
            } else {
                // c) servidor ainda nao tem a versao que o cliente ja viu
                resposta.setTipo(Mensagem.WAIT_FOR_RESPONSE);
                resposta.setTimestamp(timestampLocal);

                // [Secao 5] Guarda o GET na lista de pendentes AINDA dentro da trava, no mesmo trecho
                // da decisao. Assim uma REPLICATION nao consegue gravar a key entre esta decisao e o
                // pedido entrar na lista: ela so grava depois, e ao gravar ja encontra este pedido na lista.
                getsPendentes.add(pedido);
            }
        }

        // [Secao 6] Print do GET (fora da trava). IP e porta do cliente vem dentro da Mensagem.
        String devolvendo = resposta.getValue();
        if (Mensagem.WAIT_FOR_RESPONSE.equals(resposta.getTipo())) {
            devolvendo = Mensagem.WAIT_FOR_RESPONSE;
        }
        System.out.println("Cliente " + pedido.getIpCliente() + ":" + pedido.getPortaCliente()
                + " GET key:" + key + " ts:" + timestampCliente + ". Meu ts \u00e9 " + timestampLocal
                + ", portanto devolvendo " + devolvendo);
        return resposta;
    }
}
