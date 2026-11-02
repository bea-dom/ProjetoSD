import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;

// Cliente do KV Store: menu com INIT, PUT e GET.
// Nao sabe quem e o lider; cada requisicao vai para um dos tres servidores, sorteado.
public class Cliente {

    // Tempo maximo (ms) para conectar em um servidor; se passar, o cliente tenta o proximo
    // (no Windows, conectar numa porta sem servidor pode demorar ~2 s para falhar)
    static final int TIMEOUT_CONEXAO_MS = 500;

    // ===================== Estruturas de dados =====================

    // [Secao 4] IPs dos tres servidores (lidos no INIT)
    static String[] ipsServidores = new String[3];
    // [Secao 4] Portas dos tres servidores (lidas no INIT)
    static int[] portasServidores = new int[3];
    // true depois que o INIT foi feito pelo menos uma vez
    static boolean inicializado = false;

    // [Secao 4] Ultimo timestamp que este cliente conhece de cada key: key -> timestamp (comeca em 0)
    static HashMap<String, Long> timestampsPorKey = new HashMap<String, Long>();
    // Trava que protege timestampsPorKey (o menu e a thread de escuta podem acessar ao mesmo tempo)
    static final Object travaTimestamps = new Object();

    // [Secao 4] ServerSocket de escuta do cliente (porta escolhida pelo SO), onde chegam as respostas assincronas
    static ServerSocket servidorEscuta = null;
    // Porta do ServerSocket de escuta (vai em cada GET para o servidor saber onde responder depois)
    static int portaEscuta;

    // Sorteia qual servidor recebe cada requisicao
    static Random sorteio = new Random();

    public static void main(String[] args) {
        Scanner teclado = new Scanner(System.in);
        // [Secao 6] Menu interativo com somente INIT, PUT e GET (o cliente nunca sai)
        while (true) {
            System.out.println();
            System.out.println("Menu: INIT | PUT | GET");
            System.out.print("Escolha: ");
            if (!teclado.hasNextLine()) {
                return; // fim da entrada (so acontece com entrada por arquivo); a thread de escuta segue viva
            }
            String opcao = teclado.nextLine().trim().toUpperCase();
            try {
                if (opcao.equals("INIT")) {
                    init(teclado);
                } else if (!inicializado && (opcao.equals("PUT") || opcao.equals("GET"))) {
                    System.out.println("Fa\u00e7a o INIT primeiro.");
                } else if (opcao.equals("PUT")) {
                    put(teclado);
                } else if (opcao.equals("GET")) {
                    get(teclado);
                } else {
                    System.out.println("Op\u00e7\u00e3o inv\u00e1lida.");
                }
            } catch (NumberFormatException e) {
                System.out.println("Porta inv\u00e1lida.");
            }
        }
    }

    // [Secao 4] Inicializacao: le do teclado IP e porta dos tres servidores
    // e abre o ServerSocket de escuta (uma unica vez) numa ThreadEscuta
    static void init(Scanner teclado) {
        for (int i = 0; i < 3; i++) {
            System.out.print("IP do servidor " + (i + 1) + ": ");
            ipsServidores[i] = teclado.nextLine().trim();
            System.out.print("Porta do servidor " + (i + 1) + ": ");
            portasServidores[i] = Integer.parseInt(teclado.nextLine().trim());
        }
        inicializado = true;

        if (servidorEscuta == null) {
            try {
                servidorEscuta = new ServerSocket(0);
                portaEscuta = servidorEscuta.getLocalPort();
                new ThreadEscuta(servidorEscuta).start();
            } catch (IOException e) {
                System.out.println("N\u00e3o foi poss\u00edvel abrir a porta de escuta.");
            }
        }
    }

    // [Secao 4] Envio do PUT: le key e value do teclado e envia a um servidor aleatorio;
    // recebe o PUT_OK com o timestamp da key
    static void put(Scanner teclado) {
        System.out.print("Key: ");
        String key = teclado.nextLine().trim();
        System.out.print("Value: ");
        String value = teclado.nextLine().trim();

        Mensagem pedido = new Mensagem();
        pedido.setTipo(Mensagem.PUT);
        pedido.setKey(key);
        pedido.setValue(value);

        Mensagem resposta = enviarParaAlgumServidor(pedido);
        if (resposta != null && Mensagem.PUT_OK.equals(resposta.getTipo())) {
            atualizarTimestamp(resposta.getKey(), resposta.getTimestamp());
            // [Secao 6] Print do PUT_OK (servidor = o que este cliente contatou)
            System.out.println("PUT_OK key: " + resposta.getKey() + " value " + resposta.getValue()
                    + " timestamp " + resposta.getTimestamp() + " realizada no servidor "
                    + resposta.getIpServidor() + ":" + resposta.getPortaServidor());
        }
    }

    // [Secao 4] Envio do GET: le a key do teclado e envia a um servidor aleatorio,
    // junto com o ultimo timestamp conhecido dessa key (nao e lido do teclado)
    static void get(Scanner teclado) {
        System.out.print("Key: ");
        String key = teclado.nextLine().trim();

        long meuTimestamp = 0;
        synchronized (travaTimestamps) {
            if (timestampsPorKey.containsKey(key)) {
                meuTimestamp = timestampsPorKey.get(key);
            }
        }

        Mensagem pedido = new Mensagem();
        pedido.setTipo(Mensagem.GET);
        pedido.setKey(key);
        pedido.setTimestampCliente(meuTimestamp);

        Mensagem resposta = enviarParaAlgumServidor(pedido);
        if (resposta == null) {
            return;
        }
        String servidor = resposta.getIpServidor() + ":" + resposta.getPortaServidor();

        if (Mensagem.GET_RESPONSE.equals(resposta.getTipo())) {
            // [Secao 4] Resposta imediata do GET
            tratarRespostaGet(resposta);
        } else if (Mensagem.WAIT_FOR_RESPONSE.equals(resposta.getTipo())) {
            // [Secao 4] O servidor ainda nao tem a key atualizada: a conexao ja foi encerrada e o value
            // chegara depois, de forma assincrona, na ThreadEscuta. O cliente volta ao menu.
            System.out.println("GET key: " + key + " WAIT_FOR_RESPONSE do servidor " + servidor
                    + ", aguardando atualiza\u00e7\u00e3o");
        }
    }

    // [Secao 4] Guarda o timestamp recebido para a key, mantendo sempre o MAIOR ja visto
    // (o cliente nunca volta a um timestamp anterior)
    static void atualizarTimestamp(String key, long timestampRecebido) {
        synchronized (travaTimestamps) {
            long atual = 0;
            if (timestampsPorKey.containsKey(key)) {
                atual = timestampsPorKey.get(key);
            }
            if (timestampRecebido > atual) {
                timestampsPorKey.put(key, timestampRecebido);
            }
        }
    }

    // [Secao 4] Escolha aleatoria do servidor: sorteia o servidor inicial e, se ele nao responder,
    // tenta os seguintes em rodizio (inicio, inicio+1, inicio+2, voltando ao 0 depois do 2).
    // Na resposta, anota qual servidor foi contatado (ipServidor/portaServidor).
    // Devolve null se nenhum dos 3 responder.
    static Mensagem enviarParaAlgumServidor(Mensagem pedido) {
        // Indice (em ipsServidores/portasServidores) do servidor sorteado para a primeira tentativa
        int inicio = sorteio.nextInt(3);

        for (int k = 0; k < 3; k++) {
            int i = (inicio + k) % 3;
            try {
                Socket s = new Socket();
                s.connect(new InetSocketAddress(ipsServidores[i], portasServidores[i]), TIMEOUT_CONEXAO_MS);
                InputStreamReader is = new InputStreamReader(s.getInputStream(), "UTF-8");
                BufferedReader reader = new BufferedReader(is);
                OutputStream os = s.getOutputStream();
                DataOutputStream writer = new DataOutputStream(os);

                pedido.setIpCliente(s.getLocalAddress().getHostAddress());
                pedido.setPortaCliente(portaEscuta);
                Mensagem.enviar(pedido, writer);
                Mensagem resposta = Mensagem.receber(reader);
                s.close();
                if (resposta != null) {
                    // Anota o servidor que este cliente contatou (e o que aparece nos prints)
                    resposta.setIpServidor(ipsServidores[i]);
                    resposta.setPortaServidor(portasServidores[i]);
                    return resposta;
                }
            } catch (IOException e) {
                // servidor nao respondeu: tenta o proximo do rodizio
            }
        }
        System.out.println("N\u00e3o foi poss\u00edvel contatar o servidor");
        return null;
    }

    // [Secao 4] Trata um GET_RESPONSE, tanto a resposta imediata quanto a assincrona (apos WAIT_FOR_RESPONSE):
    // guarda o maior timestamp e imprime a linha da Secao 6. Os dados vem da Mensagem: Tx original
    // (timestampCliente), timestamp do servidor e IP/porta do servidor que respondeu.
    static void tratarRespostaGet(Mensagem resposta) {
        atualizarTimestamp(resposta.getKey(), resposta.getTimestamp());
        // [Secao 6] Print da resposta do GET
        System.out.println("GET key: " + resposta.getKey() + " value: " + resposta.getValue()
                + " obtido do servidor " + resposta.getIpServidor() + ":" + resposta.getPortaServidor()
                + ", meu timestamp " + resposta.getTimestampCliente()
                + " e do servidor " + resposta.getTimestamp());
    }

    // [Secao 4] Thread de escuta: recebe, de forma assincrona e orientada a eventos, o value que o
    // servidor envia depois de um WAIT_FOR_RESPONSE. Cada envio chega numa conexao nova, aberta pelo servidor.
    static class ThreadEscuta extends Thread {
        // ServerSocket de escuta deste cliente (o mesmo de servidorEscuta)
        private ServerSocket servidor = null;

        public ThreadEscuta(ServerSocket serverSocket) {
            servidor = serverSocket;
        }

        public void run() {
            while (true) {
                try {
                    Socket no = servidor.accept();
                    InputStreamReader is = new InputStreamReader(no.getInputStream(), "UTF-8");
                    BufferedReader reader = new BufferedReader(is);
                    Mensagem resposta = Mensagem.receber(reader);
                    no.close();
                    if (resposta != null && Mensagem.GET_RESPONSE.equals(resposta.getTipo())) {
                        // A resposta chega enquanto o menu espera: quebra de linha antes (para nao colar
                        // no prompt) e o prompt do menu reimpresso depois
                        System.out.println();
                        tratarRespostaGet(resposta);
                        System.out.print("Escolha: ");
                    }
                } catch (Exception e) {
                }
            }
        }
    }
}
