# Projeto KV Store - Sistemas Distribuídos (UFABC)

Sistema chave-valor replicado e consistente (estilo Zookeeper simplificado): 3 servidores (1 líder fixo) e N clientes, comunicando-se por TCP. Toda mensagem trocada é um objeto `Mensagem` serializado em JSON com Gson.

## Arquivos

- `Servidor.java`: servidor (líder ou não líder)
- `Cliente.java`: cliente com menu `INIT`, `PUT` e `GET`
- `Mensagem.java`: mensagem trocada entre clientes e servidores
- `gson-2.10.1.jar`: biblioteca Gson (necessária para compilar e executar)

## Requisitos

- JDK 1.8 (`javac -version` deve mostrar `1.8.x`)

## Compilação

Na pasta dos arquivos:

```
javac -cp gson-2.10.1.jar *.java
```

## Execução (Windows, CMD)

Abra 5 consoles na pasta dos arquivos: 3 servidores e 2 clientes.

**Servidores** (um por console):

```
java -cp .;gson-2.10.1.jar Servidor
```

Cada servidor pede seu IP e porta e o IP e porta do líder. O servidor cuja porta coincide com a do líder é o líder. Exemplo com o líder na porta 10097:

| Console | IP deste servidor | Porta deste servidor | IP do líder | Porta do líder |
|---|---|---|---|---|
| 1 (líder) | 127.0.0.1 | 10097 | 127.0.0.1 | 10097 |
| 2 | 127.0.0.1 | 10098 | 127.0.0.1 | 10097 |
| 3 | 127.0.0.1 | 10099 | 127.0.0.1 | 10097 |

**Clientes** (um por console):

```
java -cp .;gson-2.10.1.jar Cliente
```

No menu:

- `INIT`: informa IP e porta dos 3 servidores (`127.0.0.1` e `10097`, `10098`, `10099`).
- `PUT`: informa key e value. A requisição vai para um servidor aleatório.
- `GET`: informa a key. O cliente envia junto o último timestamp que conhece para ela.

> No Linux/macOS, troque o separador do classpath por `:` (`java -cp .:gson-2.10.1.jar Servidor`).

## Funcionamento resumido

- **PUT**: um servidor não líder encaminha ao líder. O líder grava a key com um novo timestamp, replica para os outros servidores e responde `PUT_OK` só depois de receber `REPLICATION_OK` de todos.
- **GET**: atendido pelo servidor contatado. Se o timestamp local da key for menor que o do cliente, o servidor responde `WAIT_FOR_RESPONSE` e entrega o valor depois, de forma assíncrona, quando a versão atualizada chegar.
- **Queda e retorno**: um servidor não líder que cair e voltar recupera todos os dados do líder e imprime `Informações recebidas do líder`.
- Os não líderes esperam 15 s antes de aplicar cada replicação (`ATRASO_REPLICACAO_MS` em `Servidor.java`), para permitir demonstrar o `WAIT_FOR_RESPONSE`.

Evite acentos nas keys e values digitados: o console do Windows pode trocar esses caracteres.
