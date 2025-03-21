package com.senai;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Collectors;

public class ServidorHTTP {

    private final String caminhoRaizWebapp;  // raizWebapp é uma variável de instância
    private HttpServer server;

    // Construtor que inicializa o servidor com a raiz do WebApp
    public ServidorHTTP(String raizWebapp) {
        this.caminhoRaizWebapp = raizWebapp;
    }

    // Método que inicializa o servidor HTTP
    public void iniciarServidorHTTP() {
        try {
            server = HttpServer.create(new InetSocketAddress(8000), 0);
            server.createContext("/", new HomeHandler());
            server.createContext("/atualizacao", new AtualizacaoHandler());
            server.createContext("/cadastros", new CadastroListHandler());
            server.createContext("/cadastro", new CadastroHandler());
            server.createContext("/iniciarRegistroTag", new IniciarRegistroTagHandler());
            server.createContext("/verificarStatusTag", new VerificarStatusTagHandler());
            // Rota para servir imagens da pasta src/main/resources/imagens/
            server.createContext("/imagens", new ImageHandler());

            server.setExecutor(null); // Utiliza o executor padrão
            server.start();
            System.out.println("Servidor HTTP iniciado na porta 8000");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Método para parar o servidor
    public void pararServidorHTTP() {
        if (server != null) {
            server.stop(0);
            System.out.println("Servidor HTTP parado.");
        }
    }

    // Handler para servir arquivos HTML, CSS e JS do diretório especificado
    private class HomeHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String caminhoRequisitado = exchange.getRequestURI().getPath();
            File arquivo;

            // Serve o index.html para qualquer rota não específica
            if ("/".equals(caminhoRequisitado) || caminhoRequisitado.startsWith("/index.html")) {
                arquivo = new File(caminhoRaizWebapp + "/index.html");
            } else {
                arquivo = new File(caminhoRaizWebapp + caminhoRequisitado);
            }

            // Verifica se o arquivo existe e é legível
            byte[] bytesResposta;
            if (arquivo.exists() && arquivo.isFile()) {
                String mimeType = Files.probeContentType(Paths.get(arquivo.getAbsolutePath()));
                exchange.getResponseHeaders().set("Content-Type", mimeType);
                bytesResposta = Files.readAllBytes(arquivo.toPath());
            } else {
                // Se o arquivo não for encontrado, redireciona para o index.html
                File indexFile = new File(caminhoRaizWebapp + "/index.html");
                bytesResposta = Files.readAllBytes(indexFile.toPath());
                exchange.getResponseHeaders().set("Content-Type", "text/html");
            }
            exchange.sendResponseHeaders(200, bytesResposta.length);
            OutputStream os = exchange.getResponseBody();
            os.write(bytesResposta);
            os.close();
        }
    }

    // Handler para a rota "/atualizacao"
    private class AtualizacaoHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            String jsonResponse = ControleDeAcesso.matrizRegistrosDeAcesso.length == 0
                    ? "[]"
                    : "[" +
                    Arrays.stream(ControleDeAcesso.matrizRegistrosDeAcesso)
                            .map(registro -> String.format("{\"nome\":\"%s\",\"horario\":\"%s\",\"imagem\":\"%s\"}", registro[0], registro[1],registro[2]))
                            .collect(Collectors.joining(",")) +
                    "]";
            byte[] bytesResposta = jsonResponse.getBytes();
            exchange.sendResponseHeaders(200, bytesResposta.length);
            OutputStream os = exchange.getResponseBody();
            os.write(bytesResposta);
            os.close();
        }
    }

    // Handler para listar todos os cadastros
    private class CadastroListHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            JSONArray jsonArray = new JSONArray();

            // Percorre a matrizCadastro a partir da segunda linha (ignora cabeçalho)
            for (int i = 1; i < ControleDeAcesso.matrizCadastro.length; i++) {
                String[] registro = ControleDeAcesso.matrizCadastro[i];
                if (registro != null) { // Verifica se a linha está preenchida
                    JSONObject json = new JSONObject();
                    json.put("id", registro[0]);
                    json.put("idAcesso", (registro[1] != null && !registro[1].isEmpty()) ? registro[1] : "-");
                    json.put("nome", registro[2]);
                    json.put("telefone", registro[3]);
                    json.put("email", registro[4]);
                    json.put("imagem", registro[5]) ;
                    jsonArray.put(json);
                }
            }

            // Envia a resposta como JSON
            byte[] response = jsonArray.toString().getBytes();
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        }
    }


    // Handler para cadastrar um novo usuário
    private class CadastroHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                InputStreamReader inputStreamReader = new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                StringBuilder corpoDaRequisicao = new StringBuilder();
                String linha;
                while ((linha = bufferedReader.readLine()) != null) {
                    corpoDaRequisicao.append(linha);
                }

                // Gera novo ID e cria o registro
                int novoID = ControleDeAcesso.matrizCadastro.length;

                JSONObject json = new JSONObject(corpoDaRequisicao.toString());
                String nome = json.getString("nome");
                String telefone = json.getString("telefone");
                String email = json.getString("email");
                String imagem = salvarImagem(json.getString("imagem"), nome, novoID);
                //Logs
                System.out.println("nome : " + nome + " | telefone : " + telefone + " | email : " + email);

                String[] novoUsuario = {String.valueOf(novoID), "-", nome, telefone, email, imagem};
                String[][] novaMatriz = new String[novoID + 1][novoUsuario[0].length()];

                for (int linhas = 0; linhas < ControleDeAcesso.matrizCadastro.length; linhas++) {
                    novaMatriz[linhas] = Arrays.copyOf(ControleDeAcesso.matrizCadastro[linhas], ControleDeAcesso.matrizCadastro[linhas].length);
                }

                novaMatriz[novoID] = novoUsuario;
                ControleDeAcesso.matrizCadastro = novaMatriz;
                ControleDeAcesso.salvarDadosNoArquivo();

                String responseMessage = "Cadastro recebido com sucesso!";
                exchange.sendResponseHeaders(200, responseMessage.length());
                exchange.getResponseBody().write(responseMessage.getBytes());
                exchange.close();
            } else {
                exchange.sendResponseHeaders(405, -1); // Método não permitido
            }
        }
    }

    private String salvarImagem(String imagemBase64, String nome, int id) throws IOException {
        byte[] dados = Base64.getDecoder().decode(imagemBase64);
        String nomeImagem =  id + nome + ".png";
        try (FileOutputStream fos = new FileOutputStream("src\\main\\resources\\imagens\\" +nomeImagem)) {
            fos.write(dados);
        }
        return nomeImagem;
    }

    // Classe para lidar com requisições de imagens
    private class ImageHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String imagePath = "src\\main\\resources\\imagens" + exchange.getRequestURI().getPath().replace("/imagens", "");
            File imageFile = new File(imagePath);

            if (imageFile.exists() && !imageFile.isDirectory()) {
                // Define o tipo de conteúdo como imagem (ajusta conforme o tipo de imagem que está servindo)
                exchange.getResponseHeaders().set("Content-Type", Files.probeContentType(Paths.get(imagePath)));
                exchange.sendResponseHeaders(200, imageFile.length());

                // Envia a imagem ao cliente
                try (OutputStream os = exchange.getResponseBody();
                     FileInputStream fs = new FileInputStream(imageFile)) {
                    fs.transferTo(os);
                }
            } else {
                exchange.sendResponseHeaders(404, -1); // 404 caso a imagem não seja encontrada
            }
            exchange.close();
        }
    }
    private class IniciarRegistroTagHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                // Receber o ID do usuário e o dispositivo que solicitou o registro
                String requestBody = new BufferedReader(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))
                        .lines().collect(Collectors.joining());
                JSONObject json = new JSONObject(requestBody);
                int usuarioId = json.getInt("usuarioId");
                String dispositivo = json.getString("dispositivo"); // Novo campo do dispositivo

                // Iniciar o processo de registro da tag
                ControleDeAcesso.idUsuarioRecebidoPorHTTP = usuarioId;
                ControleDeAcesso.modoCadastrarIdAcesso = true;

                // Publicar no broker qual dispositivo foi habilitado
                ControleDeAcesso.conexaoMQTT.publicarMensagem("cadastro/disp", dispositivo);

                // Criação da resposta JSON
                String response = new JSONObject()
                        .put("mensagem", "Registro de tag iniciado para o usuário " + usuarioId + " no " + dispositivo)
                        .toString();

                // Envio da resposta com cabeçalho de conteúdo JSON
                exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
                exchange.sendResponseHeaders(200, response.getBytes().length);

                // Escrita e fechamento do corpo da resposta
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes(StandardCharsets.UTF_8));
                }
                exchange.close();
            }
        }
    }

    private class VerificarStatusTagHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            int usuarioId = Integer.parseInt(exchange.getRequestURI().getPath().split("/")[2]);
            String status = ControleDeAcesso.matrizCadastro[usuarioId][1].equals("-") ? "aguardando" : "sucesso";

            String response = "{\"status\":\"" + status + "\"}";
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length());
            exchange.getResponseBody().write(response.getBytes());
            exchange.close();
        }
    }
}
