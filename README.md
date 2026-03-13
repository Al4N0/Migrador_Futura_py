# Migrador Futura 🚀
Migrador de Dados de Sistemas Delphi (Firebird) para Aplicação Web (MySQL/Python).

## Visão Geral
Este é um aplicativo de conversão e extração de dados `Desktop > Web`, criado em Python utilizando `CustomTkinter` para apresentar uma interface amigável. Ele se conecta via Driver Nativo em bases de dados locais legadas (`.fdb` - Firebird) e transporta massas de dados de forma automatizada para um banco de dados destino Moderno (`MySQL 8`), permitindo filtros, tratamento de colisão de Chave Primária, otimização de queries pesadas e injeção assíncrona com `REPLACE INTO`.

## Funcionalidades Principais 🌟
* **Painel Interativo de Configuração**: Menu centralizado para selecionar o arquivo local `.fdb` e definir as credenciais do MySQL.
* **Auto-Save via `.env`**: Suas configurações são guardadas imediatamente ao testar uma conexão bem-sucedida, não precisa reescrevê-las ao fechar e abrir o sistema.
* **Validador de Encoding Nativo**: Compatibilidade com os tipos de Strings `WIN1252`/`ANSI` muito usados em bancos locais Delphi no Brasil (evita que acentuação quebre no Python e no DB destino).
* **Migração de Clientes e Anti-Colisão**: A primeira vertente ("Migrar Clientes", módulo em `migrador_clientes.py`) puxa a tabela de Clientes (com INNER/LEFT JOINs resolvendo cidades, endereços, filiais). Inclui lógica que converte "Duplicatas de CPF/CNPJ" na origem para não serem sobregravadas no destino.
* **Opção de Limpar (Truncate)**: Escolha visualmente se deseja limpar os registros antigos do MySQL antes de re-importar as entidades da origem.

## Requisitos de Sistema 🛠️
- **Windows**: Obrigatório para encontrar as chaves corretas e interagir via Open File Dialog com bancos `C:/*`.
- **Python 3.10+**: Com a extensão ou driver base nativa pro Firebird.
- (A Instalação pede a biblioteca `fdb` do Firebird 2.5/3.0. Para o Windows, garante que a DLL `fbclient.dll` esteja na máquina e consertada).

## Instalação e Execução

1. Baixe o repositório ou faça o clone:
```bash
git clone <URL_DO_REPOSITORIO>
```
2. Instale as dependências:
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```
   *(Dependências inclusas: `fdb`, `mysql-connector-python`, `customtkinter`, `python-dotenv`, `loguru`)*

3. Inicie a Interface Principal:
```bash
python main.py
```

## Como Usar o Migrador
1. Ao Abrir a janela, perceba que o Botão **Migrar** está Desabilitado. Ele tranca o processo para prevenir acidentes.
2. Clique em **Configurar Bancos** na Engrenagem `⚙️`.
3. Escolha o banco Firebird (`Procurar...`) e defina os IPs (geralmente `localhost`), e preencha a base de dados MySQL (Ex: `migracao_futura`).
4. Aperte o Botão **Testar Conexões**. Se ficar verde, o painel se fecha e tudo fica configurado.
5. Na janela Principal, **Verifique** a Checkbox "*Limpar tabela antes (Truncate)*" se os registros desejados no MySQL devem sofrer Flush Total antes da conversão.
6. Clique no botão de Ação da sua entidade escolhida (ex: `🚀 Migrar Clientes`). A Migração ocorrerá em Background 🧵 (*Multi-Thread*) enquanto os logs exibem estatísticas detalhadas do processamento ao vivo!

## Segurança 🔒
Credenciais **jamais** devem ser comitadas no GitHub. O arquivo `.env` é gerado automaticamente na raiz da pasta _apenas localmente_ e ele já está adicionado na regra do arquivo `.gitignore`. Em produção, este projeto lê diretamente as chaves de ambiente lá contidas.
