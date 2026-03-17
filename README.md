# Migrador Futura 🚀
Migrador de Dados de Sistemas Delphi (Firebird) para Aplicação Web (MySQL/Python).

## Visão Geral
Este é um aplicativo de conversão e extração de dados `Desktop > Web`, criado em Python utilizando `CustomTkinter` para apresentar uma interface premium inspirada em layouts Delphi robustos. Ele se conecta via Driver Nativo em bases de dados locais legadas (`.fdb` - Firebird) e transporta massas de dados de forma automatizada para um banco de dados destino Moderno (`MySQL 8`).

## Interface e Design 🎨
A interface foi redesenhada para oferecer uma experiência de usuário profissional e funcional:
* **Sidebar Escura (Estilo ERP)**: Painel lateral focado em ações e parâmetros de controle.
* **Componentes Premium**: Uso de cores harmoniosas (`#2B2B40`), cantos arredondados e tipografia clara para redução de fadiga visual.
* **Feedback Interativo**: 
    - **Spinner de Animação**: Indicador visual (⠋⠙⠹) que mostra que o banco de dados está processando.
    - **Progresso Dinâmico**: Barra de status que alterna entre modo indeterminado (durante extração) e percentual (durante gravação).

## Funcionalidades Principais 🌟
* **ID Loja Obrigatório**: Garantia de integridade dos dados através da validação mandatória do número da loja antes de qualquer operação.
* **Módulos de Migração Segregados**: Migração de Clientes (funcional), Produtos e Vendas (em desenvolvimento).
* **Parâmetros de Truncate**: Opção para limpar as tabelas de destino individualmente ou via botão global.
* **Auto-Save via `.env`**: Configurações de conexão são salvas automaticamente após testes bem-sucedidos.
* **Conectividade Docker-Ready**: Configuração otimizada para bancos Firebird rodando em containers Docker ou instalações locais.

## Requisitos de Sistema 🛠️
- **Windows**: Recomendado para compatibilidade total com diálogos nativos.
- **Python 3.10+**: Necessário para execução do interpretador.
- **Firebird Client**: Certifique-se de ter a `fbclient.dll` acessível (geralmente instalada com o servidor ou client do Firebird).

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
*(Dependências principais: `fdb`, `mysql-connector-python`, `customtkinter`, `python-dotenv`, `loguru`)*

3. Inicie a Interface Principal:
```bash
python main.py
```

## Como Usar o Migrador
1. **Configuração**: Clique no botão **⚙️ Configurar Bancos** na sidebar. Informe os dados de conexão e clique em **Testar**. Se aprovado, os botões de migração serão liberados.
2. **ID Loja**: Preencha obrigatoriamente o campo **Nº da Loja** no topo da sidebar.
3. **Migração**: Selecione se deseja "Truncar antes" e clique no botão da entidade (ex: **👥 Migrar Cliente**). 
4. **Logs**: Acompanhe o processamento em tempo real no painel de log à direita.

## Segurança 🔒
Credenciais sensíveis são armazenadas localmente no arquivo `.env`, que é ignorado pelo Git através do `.gitignore`. Nunca compartilhe seu arquivo `.env` publicamente.
