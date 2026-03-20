# Migrador Futura 🚀
Migrador de Dados de Sistemas Delphi (Firebird) para Aplicação Web (MySQL/Python).

## Visão Geral
Aplicativo de conversão e extração de dados `Desktop > Web`, criado em Python com `CustomTkinter`. Conecta-se via Driver Nativo em bases de dados locais legadas (`.fdb` - Firebird) e transporta massas de dados de forma automatizada para um banco de dados moderno (`MySQL 8`).

---

## Interface e Design 🎨
* **Super Hub de Configuração**: Toda a parametrização do sistema (Bancos, Loja, Empresa e Mapeamentos) centralizada em uma janela elegante com Menu Lateral e Abas.
* **Componentes Premium**: Paleta de cores harmoniosa, cantos arredondados e tipografia clara.
* **UX de Fluxo Contínuo (Master-Detail)**: A tela de mapeamento elimina janelas modais secundárias mantendo a listagem e busca em tela dividida, com avanço automático da seleção.
* **Feedback Interativo**:
  - **Spinner de Animação** (⠋⠙⠹): Indicador visual durante extração de dados.
  - **Barra de Progresso Determinada**: Exibe `X / total` e percentual real (0–100%) durante a gravação.

---

## Funcionalidades 🌟

### Parâmetros Globais (Hub de Configurações)
Toda a configuração antes feita solta no painel foi agrupada e otimizada:
| Campo / Aba | Descrição |
|---|---|
| 🛢️ **Bancos** | Configuração de credenciais Firebird e MySQL com salvamento local no arquivo `.env`. |
| ⚙️ **Parâmetros** | **ID Loja** (destino) e **Seletor de Empresa** (Firebird - auto-populado após testar conexão). |
| 💳 **Mapeamento de Pagamentos** | Ferramenta visual de de/para: Associa cada forma de pagamento do banco origem ao ID do `Plano` no banco destino. Salva as escolhas em JSON local. |

### Módulos de Migração
| Botão | Tabelas Destino | Status |
|---|---|---|
| 👥 **Migrar Cliente** | `cliente` | ✅ Funcional |
| 📦 **Migrar Produtos** | `produto` | ✅ Funcional |
| 🏢 **Migrar Estoque** | `estoque` (SKUs) | ✅ Funcional |
| 🛒 **Migrar Venda** | `venda` + `item` + `parcela` | ✅ Funcional |
| ❌ **Truncate** | Limpa as tabelas antes. | ✅ Funcional |

Cada módulo possui checkbox **"Truncar destino antes"** para limpar as tabelas destino antes de inserir.

### Migração de Vendas + Itens (1-a-1)
A migração de vendas usa a abordagem **1-a-1 com pré-carregamento**:

1. Extrai todos os cabeçalhos de venda (`PEDIDO`) em uma query.
2. Pré-carrega **todos os itens** (`PEDIDO_ITEM`) da empresa em **uma única query Firebird**, agrupando em memória por `FK_PEDIDO` — elimina ~100 mil round-trips individuais.
3. Para cada venda: `INSERT venda` → captura `lastrowid` → `executemany(itens)` → `COMMIT` em lote a cada 500 vendas.
4. Otimizações de sessão MySQL ativadas durante o bulk insert (`unique_checks = 0`, `foreign_key_checks = 0`).

### Log em Arquivo
Cada migração gera automaticamente um arquivo de log em `logs/`:
- `logs/clientes.log` — log da última migração de clientes
- `logs/vendas.log` — log da última migração de vendas

Todos os eventos (início, progresso, erros, conclusão) são gravados com timestamp `[HH:MM:SS]`.

### Seleção de Empresa (Migração de Vendas)
Ao testar as conexões com sucesso, o sistema consulta automaticamente o Firebird e lista todas as empresas (`CHK_EMPRESA = 'S'`) com suas respectivas quantidades de vendas. O filtro `FK_EMPRESA` é aplicado no SELECT do Firebird.

---

## Arquitetura dos Arquivos
```
Migrador_Futura_py/
├── main.py               # Interface principal e UI CustomTkinter
├── core.py               # Classes base de conexão Firebird e MySQL
├── migrador_clientes.py  # Migrador de Clientes
├── migrador_produtos.py  # Migrador de Produtos
├── migrador_estoque.py   # Migrador de Estoque e SKUs
├── migrador_vendas.py    # Orquestrador de Vendas
├── migrador_itens.py     # Migrador de Itens de Venda (Sub-Módulo)
├── migrador_parcelas.py  # Migrador de Parcelas (Sub-Módulo com Mapeamento Múltiplo)
├── mapping_pagamento.json# Arquivo gerado via UI com as escolhas de mapeamento
├── requirements.txt      # Dependências Python
├── .env                  # Credenciais (ignorado pelo Git)
└── logs/                 # Logs de migração (ignorado pelo Git)
```

---

## Requisitos de Sistema 🛠️
- **Windows**: Recomendado para compatibilidade total com diálogos nativos.
- **Python 3.10+**: Obrigatório (uso de type hints modernos).
- **Firebird Client**: `fbclient.dll` acessível no sistema.

---

## Instalação e Execução

```bash
# 1. Clone o repositório
git clone <URL_DO_REPOSITORIO>

# 2. Crie e ative o ambiente virtual
python -m venv venv
venv\Scripts\activate

# 3. Instale as dependências
pip install -r requirements.txt

# 4. Inicie a aplicação
python main.py
```

*Dependências principais: `fdb`, `mysql-connector-python`, `customtkinter`, `python-dotenv`, `loguru`*

---

## Como Usar

1. **Configurar**: Clique em **⚙️ Configurar Bancos**, preencha as conexões e clique em **Testar Conexões**. As empresas do Firebird serão carregadas automaticamente no seletor.
2. **ID Loja**: Preencha o campo **🏪 ID Loja** (obrigatório).
3. **Empresa**: Selecione a empresa do Firebird no dropdown **🏢 Empresa**.
4. **Migrar**: Marque "Truncar antes" se necessário e clique no módulo desejado.
5. **Progresso**: Acompanhe a barra no rodapé — spinner durante extração, percentual + contador `X / total` durante inserção.
6. **Logs**: Acompanhe em tempo real no painel à direita. O arquivo de log é salvo automaticamente em `logs/`.

---

## Segurança 🔒
- Credenciais armazenadas **localmente** em `.env` — nunca versionadas.
- Pasta `logs/` ignorada pelo Git.
- Nunca compartilhe seu arquivo `.env` publicamente.
