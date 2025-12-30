# 📊 Painel de Controle de Criptoativos (Pipeline de Engenharia de Dados)

> **Status:** ✅ Concluído e Automatizado

## 🎯 Objetivo do Projeto
Desenvolver uma solução completa de Inteligência de Negócios (End-to-End) para monitoramento de ativos digitais, resolvendo o problema de conversão cambial (Dólar/Real) e histórico de preços em nuvem. O projeto simula um ambiente corporativo real, focando em robustez, automação e governança de dados.

## 🏗️ Arquitetura da Solução
A solução foi desenhada seguindo a arquitetura moderna de ELT/ETL:

1.  **Ingestão (Extract):** Scripts Python executados via **GitHub Actions** (CI/CD) coletam dados de APIs públicas (CoinGecko, Binance Vision).
2.  **Processamento (Transform):** Tratamento de dados com **Pandas** e conversão de tipos.
3.  **Armazenamento (Load):** Persistência dos dados no **Azure SQL Database** (Nuvem).
4.  **Modelagem (Serve):** Criação de Views SQL (`vw_Dashboard_Completa`) para cálculos de janela (`LAG`) e conversão cambial.
5.  **Visualização:** Conexão com **Power BI** para análise final.

## 🛠️ Tecnologias Utilizadas
* **Linguagem:** Python 3.9 (Libs: `pandas`, `sqlalchemy`, `requests`, `pyodbc`)
* **Cloud:** Microsoft Azure SQL Database
* **Orquestração:** GitHub Actions (Cron Jobs Diários)
* **Banco de Dados:** T-SQL (Views, Window Functions, CTEs)
* **Controle de Versão:** Git

## 🛡️ Desafios de Engenharia Superados

Durante o desenvolvimento, enfrentei e resolvi problemas comuns em cenários de Big Data e Cloud:

### 1. Resiliência de Rede e DNS
* **Problema:** Instabilidades na conexão do GitHub Actions causavam falhas de DNS (`NameResolutionError`).
* **Solução:** Implementação de um objeto `Session` com `HTTPAdapter` e estratégia de *Exponential Backoff* (Retentativas automáticas com espera progressiva).

### 2. Geofencing (Bloqueio Geográfico)
* **Problema:** A API padrão da Binance bloqueia IPs de Data Centers (Azure/GitHub) nos EUA.
* **Solução:** Desenvolvimento de uma lógica de **Fallback**. O sistema tenta extrair da **CoinGecko**; se falhar, alterna automaticamente para a **Binance Vision** (endpoint público), garantindo alta disponibilidade.

### 3. Segurança e Governança
* Uso de **GitHub Secrets** para ocultar credenciais do banco.
* Conexão criptografada com o Azure (`Encrypt=yes` e `TrustServerCertificate=yes`) via ODBC Driver 18.

## 🚀 Como Executar Localmente

1.  Clone o repositório:
    ```bash
    git clone [https://github.com/SeuUsuario/Cryptocurrency-Control-Panel.git](https://github.com/SeuUsuario/Cryptocurrency-Control-Panel.git)
    ```
2.  Instale as dependências:
    ```bash
    pip install -r requirements.txt
    ```
3.  Configure as variáveis de ambiente (`DB_SERVER`, `DB_USER`, etc.) e execute:
    ```bash
    python etl_binance.py
    ```

---
*Desenvolvido por Lucas do Nascimento Silva - Analista de BI & Futuro Engenheiro de Dados*