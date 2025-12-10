import os
import re
import time
import json
import logging
import base64
import inspect
import requests
from typing import Optional, Tuple, Any

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("azure").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

class FabricAuditor:
    def __init__(self, llm_client: Optional[Any] = None, auto_install: bool = True):
        """
        Inicializa o FabricAuditor.
        
        Args:
            llm_client (optional): Um objeto cliente LLM instanciado. Se None, configura automaticamente.
            auto_install (bool): Se True, verifica e instala dependências ausentes automaticamente.
        """
        if auto_install:
            self._ensure_dependencies()

        if llm_client:
            self.llm_client = llm_client
        else:
            logger.info("Nenhum cliente LLM fornecido. Tentando configuração automática padrão...")
            self.llm_client = self._setup_default_client()
        
        # Padrões para ignorar (auto-exclusão)
        self.ignore_patterns = [
            "def snapshot_notebook_limpo",
            "snapshot_notebook_limpo()",
            "FabricAuditor",
            "audit_code",
            "summarize_notebook",
            "mssparkutils.credentials.getToken",
            "trident.workspace.id",
            "# AUDIT_IGNORE"  # Marcador manual para ignorar células
        ]

    def _ensure_dependencies(self):
        """Verifica e instala dependências críticas se estiverem faltando."""
        required_packages = [
            ("azure.identity", "azure-identity"),
            ("azure.keyvault.secrets", "azure-keyvault-secrets"),
            ("openai", "openai")
        ]
        
        missing = []
        for import_name, install_name in required_packages:
            try:
                __import__(import_name)
            except ImportError:
                missing.append(install_name)
        
        if missing:
            print(f"📦 Dependências ausentes detectadas: {', '.join(missing)}")
            print("⏳ Instalando automaticamente... (Isso pode levar alguns instantes)")
            try:
                import subprocess
                import sys
                subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing)
                print("✅ Instalação concluída! Nota: Se ocorrerem erros de importação, reinicie o kernel.")
            except Exception as e:
                logger.error(f"❌ Falha na instalação automática: {e}")

    def _setup_default_client(self) -> Any:
        """
        Configura o cliente AzureOpenAI padrão lendo do JSON e Key Vault.
        """
        try:
            import notebookutils
            from azure.identity import ClientSecretCredential
            from azure.keyvault.secrets import SecretClient
            from openai import AzureOpenAI
            
            # 1. Ler Credenciais do Arquivo
            # Verifica se notebookutils tem nbResPath (algumas versões podem variar)
            if not hasattr(notebookutils, 'nbResPath'):
                 # Tentativa de fallback para mssparkutils se necessário, ou erro mais claro
                 pass 

            json_path = f"{notebookutils.nbResPath}/env/CS_API_REST_LOGIN.json"
            if not os.path.exists(json_path):
                raise FileNotFoundError(f"Arquivo de configuração não encontrado em: {json_path}")

            with open(json_path, encoding='utf-8') as arquivo:
                certificate = json.load(arquivo)

            # 2. Pegar segredo do Key Vault
            key_vault_url = "https://kv-azureopenia.vault.azure.net/" 
            credential = ClientSecretCredential(
                tenant_id=certificate['tenant_id'],
                client_id=certificate['client_id'],
                client_secret=certificate['client_secret']
            )
            secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
            api_key = secret_client.get_secret('OPEN-AI-KEY').value

            # 3. Configurar Cliente
            print("⚙️ Configurando Azure OpenAI (Automático)...")
            return AzureOpenAI(
                azure_endpoint="https://datasciencellm.openai.azure.com/",
                api_key=api_key,
                api_version="2024-12-01-preview",
            )
            
        except ImportError as e:
            raise ImportError(f"Dependências ausentes ou erro de importação: {e}. Instale: azure-identity, azure-keyvault-secrets, openai")
        except Exception as e:
            raise RuntimeError(f"Falha na configuração automática do cliente LLM: {e}")

    def _get_fabric_context(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Auxiliar para obter o contexto do Fabric (token, workspace_id, notebook_id).
        Usa configurações do Spark para IDs, o que é mais robusto que correspondência por nome.
        """
        try:
            from notebookutils import mssparkutils
            from pyspark.sql import SparkSession
            
            token = mssparkutils.credentials.getToken("pbi")
            
            spark = SparkSession.getActiveSession()
            if not spark:
                logger.warning("Nenhuma sessão Spark ativa encontrada.")
                return None, None, None
                
            workspace_id = spark.conf.get("trident.workspace.id", None)
            notebook_id = spark.conf.get("trident.artifact.id", None)
            
            return token, workspace_id, notebook_id
        except ImportError:
            logger.warning("notebookutils ou pyspark não encontrados. Não está rodando no Fabric?")
            return None, None, None
        except Exception as e:
            logger.warning(f"Falha ao obter contexto do Fabric: {e}")
            return None, None, None

    def _extract_code_hybrid(self) -> str:
        """
        Extrai código usando uma estratégia à prova de falhas:
        1. Tenta API do Fabric (Estratégia A).
        2. Fallback para histórico do IPython (Estratégia B).
        """
        # Estratégia A: API
        code = self._extract_via_api()
        if code:
            logger.info("Código extraído com sucesso via API do Fabric.")
            return code
        
        # Estratégia B: Fallback de Memória
        logger.info("Recorrendo à extração via memória (Estratégia B).")
        return self._extract_via_memory()

    def _extract_via_api(self) -> Optional[str]:
        token, workspace_id, notebook_id = self._get_fabric_context()
        if not token or not workspace_id or not notebook_id:
            logger.warning("Contexto do Fabric ausente para extração via API.")
            return None

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        base_url = "https://api.fabric.microsoft.com/v1"
        
        try:
            # Acesso direto usando IDs da configuração do Spark
            def_url = f"{base_url}/workspaces/{workspace_id}/items/{notebook_id}/getDefinition"
            response = requests.post(def_url, headers=headers)
            
            definition_json = {}
            if response.status_code == 200:
                definition_json = response.json()
            elif response.status_code == 202:
                # Loop de polling
                operation_url = response.headers.get("Location") or response.headers.get("Operation-Location")
                retry_after = int(response.headers.get("Retry-After", 2))
                
                if operation_url:
                    for _ in range(10): 
                        time.sleep(retry_after)
                        poll_response = requests.get(operation_url, headers=headers)
                        if poll_response.status_code == 200:
                            definition_json = poll_response.json()
                            break
                        if poll_response.status_code != 202:
                            logger.error(f"Polling falhou: {poll_response.status_code}")
                            return None
            
            if not definition_json:
                logger.error(f"Falha ao obter definição. Status final: {response.status_code}")
                return None

            # Parse da Definição
            parts = definition_json.get('definition', {}).get('parts', [])
            full_code = []
            
            payload = None
            for p in parts:
                if 'ipynb' in p.get('path', '').lower():
                    payload = p.get('payload')
                    break
            if not payload and parts:
                payload = parts[0].get('payload')

            if payload:
                decoded = base64.b64decode(payload).decode('utf-8')
                nb_json = json.loads(decoded)
                
                for cell in nb_json.get('cells', []):
                    if cell.get('cell_type') == 'code':
                        source = "".join(cell.get('source', [])) if isinstance(cell.get('source'), list) else str(cell.get('source'))
                        
                        # Verificação de auto-exclusão
                        if any(pattern in source for pattern in self.ignore_patterns):
                            continue
                            
                        full_code.append(source)
            
            return "\n\n".join(full_code)

        except Exception as e:
            logger.error(f"Estratégia A falhou: {e}")
            return None

    def _extract_via_memory(self) -> str:
        """
        Recupera células executadas da lista global `In` do IPython usando inspect.
        """
        try:
            # Usa inspect para encontrar o frame do chamador que possui 'In' (histórico do IPython)
            frame = inspect.currentframe()
            history = None
            
            # Sobe na pilha para encontrar o escopo global do notebook
            while frame:
                if 'In' in frame.f_globals and isinstance(frame.f_globals['In'], list):
                    history = frame.f_globals['In']
                    break
                frame = frame.f_back
            
            if not history:
                # Fallback para __main__ se a caminhada na pilha falhar
                import __main__
                if hasattr(__main__, 'In'):
                    history = __main__.In

            if history:
                valid_cells = []
                for cell in history:
                    if isinstance(cell, str) and cell.strip():
                        # Verificação de auto-exclusão
                        if any(pattern in cell for pattern in self.ignore_patterns):
                            continue
                        valid_cells.append(cell)
                return "\n\n".join(valid_cells)
            else:
                logger.warning("Histórico 'In' não encontrado.")
                return ""
        except Exception as e:
            logger.error(f"Estratégia B falhou: {e}")
            return ""

    def _clean_noise(self, code_string: str) -> str:
        # 1. Remove cabeçalhos de Licença Apache (e potencialmente outros)
        code_string = re.sub(r'(?m)^#\s*Copyright.*(?:\n#.*)*', '', code_string)
        code_string = re.sub(r'(?m)^#\s*Licensed under.*(?:\n#.*)*', '', code_string)
        code_string = re.sub(r'(# Licensed to the Apache[\s\S]*?#\n)', '', code_string)
        
        # 2. Remove blocos init_spark
        pattern_spark = r'def init_spark\(\):[\s\S]*?del init_spark'
        code_string = re.sub(pattern_spark, '', code_string)

        # 3. Remove sc.setJobGroup / sc.setLocalProperty
        code_string = re.sub(r'sc\.setJobGroup\(.*?\)', '', code_string)
        code_string = re.sub(r'sc\.setLocalProperty\(.*?\)', '', code_string)
        code_string = re.sub(r'(sc\.setJobGroup[\s\S]*?sourceId", "default"\))', '', code_string)
        
        # 4. Remove imports e código de infraestrutura
        code_string = re.sub(r'(?m)^import notebookutils.*$', '', code_string)
        code_string = re.sub(r'(?m)^from notebookutils.*$', '', code_string)
        code_string = re.sub(r'(import notebookutils|from notebookutils.*|initializeLHContext.*|notebookutils\.prepare.*)', '', code_string)
        
        # 5. Remove comandos Mágicos
        code_string = re.sub(r'(?m)^%.*$', '', code_string)
        code_string = re.sub(r'(get_ipython\(\)\.run_line_magic.*)', '', code_string)
        
        # 6. Redige segredos (sk-...)
        code_string = re.sub(r'sk-[a-zA-Z0-9]{20,}', 'sk-***REDACTED***', code_string)
        
        # 7. Limpeza final
        code_string = re.sub(r'^[ \t]+$', '', code_string, flags=re.MULTILINE) # Remove linhas vazias com espaços
        code_string = re.sub(r'\n{3,}', '\n\n', code_string) # Compacta newlines excessivos
        
        return code_string.strip()

    def audit_code(self) -> str:
        raw_code = self._extract_code_hybrid()
        clean_code = self._clean_noise(raw_code)
        
        if not clean_code:
            return "Nenhum código encontrado para auditar."

        system_prompt = (
'''
# Role: Engenheiro de Dados Sênior (Microsoft Fabric/Synapse Auditor)

## Contexto e Objetivo
Você é a última barreira de qualidade antes de um código ir para produção. Sua tarefa é auditar notebooks PySpark projetados para rodar em pipelines orquestrados (Data Factory/Synapse Pipelines) de forma **100% autônoma**.

**Sua mentalidade:**
* **Cético:** Assuma que o código vai falhar silenciosamente se não for verificado.
* **Orientado a Custos:** Otimização de CU (Capacity Units) no Fabric é prioridade.
* **Segurança Zero Trust:** Nenhuma credencial deve estar exposta.

---

## 1. Diretrizes de Filtragem (Redução de Ruído)
**NÃO** aponte problemas nestes casos (salvo se causarem erro explícito):
* Imports padrão (`pyspark.sql.functions`, `types`, etc.), a menos que não utilizados.
* Configuração de sessão Spark (`spark = ...`), pois o Fabric gerencia isso, mas não é um erro crítico.
* Comentários de documentação (docstrings), a menos que revelem lógica insegura.

---

## 2. Regras de Auditoria (Checklist Rigoroso)

### A. Limpeza de Artefatos Interativos (Nível: BLOQUEANTE)
O código não pode conter comandos que exijam interação humana ou poluam os logs do driver.
* **Proibido:** `display()`, `df.show()`, `df.printSchema()`, `input()`.
* **Proibido:** Bibliotecas de plotagem (`matplotlib`, `seaborn`, `plotly`).
* **Restrito:** `print()` solto. (Sugerir substituição por `logging` ou remoção).

### B. Segurança e Governança (Nível: CRÍTICO)
* **Hardcoded Secrets:** Senhas, SAS Tokens, Access Keys ou Connection Strings explícitas.
    * *Solução Obrigatória:* Usar Azure Key Vault via `mssparkutils.credentials.getSecret()`.
* **Dados Sensíveis (PII):** Logs imprimindo dados de clientes (CPF, Email, etc.).

### C. Performance e Otimização Fabric (Nível: ALTO)
* **Schema Enforcement:** Ingestão de API/JSON/CSV sem `schema` definido (risco de inferência custosa e erro de tipo).
* **Delta Lake Best Practices:**
    * Uso de `MERGE` sem colunas de poda (partition pruning).
    * Falta de `OPTIMIZE` ou `VACUUM` em processos de escrita massiva.
    * Particionamento excessivo em tabelas pequenas (< 1GB).
* **Ações Coletoras:** Uso inseguro de `.collect()` ou `.toPandas()`.
    * *Regra:* Aceitável apenas para métricas de controle minúsculas. Se usado no dataset principal -> **Reprovar**.

### D. Estabilidade e Orquestração (Nível: ALTO)
* **Controle de Fluxo:** Loops `while` sem timeout ou `for` iterando sobre dados massivos (non-vectorized operations).
* **Retorno de Pipeline:** O notebook deve finalizar com `mssparkutils.notebook.exit()` para comunicar status ao orquestrador.
* **Caminhos:** Preferência por caminhos OneLake (`abfss://...`) em vez de montagens locais legadas.

### E. Qualidade de Código (Nível: MÉDIO/DICA)
* **Magic Numbers:** Números soltos na lógica sem explicação ou constante nomeada.
* **Nomenclatura:** Variáveis como `df1`, `temp`, `teste`.
* **Tratamento de Erros:** Blocos `try/except` vazios ou genéricos (`except Exception: pass`).

---

## 3. Formato de Saída Obrigatório

Para cada problema encontrado, gere um bloco no seguinte padrão Markdown:

### 🔴 [BLOQUEANTE / CRÍTICO] ou 🟡 [ALTO] ou 🔵 [DICA]
**Trecho/Linha:** `Código ou número da linha`
**Violação:** Explique qual regra foi quebrada e o impacto (ex: "Isso fará o log do driver estourar em produção").
**Correção Sugerida:**
```python
# Exemplo de como o código deveria ser
'''
)
        
        return self._call_llm(system_prompt, clean_code)

    def summarize_notebook(self) -> str:
        raw_code = self._extract_code_hybrid()
        clean_code = self._clean_noise(raw_code)
        
        if not clean_code:
            return "Nenhum código encontrado para resumir."

        system_prompt = (
'''# Role
Você é um Engenheiro de Dados Sênior, especialista em Microsoft Fabric, Delta Lake e orquestração de pipelines complexos.

# Objetivo
Sua tarefa é analisar o código de um notebook do Microsoft Fabric (fornecido a seguir) e gerar uma **Documentação Técnica Completa**. A documentação deve ser estruturada, profissional e focar na lógica de negócios, fluxo de dados e arquitetura técnica.

# Instruções de Análise
Para realizar a tarefa, você deve ler e interpretar integralmente o notebook, considerando:
* Células de código (PySpark, Python, SQL).
* Utilização de bibliotecas específicas (`mssparkutils`, `delta`, `pyspark.sql`).
* Comentários, prints, logs e mensagens de erro.
* Chamadas de orquestração (`mssparkutils.notebook.run`, `exit`).

---

# Estrutura Obrigatória da Documentação
A saída deve seguir estritamente os tópicos abaixo:

## 1. Resumo Executivo
* **Visão Geral:** Uma descrição de alto nível do que o notebook faz.
* **Diagrama Narrativo:** Representação textual do fluxo (ex: `Origem [SAP] -> Processamento [PySpark] -> Destino [Delta Table]`).

## 2. Arquitetura e Fluxo de Dados (End-to-End)
* **Origem dos Dados:**
    * Identifique a fonte (SAP, SQL Server, OneLake, API, Arquivos RAW, etc.).
    * Liste os caminhos (paths) ou tabelas de leitura.
* **Camadas Utilizadas:**
    * Mapeie o movimento dos dados entre camadas (RAW -> BRONZE -> SILVER -> GOLD/WAREHOUSE).
* **Destino e Persistência:**
    * Tabelas ou arquivos gerados.
    * Formato de escrita (Delta, Parquet, CSV).
    * Modo de escrita (`append`, `overwrite`, `merge`).
    * Estratégia incremental (uso de `watermark`, carimbos de data/hora, chaves como `ID_VDXM`).
    * Otimizações aplicadas (`OPTIMIZE`, `VACUUM`, `PARTITION BY`).

## 3. Detalhe das Transformações e Regras de Negócio
Para cada etapa lógica do código, descreva:
* **Tratamentos:** Casts, normalização de colunas, limpeza de strings.
* **Lógica Relacional:** Joins, uniões, deduplicações.
* **Filtros:** Regras de exclusão ou seleção de dados.
* **Regras de Negócio Específicas:** Cálculos ou lógica complexa aplicada ao dataset.

## 4. Orquestração e Controle de Qualidade
* **Integração com Fabric:** Como o notebook recebe parâmetros e como retorna status (`mssparkutils.notebook.exit`).
* **Mecanismos de Resiliência:** Blocos `try/except`, validação de paths (`fs.exists`), tratamento de nulos.
* **Logging e Monitoramento:** Como o notebook registra o progresso ou erros (listas acumuladas de erros, prints de controle).

## 5. Dicionário de Estruturas (Tabelas e Variáveis)
* Liste as principais tabelas lidas e escritas.
* Indique as chaves primárias ou colunas de partição identificadas.

## 6. Observações e Recomendações (Critical Review)
Como Engenheiro Sênior, analise o código criticamente e liste:
* **Riscos Técnicos:** Pontos frágeis que podem causar falhas.
* **Performance:** Oportunidades de otimização (paralelismo, predicate pushdown, z-ordering).
* **Melhores Práticas:** Sugestões para adequar o código aos padrões do Microsoft Fabric e Delta Lake.

---

**[INSERIR CÓDIGO DO NOTEBOOK AQUI]**
'''
        )
        
        return self._call_llm(system_prompt, clean_code)

    def _call_llm(self, system_prompt: str, user_content: str) -> str:
        try:
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content}
            ]
            
            # Verifica se é um cliente OpenAI (novo padrão)
            if hasattr(self.llm_client, 'chat'):
                deployment_name = "Qualificacao_de_JSON" # Nome do deployment fixo conforme solicitado
                response = self.llm_client.chat.completions.create(
                    model=deployment_name,
                    messages=messages,
                    temperature=0.01,
                    max_tokens=3000,
                )
                return response.choices[0].message.content

            # Fallback para LangChain (caso o usuário tenha passado um cliente customizado antigo)
            elif hasattr(self.llm_client, 'invoke') or hasattr(self.llm_client, '__call__'):
                from langchain.schema import HumanMessage, SystemMessage
                lc_messages = [
                    SystemMessage(content=system_prompt),
                    HumanMessage(content=user_content)
                ]
                if hasattr(self.llm_client, 'invoke'):
                    response = self.llm_client.invoke(lc_messages)
                else:
                    response = self.llm_client(lc_messages)
                return getattr(response, 'content', str(response))
            
            else:
                return "Erro: Cliente LLM não reconhecido."
            
        except Exception as e:
            return f"Chamada ao LLM Falhou: {e}"
