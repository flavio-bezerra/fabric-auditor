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
logger = logging.getLogger(__name__)

class FabricAuditor:
    def __init__(self, llm_client: Any):
        """
        Inicializa o FabricAuditor.
        
        Args:
            llm_client: Um objeto cliente LLM instanciado (ex: AzureChatOpenAI do LangChain).
                        Deve suportar o método `invoke` (padrão LangChain) ou `__call__` aceitando uma lista de mensagens.
        """
        self.llm_client = llm_client
        
        # Padrões para ignorar (auto-exclusão)
        self.ignore_patterns = [
            "def snapshot_notebook_limpo",
            "snapshot_notebook_limpo()",
            "FabricAuditor",
            "audit_code",
            "summarize_notebook",
            "mssparkutils.credentials.getToken",
            "trident.workspace.id"
        ]

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
            "Você é um Arquiteto de Soluções Sênior especializado em PySpark e Segurança de Aplicações.\n"
            "Sua tarefa é revisar o código Python extraído de um Notebook Spark no Microsoft Fabric.\n\n"
            "Analise o código focando estritamente nestes três pilares. Use a estrutura de indentação abaixo para o seu relatório:\n\n"
            "1. SEGURANÇA (PRIORIDADE CRÍTICA)\n"
            "   - Procure por credenciais hardcoded (Chaves de API, tokens SAS, senhas, connection strings).\n"
            "   - Identifique riscos de SQL Injection (ex: uso inseguro de f-strings em queries SQL).\n"
            "   - Verifique exposição de dados sensíveis (PII) em comandos print ou logs.\n\n"
            "2. PERFORMANCE SPARK (ALTA PRIORIDADE)\n"
            "   - Identifique uso perigoso de '.collect()' ou '.toPandas()' em datasets grandes (Risco de Driver OOM).\n"
            "   - Aponte loops 'for' iterando sobre linhas de DataFrames (anti-padrão).\n"
            "   - Sugira joins do tipo 'broadcast' se houver evidência de tabelas pequenas.\n"
            "   - Aponte ineficiências de I/O (ex: problema de muitos arquivos pequenos).\n\n"
            "3. QUALIDADE DE CÓDIGO E BOAS PRÁTICAS\n"
            "   - Verifique legibilidade e padrões de nomenclatura de variáveis.\n"
            "   - Identifique imports não utilizados ou código morto.\n\n"
            "FORMATO DE SAÍDA (Markdown):\n\n"
            "## Relatório de Auditoria\n\n"
            "### 1. Riscos de Segurança\n"
            "* (Liste os achados aqui ou declare \"Nenhum risco crítico identificado\")\n\n"
            "### 2. Gargalos de Performance\n"
            "* (Liste os achados com sugestões específicas de correção)\n\n"
            "### 3. Sugestões de Qualidade\n"
            "* (Dicas breves de refatoração)\n\n"
            "---\n"
            "**Veredito Final:** (Aprovado / Requer Revisão / Falha Crítica)"
        )
        
        return self._call_llm(system_prompt, clean_code)

    def summarize_notebook(self) -> str:
        raw_code = self._extract_code_hybrid()
        clean_code = self._clean_noise(raw_code)
        
        if not clean_code:
            return "Nenhum código encontrado para resumir."

        system_prompt = (
            "Você é um Gerente Técnico de Produto (PM).\n"
            "Resuma o que este código Python/Spark faz para uma audiência de negócios.\n\n"
            "DIRETRIZES:\n"
            "- NÃO descreva a sintaxe (ex: \"ele importa pandas\", \"ele define uma função\").\n"
            "- FOCO NO FLUXO DE DADOS: De onde o dado vem? Qual transformação de negócio acontece? Onde ele é salvo?\n"
            "- IDENTIFIQUE O OBJETIVO: Infira se é um relatório de vendas, um modelo preditivo, um pipeline de ETL, etc.\n"
            "- IDIOMA: Português do Brasil.\n"
            "- ESTILO: Profissional, direto, sem emojis.\n\n"
            "RESTRIÇÃO DE TAMANHO:\n"
            "Máximo de 2 parágrafos curtos.\n\n"
            "FORMATO DE SAÍDA:\n"
            "Comece as frases com verbos de ação (ex: \"Processa...\", \"Ingere...\", \"Calcula...\")."
        )
        
        return self._call_llm(system_prompt, clean_code)

    def _call_llm(self, system_prompt: str, user_content: str) -> str:
        try:
            # Importação tardia ou assumindo que o usuário tem langchain instalado
            from langchain.schema import HumanMessage, SystemMessage
            
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_content)
            ]
            
            # Suporte a LangChain moderno (.invoke) e legado (__call__)
            if hasattr(self.llm_client, 'invoke'):
                response = self.llm_client.invoke(messages)
            else:
                response = self.llm_client(messages)
                
            # O retorno geralmente é um objeto AIMessage, queremos o .content
            return getattr(response, 'content', str(response))
            
        except ImportError:
            return "Erro: A biblioteca 'langchain' é necessária para usar este recurso. Instale-a com 'pip install langchain'."
        except Exception as e:
            return f"Chamada ao LLM Falhou: {e}"
