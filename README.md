# Fabric Auditor 🕵️‍♂️📊

**Fabric Auditor** é uma biblioteca Python projetada especificamente para rodar dentro de **Microsoft Fabric Notebooks**. Ela extrai automaticamente o código do notebook atual, limpa "ruídos" (como boilerplate do Spark e comandos mágicos), e envia o código limpo para um Modelo de Linguagem (LLM) para auditoria de segurança, performance ou sumarização.

## 🚀 Funcionalidades

*   **Extração Híbrida "Fail-Safe"**: Tenta obter o código via API do Fabric (mais preciso). Se falhar ou demorar, faz fallback automático para a memória da sessão (IPython history).
*   **Limpeza Inteligente**: Remove automaticamente:
    *   Cabeçalhos de licença Apache.
    *   Blocos de inicialização do Spark (`init_spark`).
    *   Configurações de `sc.setJobGroup`.
    *   Comandos mágicos (`%time`, `%pip`).
    *   **Redação de Segredos**: Mascara automaticamente chaves de API (ex: `sk-...`) antes de enviar ao LLM.
*   **Agnóstico a LLM**: Projetado para funcionar com qualquer modelo compatível com **LangChain** (Azure OpenAI, OpenAI, Ollama, etc.).

---

## 📦 Como Instalar no Microsoft Fabric

Como esta biblioteca está hospedada em um repositório Git, você pode instalá-la diretamente no seu ambiente.

### Opção 1: Instalação Direta via Git (Recomendado)
Você pode instalar diretamente na sessão do notebook usando `%pip` apontando para o seu repositório.

**Repositório Público:**
```python
%pip install git+https://github.com/SEU_USUARIO/fabric-auditor.git
```

**Repositório Privado (com Token):**
Se o repositório for privado, você precisará de um Personal Access Token (PAT).
```python
# Exemplo com GitHub
%pip install git+https://SEU_TOKEN@github.com/SEU_USUARIO/fabric-auditor.git
```

### Opção 2: Instalação via Environment (Produção)
Para disponibilizar a biblioteca em todos os notebooks de um Workspace:

1.  No Microsoft Fabric, vá em **Manage environments** (ou crie um novo).
2.  Na seção **Public Libraries**, adicione as dependências: `langchain`, `openai`.
3.  Para a biblioteca `fabric_auditor`, você tem duas escolhas:
    *   **Upload do Wheel**: Gere o `.whl` localmente (`python setup.py bdist_wheel`) e faça upload na aba **Custom Libraries**.
    *   **PyPI (se publicado)**: Se você publicar no PyPI futuramente, basta adicionar `fabric-auditor` nas Public Libraries.
4.  Publique o ambiente e anexe-o ao seu Notebook.

---

## 🚀 Uso Rápido (Configuração Automática)

Se você já possui o ambiente configurado com o arquivo de credenciais padrão, a biblioteca se configura automaticamente:

```python
from fabric_auditor import FabricAuditor

# Inicializa sem argumentos -> Tenta ler JSON e KeyVault automaticamente
auditor = FabricAuditor()

# Executa a auditoria
print("🔍 Auditoria:")
print(auditor.audit_code())

# Gera o resumo
print("\n📝 Resumo:")
print(auditor.summarize_notebook())
```

### Pré-requisitos para Uso Rápido
Para que a configuração automática funcione, você precisa ter:
1.  Um arquivo JSON em: `{notebookutils.nbResPath}/env/CS_API_REST_LOGIN.json`
2.  O JSON deve seguir este formato:
    ```json
    {
        "tenant_id": "...",
        "client_id": "...",
        "client_secret": "..."
    }
    ```
3.  As bibliotecas `azure-identity` e `azure-keyvault-secrets` instaladas.

---

## 🛠️ Como Usar (Configuração Manual)

Aqui está um exemplo completo de como configurar o modelo manualmente (usando Azure OpenAI) e rodar a auditoria.

```python
from fabric_auditor import FabricAuditor
from langchain.chat_models import AzureChatOpenAI

# 1. Configuração do Modelo (Exemplo com Azure OpenAI)
# Certifique-se de ter as variáveis ou use um Key Vault para a chave
llm_model = AzureChatOpenAI(
    openai_api_base="https://datasciencellm.openai.azure.com/",
    openai_api_key="SUA_CHAVE_AQUI", # Recomendado: mssparkutils.credentials.getSecret(...)
    openai_api_version="2024-12-01-preview",
    deployment_name="gpt-4",
    temperature=0.0
)

# 2. Inicializar o Auditor
# Passamos o cliente LLM diretamente para o auditor
auditor = FabricAuditor(llm_client=llm_model)

# 3. Auditar o Código (Segurança, Performance e Qualidade)
print("🔍 Iniciando Auditoria...\n")
relatorio = auditor.audit_code()
print(relatorio)

# 4. Gerar Resumo do Notebook
print("\n📝 Gerando Resumo...\n")
resumo = auditor.summarize_notebook()
print(resumo)
```

## ⚙️ Como Funciona (Por Baixo do Capô)

1.  **Inicialização**: O `FabricAuditor` recebe seu cliente LLM configurado.
2.  **Extração**:
    *   O auditor tenta identificar o ID do Workspace e do Notebook atuais.
    *   Ele chama a API `POST /getDefinition` do Fabric.
    *   Se a API demorar (status 202), ele aguarda.
    *   Se a API falhar, ele varre a variável global `In` do Python para pegar as células executadas.
3.  **Limpeza**: O código bruto passa por uma série de Regex para remover códigos de infraestrutura que não interessam ao LLM.
4.  **Análise**: O código limpo é enviado ao LLM com um System Prompt especializado (Auditor de Segurança ou Resumidor).

## 🛡️ Segurança

*   A biblioteca possui um mecanismo de **Auto-Exclusão**: ela ignora células que contenham seu próprio código de chamada para evitar loops ou alucinações sobre o próprio auditor.
*   Chaves que seguem o padrão `sk-...` são mascaradas automaticamente antes do envio.

---

**Desenvolvido para Data Engineering Moderno no Microsoft Fabric.**
