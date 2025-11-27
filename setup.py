from setuptools import setup, find_packages

setup(
    name="fabric_auditor",
    version="0.1.0",
    description="Uma biblioteca para auditar e resumir Notebooks do Microsoft Fabric usando LLMs.",
    author="Seu Nome",
    packages=find_packages(),
    install_requires=[
        "requests",
        "openai==1.51.0",
        "pydantic==2.12.2",
        "httpx==0.27.2",
        "azure-identity",
        "azure-keyvault-secrets",
    ],
)
