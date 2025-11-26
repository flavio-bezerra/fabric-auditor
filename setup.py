from setuptools import setup, find_packages

setup(
    name="fabric_auditor",
    version="0.1.0",
    description="Uma biblioteca para auditar e resumir Notebooks do Microsoft Fabric usando LLMs.",
    author="Seu Nome",
    packages=find_packages(),
    install_requires=[
        "requests",
        "langchain-community==0.3.20",
        "sentence-transformers==4.0.1",
        "azure-core==1.31.0",
        "fsspec==2024.3.1",
        "filelock==3.11.0",
        "threadpoolctl==3.6.0",
        "openai==1.93.3",
    ],
)
