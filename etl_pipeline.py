
"""
etl_pipeline.py
----------------
Pequeno framework de ETL com classes, funções utilitárias e tipagem
"""
from typing import List, Callable, Any
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


class DataExtractor:
    """Responsável por extrair dados de uma fonte"""
    def __init__(self, source: str):
        self.source = source

    def extract(self) -> List[str]:
        logging.info("Extraindo dados de %s", self.source)
        # Simulação de extração
        data = [f"linha_{i}" for i in range(1, 6)]
        return data


class DataTransformer:
    """Aplica transformações em uma lista de registros"""
    def __init__(self, transforms: List[Callable[[str], Any]]):
        self.transforms = transforms

    def transform(self, data: List[str]) -> List[Any]:
        logging.info("Transformando %d registros", len(data))
        for func in self.transforms:
            data = [func(d) for d in data]
        return data


class DataLoader:
    """Carrega dados em um destino (ex.: banco, arquivo, API)"""
    def __init__(self, destination: str):
        self.destination = destination

    def load(self, data: List[Any]) -> None:
        logging.info("Carregando %d registros em %s", len(data), self.destination)
        # Simulação de carga
        for row in data:
            logging.debug("Carregado: %s", row)


class DataPipeline:
    """Pipeline completo combinando Extract, Transform e Load"""
    def __init__(self, extractor: DataExtractor, transformer: DataTransformer, loader: DataLoader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self) -> None:
        raw = self.extractor.extract()
        processed = self.transformer.transform(raw)
        self.loader.load(processed)


# Função utilitária de exemplo
def upper_case(text: str) -> str:
    return text.upper()
