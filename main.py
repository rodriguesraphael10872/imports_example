
"""
main.py
-------
Script de execução agendada do pipeline.
"""
from etl_pipeline import DataExtractor, DataTransformer, DataLoader, DataPipeline, upper_case


def main():
    extractor = DataExtractor(source="dummy_source.csv")
    transformer = DataTransformer(transforms=[upper_case])
    loader = DataLoader(destination="datawarehouse")
    pipeline = DataPipeline(extractor, transformer, loader)
    pipeline.run()


if __name__ == "__main__":
    main()
