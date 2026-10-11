# Desafio LACEDA — Integração CNEFE-BA × Base de Busca

Encontra o **setor censitário** de cada endereço da base de busca, cruzando-a com o CNEFE da Bahia.

## Abordagem

1. **Limpeza**: leitura dos CSVs com PySpark e descarte das colunas do CNEFE que não são usadas.
2. **Padronização**: criação de uma coluna `enderFormatado` nas duas bases, concatenando logradouro, bairro/localidade, número e CEP.
3. **Filtro por CEP (blocking)**: cada endereço da busca só é comparado com os endereços do CNEFE que têm o mesmo CEP, o que evita centenas de milhões de comparações.
4. **Fuzzy matching**: o RapidFuzz (`extractOne`) escolhe o candidato mais parecido, e o `COD_SETOR` dele é gravado em `setor_censitario_encontrado`. Sem match, o valor fica vazio.

## Dependências

- Python 3.9+
- Apache Spark 4.2.0
- Java 8, 11 ou 17 (exigido pelo Spark)
- Bibliotecas em `requirements.txt`: `pyspark`, `rapidfuzz`

## Passo a passo

```bash
git clone https://github.com/bispomariana/desafioTecnicoLACEDA.git

python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# coloque as bases (separador ";") em data/:
#   data/base_CNEFE_BA.csv
#   data/base_busca.csv

python main.py
```

Saída: `data/base_busca_final.csv/`, com todas as colunas da base de busca mais `enderFormatado` e `setor_censitario_encontrado`.

## Estrutura

```
.
├── main.py
├── requirements.txt
├── README.md
└── data/        # bases de entrada e saída (não versionadas)
```

## Premissas

- O CEP da base de busca está preenchido e correto.
- O CNEFE é a fonte de verdade para o `COD_SETOR`.
- Um score de similaridade ≥ 60 indica um match aceitável.
- Dentro do mesmo CEP, o candidato com maior score é o endereço correto.
- Os CSVs usam `;` como separador e têm cabeçalho.

## Limitações conhecidas

- Endereços sem CEP, ou com CEP errado, ficam sem setor.
- O laço com `collect()` roda no driver e dispara uma consulta Spark por registro; funciona, mas é lento e não escala para bases de busca muito grandes.
- A acurácia não foi medida contra um gabarito, e o limiar 60 foi definido sem calibração.
- Endereços muito parecidos no mesmo CEP (ex.: números diferentes na mesma rua) podem ser confundidos.