# Project

ECE Big Data processing project (fall 2026): preparation, exploration, enrichment and streaming of the [IMDb non-commercial datasets](https://developer.imdb.com/non-commercial-datasets/) with Apache Spark and Apache Kafka.

## Contributors

| Name | Email | Tasks |
|---|---|---|
| Alban VILLAIN | Albengs | Arround half of data exploration. Most of data enrichement and the final question. 
| Ahmed amine ZAHRAOUI | ahmed-zhr | Data gathering and quality. Arround half of the data exploration. Helped for finding and solving the final question |

## Running the project

Requirements: Docker. Java, Spark, Python and Jupyter are in the container. The iMDb database must be downloaded and placed in the following directory: imdb_data/name.basics.tsv.gz/name.basics.tsv (and so on for the 7 DB )

1. Start Jupyter, then open <http://localhost:8888>:

   ```bash
   docker compose up -d
   ```

2. Run these notebooks in order, each with `MAX_ROWS = -1` so every row of the IMDB datasets are gathered. Use any other positive integer to reduce execution time (but with less data)

   1. `IMDb_Data_Quality_and_Exploration.ipynb`:It downloads the IMDb files if they're missing and makes data preparation and exploration. 
   2. `IMDb_Data_Enrichment.ipynb`: data enrichment.
   3. `IMDb_Open_Question.ipynb`: the final question.

3. Start Kafka and the Wikipedia producer, then run `IMDb_Wikipedia_Streaming.ipynb`:

   ```bash
   docker compose --profile streaming up -d
   ```

4. Stop everything:

   ```bash
   docker compose --profile streaming down
   ```

Results are written to `results/full/`.

## Use of AI tools

| Tool | Tasks |
|---|---|
| Claude Code (Anthropic, model Claude Opus 5.5) | helped with code for the notebooks |

The enriched text (synopses and biographies) is not AI-generated: it is taken from Wikipedia and Wikidata, with a source URL on every record.


