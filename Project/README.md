# IMDb Big Data project

PySpark notebooks for the ECE Big Data project on the [IMDb datasets](https://developer.imdb.com/non-commercial-datasets/). Everything runs in Docker: you only need [Docker](https://www.docker.com/products/docker-desktop/) with at least 8 GB of memory.

## Files

| File | Role |
|---|---|
| `docker-compose.yml` | Starts Jupyter with PySpark (and Kafka for streaming) |
| `IMDb_Data_Quality_and_Exploration.ipynb` | Data preparation and exploration |
| `IMDb_Data_Enrichment.ipynb` | Data enrichment (synopses and biographies from Wikipedia) |
| `IMDb_Open_Question.ipynb` | Open question: Wikipedia attention vs IMDb popularity |
| `IMDb_Wikipedia_Streaming.ipynb` | Streaming of live Wikipedia changes with Kafka |
| `wikimedia_client.py` | Helper used by the notebooks (not run directly) |
| `wiki_producer.py` | Sends live Wikipedia changes to Kafka (run by Docker) |
| `imdb_data/` | IMDb files, downloaded automatically if missing |

Results are written to `results/`. For settings and troubleshooting, see [RUN_GUIDE.md](RUN_GUIDE.md).

## How to run

1. Start Jupyter, then open <http://localhost:8888>:

   ```bash
   docker compose up -d
   ```

2. Run the notebooks **in this order**. In each one, set `MAX_ROWS = -1` (full data) at the top, then use **Run → Run All Cells**:

   1. `IMDb_Data_Quality_and_Exploration.ipynb` (30–60 min)
   2. `IMDb_Data_Enrichment.ipynb` (45–60 min)
   3. `IMDb_Open_Question.ipynb` (15–25 min)

3. For streaming, start Kafka, then run `IMDb_Wikipedia_Streaming.ipynb` (about 20 min):

   ```bash
   docker compose --profile streaming up -d
   ```

4. Stop everything when you're done:

   ```bash
   docker compose --profile streaming down
   ```

`MAX_ROWS = 3000` (the default) runs on a sample: the 3,000 most-voted titles, the episodes of those series and every row linked to them. Every question gets an answer, but only for popular titles. Results go to `results/sample_3000/`.

## Side note
The git history does not match our work separation. We made a separate Git for the project but when working on the project we failed to correcly merge and added local files to the project wich made the other PC crash. We saved the files and upladed them in this Repo so it would resemble the work separation on the original Git.
