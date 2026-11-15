# Report

All numbers below come from a run with `MAX_ROWS = 3000`. Result tables are in `results/sample_3000/`. Since our machine were not powerfull enough to run the full study on the full dataset. However, our code should be fit to run on the full imdb dataset provided the `MAX_ROWS` variable is set to -1
It is important to note that non negligeable size ouf our results are due to the sampling of only the 3000 titles. Othr conclusions can be drawn after running the scripts on the full database.


## 1. Data preparation

### Results

**Top 5 columns with the most missing values:**

| Table | Column | Missing |
|---|---|---|
| title_basics | endYear | 99.12 % |
| title_akas | attributes | 96.63 % |
| name_basics | deathYear | 88.61 % |
| title_akas | language | 73.58 % |
| title_principals | job | 68.16 % |

**Other issues found:**

| Issue | Rows |
|---|---|
| `knownForTitles` referencing titles outside the sample | 127,770 |
| Episodes with season or episode number ≤ 0 | 29 |
| Credits on titles released before the person's birth | 11 |
| People who died before they were born | 2 |
| Duplicates, invalid ratings or runtimes | 0 |

**Handling:**
- invalid values become NULL;
- impossible dates are set to NULL, but the person is kept;
- orphan rows are removed;
- nothing is imputed.

**Trade-off:** we didn't delete people with impossible dates, because their credits would become orphans.

**Methodology: how the sample is built.**
- Chosen: keep the 3,000 most-voted titles and their episodes, then only the rows of the other files linked to them. Every question gets an answer, because all tables refer to the same titles.
- Alternatives considered:
  - The first N rows of each file: our first approach. The files are sorted by ID, so the samples didn't refer to each other: the titles were from 1894–1910, and almost every result was empty.
  - A random sample: most IMDb titles are episodes or obscure titles with few votes, so the questions about successful movies would stay almost empty.
- Cost: the sample only covers popular titles, which is why some counts (such as `knownForTitles`) are not representative.

## 2. Data exploration

| Question | Rows | Example |
|---|---|---|
| Movies with unusual runtimes for their genre | 186 | Gangs of Wasseypur (321 min, Action) |
| Top 3 genres per year, 1980–2025 | 150 | Drama is first in 35 of 46 years |
| Directors with one movie ≥ 8.0, others ≤ 6.0 | 1 | M. Night Shyamalan (The Sixth Sense) |
| Series with best episode early and worst in the last season | 25 | 13 Reasons Why |
| Actors successful in ≥ 2 / ≥ 3 languages | 74 / 2 | Max von Sydow |
| Actors with an early-career peak | 63 | Robert De Niro |
| Multi-talented professionals | 851 | Sylvester Stallone |

**Multiple roles on the same film:** these movies rate slightly higher (7.14 vs 6.93). The effect is small: +0.11 points for the same person.

**Definitions:**
- Feature length: ≥ 70 min.
- Unusual runtime: outside 1.5×IQR of the genre, chosen because runtimes are biased.
- Success: rating ≥ 7.5 with ≥ 1,000 votes.

**Methodology: a movie's original language.** IMDb has no "original language" column. On the alternative title marked as original, the language field is always empty.
- Chosen: look at the other alternative titles that have the same name as the original title, and take their most frequent language. When they have no language, their country's main language is used (ex FR → French).
- Alternatives considered:
  - Assuming the language of the first release country: simpler, but wrong for many co-productions and festival premieres.
  - Wikidata's "original language" property: more reliable, but it would need one extra request per movie.
- Cost: the estimate can be wrong for international co-productions.

## 3. Data enrichment

The top 10 films per genre and year (1980–2025) were enriched with a synopsis, a summary and 3 principals' biographies from Wikipedia and Wikidata.

| Measure | Films |
|---|---|
| Films enriched | 2,090 |
| With a synopsis | 2,088 (99.9 %) |
| With 3 biographies | 2,069 (99.0 %) |
| With a "missing information" note | 23 (1.1 %) |

**Method:**
- films and people are matched through the IMDb ID stored in Wikidata, not by title, which avoids wrong matches;
- the data is downloaded slowly to avoid overloading the server (at most 1 request per second, batched);
- the data is processed in Spark;
- every value keeps its source URL.

**Trade-off:** the public APIs were used rather than scraping web pages.


## 4. Streaming

Live Wikipedia edits are sent to Kafka by `wiki_producer.py`, then filtered by Spark Structured Streaming against a lookup of IMDb titles and names.

| Measure (5-minute run) | Value |
|---|---|
| Events read | 11,349 (36 per second) |
| From English Wikipedia or Wikidata | 34 % |
| Matching IMDb data | 9 (e.g. *Pacific Rim*, Brad Bird) |
| Lateness of matched events | 3 s median, 8 s max |

**Late data:**
- windows use the edit time;
- a 10-minute watermark lets late events be counted in the right window;
- events later than that are dropped and counted, and they stay in Kafka for 24 hours;
- duplicate events are removed.

**Trade-off:** a longer watermark gives more complete counts, but uses more memory and delays the final results.

**Methodology: where the stream is filtered.**
- Chosen: the producer sends every Wikipedia event unchanged to Kafka, and Spark does the filtering.
- Alternative considered: filtering in the producer, keeping only English Wikipedia edits. Kafka would hold about 3 times less data.
- Why not: the raw stream stays in Kafka for 24 hours. It can be reprocessed with a different lookup or filter, and Spark can measure selectivity (the share of events that match).

## 5. Open question

**Does a film's attention on Wikipedia match its popularity on IMDb?** For each film we used its page views, article size and number of language editions.

| Rank correlation with IMDb votes | Value |
|---|---|
| Page views (12 months) | 0.54 |
| Number of language editions | 0.69 |
| Article size | 0.48 |
| IMDb rating vs page views | 0.36 |

**Findings:**
- Popularity explains only part of Wikipedia attention: R² is 0.26 in the log-log fit.
- Recent films are viewed far more than their votes predict, ex : Dhurandhar and Avatar: Fire and Ash (2025). So the 2020s have the weakest correlation (0.38).
- Some hits have short articles: Snatch, Prisoners.
- Limitation: a few low-view films are matching errors, ex: Avengers: Endgame is matched to its re-release.

**Trade-off:** rank correlations were used instead of Pearson, because the counts are very biased.

**Methodology: measuring Wikipedia attention.**
- Chosen:** human page views over the last 12 months, plus article size and the number of language editions.
- Alternatives considered:
  - All-time page views: the data only starts in 2015, so older films would be undercounted.
  - Edit counts: they would need many more requests and edits would not represent popularyty of the masses.
  - Google Trends: not an open source, and it bans automated collection.
- Cost: 12 months favours films released or in the news this year. That is what the 2025 outliers show.
