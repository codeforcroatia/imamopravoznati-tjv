# TJV register scraper
This is a scraper that runs on [Morph](https://morph.io). To get started [see Morph.io the documentation](https://morph.io/documentation).
Scraper runs and data on Morph.io: https://morph.io/codeforcroatia/imamopravoznati-tjv.

Python 3.5 with Pandas.

## How it works?
1. Scraper will take data from official CSV register at https://tjv.pristupinfo.hr/ (TJV)
2. Scraped TJV CSV data will be stored to `allData` table
3. Then data will be processed and cleanup - it will compare existing `data` and `allData` and mark what was updated since last run.
4. Clean data will be stored to `data` table

Work by [SelectSoft](https://github.com/SelectSoft)
