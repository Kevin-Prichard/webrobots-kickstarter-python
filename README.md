# werobots-kickstarter-python
Simple reader for the Kickstarter scrape produced by WeRobots.com: http://webrobots.io/kickstarter-datasets/

Dependencies:
    TextBlob
    BeautifulSoup
    lxml
    nltk
    peewee (2.6.0)

Usage:

    % ./wr_ks_reader.py <webrobots_ks_data.json> <usd_fx_csv>

Example with supplied sample data:

    % ./wr_ks_reader.py sample-data/five_projects_from-2014-12-02.json sample-data/usd_all_2015-03-25.csv

Example with filtering predicate, selects projects from just two categories:

    % ./wr_ks_reader.py sample-data/five_projects_from-2014-12-02.json sample-data/usd_all_2015-03-25.csv category/id=284,286

