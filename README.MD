# Capstone Economics Project and Paper

## Prerequisites to running notebooks
Make sure to install python requirements with:
```shell
pip install -r requirements.txt
```

For economic data, you need to go and download it from the statistical
agency for that entity. More information on exact variables used and
where to find them on the website can be found in `notebooks/` for the
Data Collection ones.

To run the political data collection notebooks, you will need to install
`firefox` and `geckodriver` for scraping to work for the 2014 and 2010
websites. If you are on MacOS and have brew installed, you can use the
following commands:
```shell
brew install geckodriver
```

```shell
brew install --cask firefox
```