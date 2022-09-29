# Virgin Media Apache Beam Pipeline
A simple apache beam pipeline that processes transaction data, aggregates and filters the data, and then outputs the results as a compressed json line file.

# Dependencies
## Virtual Environment Config
The project uses [pipenv](https://pipenv.pypa.io/en/latest/) for dependency management. You can create the virtual environment using the following command from the repository base folder: 

`pipenv shell`

If you do not have pipenv installed instructions can be found on the link above.

# Quickstart
## Using pipenv

`pipenv shell`

`python src/virginmedia_pipeline.py`

`python src/test_pipeline.py`