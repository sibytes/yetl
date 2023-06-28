# What Is Yetl

Website: https://www.yetl.io/


## Development Setup

```
pip install -r requirements.txt
```

## Unit Tests

To run the unit tests with a coverage report.

```
pip install -e .
pytest test/unit --junitxml=junit/test-results.xml --cov=yetl --cov-report=xml --cov-report=html
```

## Integration Tests

To run the integration tests with a coverage report.

```
pip install -e .
pytest test/integration --junitxml=junit/test-results.xml --cov=yetl --cov-report=xml --cov-report=html
```

## Build

```
python setup.py sdist bdist_wheel
```

## Publish


```
twine upload dist/*
```
