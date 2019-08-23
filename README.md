# azure-kusto-notebooks for Python

## Background

This project is a utility for streamlining the usage of [`azure-kusto-data`](https://github.com/Azure/azure-kusto-python) when running from a [Jupyter](https://jupyter.org/) notebook. 

Specifically, it handles parallalization of kusto queries, and simplifying AAD authentication. 

## Publish script

```bash
python publish.py
```

## Test
```bash
pip uninstall azure-kusto-notebooks -y
pip install azure-kusto-notebooks
pip install --force-reinstall azure-nspkg==1.0.0
pytest tests
```
