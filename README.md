
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
