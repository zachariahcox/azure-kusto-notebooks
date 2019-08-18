
## Publish script

```bash
pip install twine
python setup.py sdist
twine upload dist/*
```

## Test
```bash
pip install pytest
pytest tests
```