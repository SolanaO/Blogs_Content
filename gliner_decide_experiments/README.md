# 25+1 Experiments with GLiNER2.5-Decide

This notebook and deployment code accompany the blogpost [25+1 Experiments with GLiNER2.5-Decide]().

## Environment and notebook setup

These instructions use a Jupyter notebook in VS Code. Install `pykernel` to run the notebook with its environment. 

Create a virtual environment

```bash
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python "gliner2[local]" ipykernel
```

Explicit kernel registration is optional if your IDE already detects the environment. 

```bash
.venv/bin/python -m ipykernel install --user \
  --name gliner-decide \
  --display-name "Python (GLiNER Decide)"
  ```

Once the environment is set:

1. Open the notebook in VS Code.
2. Select Python (GLiNER Decide) using Select Kernel.
3. Run the model-loading cell once, then run the examples.
  

## Local GLiNER2.5-Decide deployment

Minimal adaptation of [DevBuilder](https://github.com/silviaonofreilab/devbuilder): manifest hashing and CPU ONNX Runtime loading follow its patterns. GLiNER needs its own scoring export instead of the standard Transformers sequence classification export.

### Run

Keep these files together. From this folder, using your existing Python 3.11 `uv` environment with `gliner2` installed:

```bash
uv pip install onnx onnxruntime pytest
uv run python deploy_decider.py
uv run pytest -q -s test_decider.py
```

The script downloads the checkpoint (reusing the Hugging Face cache), exports full-precision ONNX, writes the manifest, and prints one local prediction. The registered smoke test makes one additional inference call with two tasks and prints its result. It checks output structure, permitted labels, and finite confidence values; it is not an accuracy or PyTorch/ORT parity evaluation.

Generated files are kept in `artifacts/`: `model.onnx`, `tokenizer/`, `manifest.json`, and any tensor-data sidecar produced by the exporter. The manifest records file hashes, checkpoint revision, package versions, and export settings. Hashes are recorded, not automatically verified at load time.

### Three functions

- `export_decider()` downloads and exports the scoring graph and tokenizer, returning metadata.
- `write_manifest(metadata)` writes the artifact manifest.
- `run_decider(text, schema, include_confidence=True)` loads local artifacts once per process and returns classification results.

```python
from deploy_decider import run_decider

result = run_decider(
    "I was charged twice. Please refund the duplicate.",
    {"queue": ["billing", "technical_support", "shipping"]},
)
print(result)
```

The schema accepts label lists, label-description dictionaries, and task configurations with `labels`, `multi_label`, and `cls_threshold`, using GLiNER's schema builder. The result uses the familiar `label` and `confidence` fields; a multi-label task returns a list.

### Deployment scope

The ONNX graph contains the encoder, selection of label-token embeddings, and classification head. GLiNER's existing Python processor prepares the text and schema. Python converts output scores to probabilities and selects labels. Thus `gliner2`, Transformers, and PyTorch remain environment dependencies for preprocessing, but inference does not load the PyTorch checkpoint.

This version supports one text at a time, multiple classification tasks, and variable sequence and label counts. It does not export span/relation extraction or the separate cross-question constrained decoding API. Text plus schema must fit the encoder's position limit; longer inputs raise an error rather than being silently truncated. Runtime loading is local-only after export; initial checkpoint acquisition requires access to Hugging Face.
