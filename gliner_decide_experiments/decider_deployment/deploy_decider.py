"""Minimal local GLiNER2.5-Decide deployment.
Only the neural scoring graph runs in ONNX Runtime.
GLiNER's existing processor prepares schemas; Python selects labels.
"""

import hashlib
import json
import sys
from datetime import UTC, datetime
from functools import lru_cache
from importlib.metadata import version
from pathlib import Path

import numpy as np
import onnxruntime as ort


MODEL_ID = "fastino/GLiNER2.5-Decide"
ARTIFACTS = Path("artifacts")
OPSET = 17
DEMO_TEXT = "I was charged twice for my subscription. Please refund the duplicate."
DEMO_SCHEMA = {"queue": ["billing", "technical_support", "shipping"]}


def _prepare_inputs(processor, text, tasks, max_input_tokens):
    """Reuse GLiNER preprocessing and recover labels from the encoded schema."""
    from gliner2.inference.schema import Schema

    schema = Schema()
    for name, specification in tasks.items():
        if isinstance(specification, dict) and "labels" in specification:
            options = dict(specification)
            labels = options.pop("labels")
        else:
            labels, options = specification, {}
        if not labels:
            raise ValueError(f"Task {name!r} must contain at least one label.")
        schema.classification(name, labels, **options)

    batch = processor.collate_fn_inference([(text, schema)], error_policy="raise")
    input_ids = batch.input_ids.numpy().astype(np.int64, copy=False)
    if input_ids.shape[1] > max_input_tokens:
        raise ValueError(
            f"Text plus schema uses {input_ids.shape[1]} tokens; "
            f"this export permits {max_input_tokens}. Shorten the input or schema."
        )

    configurations = schema.build()["classifications"]
    marker_id = processor.tokenizer.convert_tokens_to_ids("[L]")
    positions, layout = [], []
    for index, (tokens, specification) in enumerate(
        zip(batch.schema_tokens_list[0], configurations)
    ):
        names = [tokens[j + 1] for j, token in enumerate(tokens[:-1]) if token == "[L]"]
        indices = [
            j for j, (segment, _, schema_index) in enumerate(batch.mapped_indices[0])
            if segment == "schema" and schema_index == index
            and input_ids[0, j] == marker_id
        ]
        if not names or len(names) != len(indices):
            raise ValueError("Could not align the classification labels with their tokens.")
        positions.extend(indices)
        layout.append((specification, names))

    if len(layout) != len(tasks) or not positions:
        raise ValueError("The prepared schema does not contain the requested tasks.")

    return {
        "input_ids": input_ids,
        "attention_mask": batch.attention_mask.numpy().astype(np.int64, copy=False),
        "label_positions": np.asarray(positions, dtype=np.int64),
    }, layout


def export_decider():
    """Download the checkpoint and save its FP32 scoring graph and tokenizer."""
    import torch
    from gliner2 import AutoExtractor
    from huggingface_hub import model_info

    ARTIFACTS.mkdir(exist_ok=True)
    revision = model_info(MODEL_ID).sha
    model = AutoExtractor.from_pretrained(MODEL_ID, revision=revision).cpu().float().eval()
    model.processor.change_mode(is_training=False)

    class ClassificationGraph(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.encoder = model.encoder
            self.classifier = model.classifier

        def forward(self, input_ids, attention_mask, label_positions):
            hidden = self.encoder(
                input_ids=input_ids, attention_mask=attention_mask
            ).last_hidden_state
            label_embeddings = hidden[0].index_select(0, label_positions)
            return self.classifier(label_embeddings).squeeze(-1)

    max_input_tokens = model.encoder.config.max_position_embeddings
    feed, _ = _prepare_inputs(model.processor, DEMO_TEXT, DEMO_SCHEMA, max_input_tokens)
    graph = ClassificationGraph().eval()
    with torch.no_grad():
        torch.onnx.export(
            graph,
            tuple(torch.from_numpy(feed[name]) for name in feed),
            str(ARTIFACTS / "model.onnx"),
            input_names=list(feed),
            output_names=["logits"],
            dynamic_axes={
                "input_ids": {1: "sequence_length"},
                "attention_mask": {1: "sequence_length"},
                "label_positions": {0: "number_of_labels"},
                "logits": {0: "number_of_labels"},
            },
            opset_version=OPSET,
            dynamo=False,
        )
    model.processor.tokenizer.save_pretrained(ARTIFACTS / "tokenizer")
    print("Exported classification graph and tokenizer to artifacts/.")
    return {
        "model_id": MODEL_ID,
        "model_revision": revision,
        "max_input_tokens": max_input_tokens,
        "token_pooling": model.processor.token_pooling,
    }


def write_manifest(metadata):
    """DevBuilder-style file hashes and export metadata, without its config layer."""
    import onnx

    def sha256(path):
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1 << 20), b""):
                digest.update(chunk)
        return digest.hexdigest()

    proto = onnx.load(str(ARTIFACTS / "model.onnx"), load_external_data=False)
    filenames = {"model.onnx"}
    # Include external tensor data if the exporter produced a sidecar file.
    for tensor in proto.graph.initializer:
        for entry in tensor.external_data:
            if entry.key == "location":
                filenames.add(entry.value)
    filenames.update(
        str(path.relative_to(ARTIFACTS))
        for path in (ARTIFACTS / "tokenizer").rglob("*") if path.is_file()
    )
    manifest = {
        "manifest_version": 1,
        **metadata,
        "artifact_filename": "model.onnx",
        "precision": "float32",
        "onnx_opset": OPSET,
        "onnx_ir_version": proto.ir_version,
        "inputs": ["input_ids", "attention_mask", "label_positions"],
        "outputs": ["logits"],
        "scope": "independent classification, one text with dynamic schema",
        "files_sha256": {name: sha256(ARTIFACTS / name) for name in sorted(filenames)},
        "exported_with": {
            package: version(package)
            for package in ("gliner2", "torch", "transformers", "onnx", "onnxruntime")
        },
        "python": sys.version.split()[0],
        "exported_at": datetime.now(UTC).isoformat(),
    }
    (ARTIFACTS / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    _load_runtime.cache_clear()
    print("Wrote artifacts/manifest.json.")


@lru_cache(maxsize=1)
def _load_runtime():
    """Load local artifacts once per Python process; no checkpoint is loaded."""
    from gliner2.processor import SchemaTransformer
    from transformers import AutoTokenizer

    manifest = json.loads((ARTIFACTS / "manifest.json").read_text(encoding="utf-8"))
    tokenizer = AutoTokenizer.from_pretrained(
        ARTIFACTS / "tokenizer", local_files_only=True
    )
    processor = SchemaTransformer(
        tokenizer=tokenizer, token_pooling=manifest["token_pooling"]
    )
    options = ort.SessionOptions()
    options.intra_op_num_threads = 1  # Same CPU default as DevBuilder.
    session = ort.InferenceSession(
        str(ARTIFACTS / manifest["artifact_filename"]),
        sess_options=options,
        providers=["CPUExecutionProvider"],
    )
    return processor, session, manifest


def run_decider(text, schema, include_confidence=True):
    """Run the notebook's classification schemas through the local ORT graph.

    Supports single-label and multi-label task dictionaries. This does not
    implement entity extraction or the separate constrained Classifier API.
    """
    processor, session, manifest = _load_runtime()
    feed, layout = _prepare_inputs(processor, text, schema, manifest["max_input_tokens"])
    logits = session.run(["logits"], feed)[0]
    result, offset = {}, 0
    for configuration, labels in layout:
        values = logits[offset:offset + len(labels)].astype(np.float64)
        offset += len(labels)
        multi = configuration.get("multi_label", False)
        activation = configuration.get("class_act", "auto")
        if activation == "sigmoid" or (activation == "auto" and multi):
            probabilities = np.exp(-np.logaddexp(0, -values))
        elif activation in ("softmax", "auto"):
            exponentials = np.exp(values - values.max())
            probabilities = exponentials / exponentials.sum()
        else:
            raise ValueError(f"Unsupported activation: {activation}")

        if multi:
            selected = np.flatnonzero(
                probabilities >= configuration.get("cls_threshold", 0.5)
            ).tolist()
            # Match GLiNER's classify_text fallback when no label clears the threshold.
            if not selected:
                selected = [int(probabilities.argmax())]
        else:
            selected = [int(probabilities.argmax())]
        answers = [
            {"label": labels[i], "confidence": float(probabilities[i])}
            if include_confidence else labels[i]
            for i in selected
        ]
        result[configuration["task"]] = answers if multi else answers[0]
    return result


def main():
    metadata = export_decider()
    write_manifest(metadata)
    print("\nLocal ONNX Runtime prediction:")
    print(json.dumps(run_decider(DEMO_TEXT, DEMO_SCHEMA), indent=2))


if __name__ == "__main__":
    main()
