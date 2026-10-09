"""One inference smoke test. Run deploy_decider.py first."""

import json
import math

from deploy_decider import run_decider


def test_decider_inference():
    schema = {
        "sentiment": ["positive", "negative", "neutral"],
        "topics": {
            "labels": ["delivery", "product_quality", "billing", "support"],
            "multi_label": True,
            "cls_threshold": 0.5,
        },
    }
    result = run_decider(
        "Delivery was prompt and the product works beautifully.", schema
    )
    print(json.dumps(result, indent=2))

    assert set(result) == set(schema)
    assert isinstance(result["sentiment"], dict)
    assert isinstance(result["topics"], list) and result["topics"]
    for name, answers in result.items():
        allowed = schema[name] if name == "sentiment" else schema[name]["labels"]
        for answer in answers if isinstance(answers, list) else [answers]:
            assert answer["label"] in allowed
            assert math.isfinite(answer["confidence"])
            assert 0.0 <= answer["confidence"] <= 1.0
