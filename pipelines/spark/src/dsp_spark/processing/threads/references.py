"""Read pinned, reviewed inputs. Synthetic reference fixtures stay isolated."""

from dsp_collector.normalizers.observation import parse_json
from dsp_collector.runtime.durable import ArtifactStore, sha256

from .features import INTENTS, METHOD


def pinned(reference):
    store = ArtifactStore(reference["base_uri"])
    raw = store.read(reference["artifact_uri"])
    if sha256(raw) != reference["content_sha256"]:
        raise ValueError("Reference input checksum mismatch")
    return parse_json(raw)


def load_aliases(config, *, fixture=False):
    if config.get("admin_aliases"):
        raise ValueError("Inline administrative assertions are not reviewed references")
    reference = config.get("admin_reference")
    if not reference:
        return []
    value = pinned(reference)
    if (
        value.get("status") != "REVIEWED"
        or not value.get("review_reference")
        or not value.get("reference_version")
    ):
        raise ValueError("Administrative reference review is missing")
    if value.get("origin") == "SYNTHETIC" and not fixture:
        raise PermissionError("Synthetic admin units cannot enter a live generation")
    units = {
        (u["admin_version"], u["admin_unit_id"])
        for u in value["units"]
        if u["level"] == "PROVINCE" and u["admin_version"] in ("PRE_2025", "POST_2025")
    }
    aliases = []
    for item in value["aliases"]:
        if (item["admin_version"], item["admin_unit_id"]) not in units or not item[
            "alias"
        ].strip():
            raise ValueError("Alias has no actual unit in this reviewed reference")
        aliases.append(
            {**item, "reviewed": True, "reference_version": value["reference_version"]}
        )
    return aliases


def load_annotations(config, *, fixture=False):
    reference = config.get("classification_reference")
    if not reference:
        return {}
    value = pinned(reference)
    if value.get("origin") == "SYNTHETIC" and not fixture:
        raise PermissionError(
            "Synthetic annotation evidence cannot qualify live labels"
        )
    if (
        value.get("classifier_version") != METHOD
        or not value.get("reviewer")
        or not value.get("evaluation_reference")
    ):
        raise ValueError("Classifier version and reviewed evaluation evidence required")
    # Independently reconcile recorded held-out predictions and manual labels.
    examples = value["evaluation_examples"]
    if not examples or len({e["example_id"] for e in examples}) != len(examples):
        raise ValueError("Unique manually labeled evaluation examples required")
    tp = sum(
        bool(e["related_prediction"]) and bool(e["related_gold"]) for e in examples
    )
    predicted = sum(bool(e["related_prediction"]) for e in examples)
    precision = tp / predicted if predicted else 0
    scores = []
    for label in INTENTS:
        t = sum(
            label in e["intent_prediction"] and label in e["intent_gold"]
            for e in examples
        )
        p = sum(label in e["intent_prediction"] for e in examples)
        g = sum(label in e["intent_gold"] for e in examples)
        scores.append(2 * t / (p + g) if p + g else 0)
    if precision < 0.90 or sum(scores) / len(scores) < 0.80:
        raise ValueError(
            "Evaluation does not meet relevance precision / 13-label macro-F1 targets"
        )
    annotations = {}
    for row in value["annotations"]:
        if row["content_id"] in annotations or not set(row["intent_labels"]).issubset(
            INTENTS
        ):
            raise ValueError("Duplicate or unknown intent annotation")
        if not row.get("text_hash") or not row.get("evidence_spans"):
            raise ValueError("Annotation needs own-text hash and evidence spans")
        annotations[row["content_id"]] = {
            **row,
            "evaluation_reference": value["evaluation_reference"],
        }
    return annotations


def qualify(value, annotations):
    annotation = annotations.get(value["content_id"])
    if not annotation:
        return value
    if annotation["text_hash"] != value["text_hash"]:
        return value  # An old annotation does not qualify edited content.
    if any(
        span not in value["text_normalized"]
        for span in annotation["evidence_spans"].values()
    ):
        raise ValueError("Classification evidence is outside own text")
    return {
        **value,
        "intent_labels": annotation["intent_labels"],
        "is_pickleball_related": annotation["is_pickleball_related"],
        "classification_status": "EVALUATED_QUALIFIED",
        "sentiment": annotation.get("sentiment"),
    }
