#!/usr/bin/env python3
"""
Agent 2: DQ Execution & Enrichment Expert (Standalone Script)

Features:
- Reads validation rules from YAML (from Agent 1)
- Executes rules across all entities (Excel sheets)
- Generates per-entity issues CSVs and a combined issues CSV
- Converts profiling JSON to per-entity profile CSVs and a summary CSV
- Produces remediation suggestions using Groq Chat Completions API, with caching and fallbacks

Inputs (defaults, override via CLI):
- --excel outputs/input.xlsx             (path to Excel data)
- --rules outputs/quality_validation_instructions.yaml
- --profiling outputs/profiling_results.json
- --output outputs

Outputs:
- outputs/issues/{entity}_issues.csv
- outputs/quality_issues_with_remediation.csv
- outputs/profiles/{entity}_profile.csv
- outputs/profiles/profile_summary.csv

Note:
- This script does not require CrewAI; it runs standalone.
- Remediation suggestions use Groq at https://api.groq.com/openai/v1/chat/completions by default.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
import numpy as np

try:
    import yaml  # type: ignore
except Exception as exc:  # pragma: no cover
    raise RuntimeError("PyYAML is required. Install with: pip install pyyaml") from exc

try:
    import requests  # type: ignore
except Exception as exc:  # pragma: no cover
    raise RuntimeError("requests is required. Install with: pip install requests") from exc


# ----------------------------- Constants & Types ----------------------------- #

SEVERITY_ORDER = {
    "CRITICAL": 0,
    "HIGH": 1,
    "MEDIUM": 2,
    "LOW": 3,
}

DEFAULT_SEVERITY = "MEDIUM"

SUPPORTED_RULE_TYPES = {
    "NOT_NULL",
    "UNIQUE",
    "RANGE_CHECK",
    "ALLOWED_VALUES",
    "DATE_LOGIC",
    "FOREIGN_KEY",
    "PATTERN_MATCH",
}

EXCEL_HEADER_OFFSET = 2  # Data row 0 -> Excel row 2 (header is row 1)


# ------------------------------- Data Classes -------------------------------- #

@dataclass
class Rule:
    rule_id: str
    rule_type: str
    entity: str
    columns: List[str] = field(default_factory=list)
    params: Dict[str, Any] = field(default_factory=dict)
    severity: str = DEFAULT_SEVERITY
    description: Optional[str] = None


# ---------------------------- Utility Functions ----------------------------- #

def _safe_ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _to_list(value: Union[str, Iterable[str]]) -> List[str]:
    if isinstance(value, str):
        return [value]
    return list(value)


def _normalize_severity(value: Optional[str]) -> str:
    if not value:
        return DEFAULT_SEVERITY
    v = str(value).strip().upper()
    return v if v in SEVERITY_ORDER else DEFAULT_SEVERITY


def _is_null_like(val: Any) -> bool:
    if pd.isna(val):
        return True
    if isinstance(val, str) and val.strip() == "":
        return True
    return False


def _coerce_numeric(series: pd.Series) -> Tuple[pd.Series, pd.Series]:
    coerced = pd.to_numeric(series, errors="coerce")
    non_numeric_mask = series.notna() & coerced.isna()
    return coerced, non_numeric_mask


def _coerce_datetime(series: pd.Series) -> Tuple[pd.Series, pd.Series]:
    coerced = pd.to_datetime(series, errors="coerce")
    invalid_mask = series.notna() & coerced.isna()
    return coerced, invalid_mask


# ------------------------------ YAML Rule Loader ----------------------------- #

def load_rules_from_yaml(rules_path: str) -> List[Rule]:
    if not os.path.exists(rules_path):
        raise FileNotFoundError(f"Rules YAML not found: {rules_path}")

    with open(rules_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    rules: List[Rule] = []

    def parse_rule(raw: Dict[str, Any], default_entity: Optional[str]) -> Optional[Rule]:
        rule_type = str(raw.get("type", "")).strip().upper()
        if rule_type not in SUPPORTED_RULE_TYPES:
            return None

        rule_id = str(raw.get("id") or raw.get("rule_id") or "").strip() or f"RULE_{len(rules)+1:03d}"
        entity = str(raw.get("entity") or default_entity or "").strip()
        if not entity:
            return None

        # columns / column
        cols: List[str] = []
        if "columns" in raw and raw["columns"] is not None:
            cols = _to_list(raw["columns"])
        elif "column" in raw and raw["column"] is not None:
            cols = _to_list(raw["column"])

        params = dict(raw)
        for k in ["id", "rule_id", "type", "entity", "columns", "column", "severity", "description"]:
            params.pop(k, None)

        severity = _normalize_severity(raw.get("severity"))
        description = raw.get("description")

        return Rule(
            rule_id=rule_id,
            rule_type=rule_type,
            entity=entity,
            columns=cols,
            params=params,
            severity=severity,
            description=description,
        )

    # Flexible parsing across common schemas
    if isinstance(data, dict):
        if "entities" in data and isinstance(data["entities"], list):
            for ent in data["entities"]:
                if not isinstance(ent, dict):
                    continue
                name = ent.get("name") or ent.get("entity") or ent.get("sheet")
                if not name:
                    # Also support { entity_name: { rules: [...] } }
                    if len(ent.keys()) == 1:
                        name = list(ent.keys())[0]
                        ent = ent[name]  # type: ignore
                name = str(name)
                ent_rules = ent.get("rules") if isinstance(ent, dict) else None
                if isinstance(ent_rules, list):
                    for r in ent_rules:
                        if isinstance(r, dict):
                            rule = parse_rule(r, default_entity=name)
                            if rule:
                                rules.append(rule)
        elif "rules" in data and isinstance(data["rules"], list):
            for r in data["rules"]:
                if isinstance(r, dict):
                    rule = parse_rule(r, default_entity=r.get("entity"))
                    if rule:
                        rules.append(rule)
        else:
            # Assume mapping { entity_name: [rules...] }
            for ent_name, ent_rules in data.items():
                if isinstance(ent_rules, list):
                    for r in ent_rules:
                        if isinstance(r, dict):
                            rule = parse_rule(r, default_entity=str(ent_name))
                            if rule:
                                rules.append(rule)
    elif isinstance(data, list):
        # list of {entity, rules}
        for ent in data:
            if not isinstance(ent, dict):
                continue
            name = ent.get("name") or ent.get("entity") or ent.get("sheet")
            if not name:
                continue
            name = str(name)
            ent_rules = ent.get("rules")
            if isinstance(ent_rules, list):
                for r in ent_rules:
                    if isinstance(r, dict):
                        rule = parse_rule(r, default_entity=name)
                        if rule:
                            rules.append(rule)

    return rules


# ------------------------------- LLM Remediation ------------------------------ #

class RemediationLLM:
    """LLM client for remediation suggestions using Groq Chat Completions API."""

    def __init__(
        self,
        groq_api_key: Optional[str],
        model: str = "llama-3.1-8b-instant",
        temperature: float = 0.2,
        timeout_sec: int = 30,
        cache_path: Optional[str] = None,
        base_url: str = "https://api.groq.com/openai/v1",
        max_tokens: int = 120,
    ) -> None:
        self.groq_api_key = groq_api_key
        self.model = model
        self.temperature = temperature
        self.timeout_sec = timeout_sec
        self.cache_path = cache_path
        self.base_url = base_url.rstrip("/")
        self.max_tokens = max_tokens
        self.cache: Dict[str, str] = {}
        if cache_path and os.path.exists(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    self.cache = json.load(f)
            except Exception:
                self.cache = {}

    def _save_cache(self) -> None:
        if not self.cache_path:
            return
        try:
            _safe_ensure_dir(os.path.dirname(self.cache_path))
            with open(self.cache_path, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def suggest(self, prompt_key: str, prompt_text: str) -> str:
        if prompt_key in self.cache:
            return self.cache[prompt_key]

        if not self.groq_api_key:
            # No key available: fallback
            suggestion = "Review source process and correct the value to meet the rule criteria."
            self.cache[prompt_key] = suggestion
            self._save_cache()
            return suggestion

        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.groq_api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a pharmaceutical data quality remediation expert. "
                        "Provide concise, specific, action-oriented fixes that respect GMP and FDA guidance. "
                        "Tailor the suggestion to the rule type and column domain. "
                        "Prefer operational actions (e.g., recalibrate instrument, update master data, correct mapping). "
                        "Do NOT change the data or propose generic messages."
                    ),
                },
                {"role": "user", "content": prompt_text},
            ],
        }

        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=self.timeout_sec)
            resp.raise_for_status()
            data = resp.json()
            suggestion = (
                (data.get("choices") or [{}])[0]
                .get("message", {})
                .get("content", "")
                .strip()
            )
            if not suggestion:
                raise ValueError("Empty response from LLM")
        except Exception:
            # Fallback minimal heuristic message
            suggestion = "Review source process and correct the value to meet the rule criteria."

        # Cache and persist
        self.cache[prompt_key] = suggestion
        self._save_cache()
        return suggestion


def build_remediation_prompt(
    entity: str,
    column_name: str,
    rule_type: str,
    description: Optional[str],
    actual_value: Any,
    expected_value: Any,
    severity: str,
) -> Tuple[str, str]:
    # Key for cache; keep it compact but specific
    key_parts = [
        entity,
        column_name,
        rule_type,
        str(description or ""),
        str(expected_value),
        severity,
    ]
    prompt_key = "|".join(key_parts)

    prompt = (
        "Context: Pharmaceutical manufacturing dataset under GMP / FDA 21 CFR Part 11.\n"
        f"Entity: {entity}\n"
        f"Column: {column_name}\n"
        f"Rule type: {rule_type}\n"
        f"Severity: {severity}\n"
        f"Rule description: {description or 'N/A'}\n"
        f"Observed value: {actual_value!r}\n"
        f"Expected/Constraint: {expected_value!r}\n\n"
        "Task: Provide ONE short, specific, actionable remediation suggestion (max 25 words). "
        "Do not be generic; mention process/instrument/master data/system fix where relevant."
    )
    return prompt_key, prompt


# ---------------------------- Rule Execution Engine --------------------------- #

def execute_rules(
    excel_path: str,
    rules: List[Rule],
    output_dir: str,
    remediation_llm: Optional[RemediationLLM] = None,
) -> pd.DataFrame:
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"Excel file not found: {excel_path}")

    # Load all sheets as entities
    sheets: Dict[str, pd.DataFrame] = pd.read_excel(excel_path, sheet_name=None)

    # Normalize column names to strings (keep original capitalization)
    for name, df in sheets.items():
        df.columns = [str(c) for c in df.columns]
        sheets[name] = df

    all_issues: List[Dict[str, Any]] = []

    # Precompute reference sets for FK checks by entity->column->set
    ref_values: Dict[Tuple[str, str], set] = {}

    def get_ref_values(entity: str, column: str) -> set:
        key = (entity, column)
        if key in ref_values:
            return ref_values[key]
        ref_set: set = set()
        if entity in sheets and column in sheets[entity].columns:
            ref_series = sheets[entity][column].dropna()
            ref_set = set(ref_series.unique().tolist())
        ref_values[key] = ref_set
        return ref_set

    for rule in rules:
        if rule.entity not in sheets:
            print(f"[WARN] Entity '{rule.entity}' not found in Excel. Skipping rule {rule.rule_id}.")
            continue

        df = sheets[rule.entity]
        rule_type = rule.rule_type
        severity = rule.severity

        # Execute each rule type
        if rule_type == "NOT_NULL":
            for col in rule.columns:
                if col not in df.columns:
                    print(f"[WARN] Column '{col}' not in entity '{rule.entity}' for rule {rule.rule_id}")
                    continue
                mask = df[col].apply(_is_null_like)
                for idx in df[mask].index:
                    actual = df.loc[idx, col]
                    issue = {
                        "row_number": int(idx) + EXCEL_HEADER_OFFSET,
                        "entity": rule.entity,
                        "column_name": col,
                        "rule_id": rule.rule_id,
                        "rule_violated": rule_type,
                        "issue_description": rule.description or f"Value is null/empty in '{col}'.",
                        "actual_value": actual,
                        "expected_value": "NOT NULL",
                        "severity": severity,
                    }
                    issue["remediation_suggestion"] = _maybe_remediate(remediation_llm, issue)
                    all_issues.append(issue)

        elif rule_type == "UNIQUE":
            subset = rule.columns if rule.columns else df.columns.tolist()
            for col in subset:
                if col not in df.columns:
                    print(f"[WARN] Column '{col}' not in entity '{rule.entity}' for rule {rule.rule_id}")
            valid_cols = [c for c in subset if c in df.columns]
            if not valid_cols:
                continue
            dup_mask = df.duplicated(subset=valid_cols, keep=False)
            # Exclude rows where any col in subset is null-like (often uniqueness ignores nulls)
            null_any = df[valid_cols].applymap(_is_null_like).any(axis=1)
            viol_idx = df.index[dup_mask & ~null_any]
            for idx in viol_idx:
                actual_vals = {c: df.loc[idx, c] for c in valid_cols}
                issue = {
                    "row_number": int(idx) + EXCEL_HEADER_OFFSET,
                    "entity": rule.entity,
                    "column_name": ",".join(valid_cols),
                    "rule_id": rule.rule_id,
                    "rule_violated": rule_type,
                    "issue_description": rule.description or f"Duplicate value(s) in {valid_cols}.",
                    "actual_value": json.dumps(actual_vals, ensure_ascii=False),
                    "expected_value": "UNIQUE across dataset",
                    "severity": severity,
                }
                issue["remediation_suggestion"] = _maybe_remediate(remediation_llm, issue)
                all_issues.append(issue)

        elif rule_type == "RANGE_CHECK":
            # Params: min, max, inclusive (default True)
            inclusive = bool(rule.params.get("inclusive", True))
            for col in rule.columns:
                if col not in df.columns:
                    print(f"[WARN] Column '{col}' not in entity '{rule.entity}' for rule {rule.rule_id}")
                    continue
                series = df[col]
                numeric, non_numeric_mask = _coerce_numeric(series)

                # Non-numeric where value present -> violation
                for idx in df[non_numeric_mask].index:
                    actual = df.loc[idx, col]
                    issue = {
                        "row_number": int(idx) + EXCEL_HEADER_OFFSET,
                        "entity": rule.entity,
                        "column_name": col,
                        "rule_id": rule.rule_id,
                        "rule_violated": rule_type,
                        "issue_description": rule.description or f"Non-numeric value in numeric range column '{col}'.",
                        "actual_value": actual,
                        "expected_value": {
                            "min": rule.params.get("min"),
                            "max": rule.params.get("max"),
                            "inclusive": inclusive,
                        },
                        "severity": severity,
                    }
                    issue["remediation_suggestion"] = _maybe_remediate(remediation_llm, issue)
                    all_issues.append(issue)

                min_v = rule.params.get("min")
                max_v = rule.params.get("max")
                lower_mask = pd.Series(False, index=df.index)
                upper_mask = pd.Series(False, index=df.index)
                if min_v is not None:
                    lower_mask = numeric < float(min_v) if inclusive else numeric <= float(min_v)
                if max_v is not None:
                    upper_mask = numeric > float(max_v) if inclusive else numeric >= float(max_v)
                viol_mask = (lower_mask | upper_mask) & series.notna()

                for idx in df[viol_mask].index:
                    actual = df.loc[idx, col]
                    exp_val = {
                        "min": min_v,
                        "max": max_v,
                        "inclusive": inclusive,
                    }
                    issue = {
                        "row_number": int(idx) + EXCEL_HEADER_OFFSET,
                        "entity": rule.entity,
                        "column_name": col,
                        "rule_id": rule.rule_id,
                        "rule_violated": rule_type,
                        "issue_description": rule.description or f"Value outside allowed range for '{col}'.",
                        "actual_value": actual,
                        "expected_value": exp_val,
                        "severity": severity,
                    }
                    issue["remediation_suggestion"] = _maybe_remediate(remediation_llm, issue)
                    all_issues.append(issue)

        elif rule_type == "ALLOWED_VALUES":
            allowed: List[Any] = _to_list(rule.params.get("allowed_values", []))
            case_insensitive = bool(rule.params.get("case_insensitive", False))
            for col in rule.columns:
                if col not in df.columns:
                    print(f"[WARN] Column '{col}' not in entity '{rule.entity}' for rule {rule.rule_id}")
                    continue
                series = df[col]
                if case_insensitive:
                    allowed_norm = {str(v).casefold() for v in allowed}
                    in_allowed = series.astype(str).fillna("").apply(lambda s: s.casefold() in allowed_norm)
                else:
                    in_allowed = series.isin(allowed)
                viol_mask = (~in_allowed) & series.notna()
                for idx in df[viol_mask].index:
                    actual = df.loc[idx, col]
                    issue = {
                        "row_number": int(idx) + EXCEL_HEADER_OFFSET,
                        "entity": rule.entity,
                        "column_name": col,
                        "rule_id": rule.rule_id,
                        "rule_violated": rule_type,
                        "issue_description": rule.description or f"Value not in allowed set for '{col}'.",
                        "actual_value": actual,
                        "expected_value": allowed,
                        "severity": severity,
                    }
                    issue["remediation_suggestion"] = _maybe_remediate(remediation_llm, issue)
                    all_issues.append(issue)

        elif rule_type == "DATE_LOGIC":
            # Params: earlier_column, later_column, relation in {"lt","lte","eq","gte","gt"}
            earlier_col = rule.params.get("earlier_column")
            later_col = rule.params.get("later_column")
            relation = str(rule.params.get("relation", "lte")).lower()  # default earlier <= later
            if not earlier_col or not later_col:
                print(f"[WARN] DATE_LOGIC requires 'earlier_column' and 'later_column' in rule {rule.rule_id}")
                continue
            if earlier_col not in df.columns or later_col not in df.columns:
                print(f"[WARN] Columns '{earlier_col}'/'{later_col}' missing in entity '{rule.entity}'")
                continue
            early, early_invalid = _coerce_datetime(df[earlier_col])
            late, late_invalid = _coerce_datetime(df[later_col])
            # Invalid date formats are violations
            invalid_idx = df.index[early_invalid | late_invalid]
            for idx in invalid_idx:
                actual = {"earlier": df.loc[idx, earlier_col], "later": df.loc[idx, later_col]}
                exp = {"relation": relation, "earlier_column": earlier_col, "later_column": later_col}
                issue = {
                    "row_number": int(idx) + EXCEL_HEADER_OFFSET,
                    "entity": rule.entity,
                    "column_name": f"{earlier_col},{later_col}",
                    "rule_id": rule.rule_id,
                    "rule_violated": rule_type,
                    "issue_description": rule.description or "Invalid date format(s).",
                    "actual_value": json.dumps(actual, ensure_ascii=False),
                    "expected_value": exp,
                    "severity": severity,
                }
                issue["remediation_suggestion"] = _maybe_remediate(remediation_llm, issue)
                all_issues.append(issue)

            comp_mask = pd.Series(False, index=df.index)
            if relation == "lt":
                comp_mask = ~(early < late)
            elif relation == "lte":
                comp_mask = ~(early <= late)
            elif relation == "eq":
                comp_mask = ~(early == late)
            elif relation == "gte":
                comp_mask = ~(early >= late)
            elif relation == "gt":
                comp_mask = ~(early > late)
            else:
                print(f"[WARN] Unsupported relation '{relation}' in DATE_LOGIC for rule {rule.rule_id}")
                continue
            comp_mask = comp_mask & early.notna() & late.notna()
            for idx in df[comp_mask].index:
                actual = {"earlier": df.loc[idx, earlier_col], "later": df.loc[idx, later_col]}
                exp = {"relation": relation, "earlier_column": earlier_col, "later_column": later_col}
                issue = {
                    "row_number": int(idx) + EXCEL_HEADER_OFFSET,
                    "entity": rule.entity,
                    "column_name": f"{earlier_col},{later_col}",
                    "rule_id": rule.rule_id,
                    "rule_violated": rule_type,
                    "issue_description": rule.description or f"Date relationship violated: {earlier_col} {relation} {later_col}.",
                    "actual_value": json.dumps(actual, ensure_ascii=False),
                    "expected_value": exp,
                    "severity": severity,
                }
                issue["remediation_suggestion"] = _maybe_remediate(remediation_llm, issue)
                all_issues.append(issue)

        elif rule_type == "FOREIGN_KEY":
            # Params: referenced_entity/referent_entity/parent_entity, referenced_column/parent_column
            ref_entity = (
                rule.params.get("referenced_entity")
                or rule.params.get("parent_entity")
                or rule.params.get("target_entity")
            )
            ref_column = (
                rule.params.get("referenced_column")
                or rule.params.get("parent_column")
                or rule.params.get("target_column")
            )
            if not ref_entity or not ref_column or not rule.columns:
                print(f"[WARN] FOREIGN_KEY requires columns and referenced entity/column in rule {rule.rule_id}")
                continue
            if ref_entity not in sheets or ref_column not in sheets.get(ref_entity, pd.DataFrame()).columns:
                print(f"[WARN] Referenced {ref_entity}.{ref_column} not found for rule {rule.rule_id}")
                continue
            ref_set = get_ref_values(str(ref_entity), str(ref_column))
            for col in rule.columns:
                if col not in df.columns:
                    print(f"[WARN] Column '{col}' not in entity '{rule.entity}' for rule {rule.rule_id}")
                    continue
                series = df[col]
                non_null_mask = series.notna()
                not_found_mask = ~series.isin(list(ref_set)) & non_null_mask
                for idx in df[not_found_mask].index:
                    actual = df.loc[idx, col]
                    exp = f"Value must exist in {ref_entity}.{ref_column}"
                    issue = {
                        "row_number": int(idx) + EXCEL_HEADER_OFFSET,
                        "entity": rule.entity,
                        "column_name": col,
                        "rule_id": rule.rule_id,
                        "rule_violated": rule_type,
                        "issue_description": rule.description or f"Foreign key not found in {ref_entity}.{ref_column}.",
                        "actual_value": actual,
                        "expected_value": exp,
                        "severity": severity,
                    }
                    issue["remediation_suggestion"] = _maybe_remediate(remediation_llm, issue)
                    all_issues.append(issue)

        elif rule_type == "PATTERN_MATCH":
            pattern = rule.params.get("pattern") or rule.params.get("regex")
            if not pattern:
                print(f"[WARN] PATTERN_MATCH requires 'pattern' in rule {rule.rule_id}")
                continue
            flags = re.IGNORECASE if bool(rule.params.get("case_insensitive", False)) else 0
            try:
                regex = re.compile(str(pattern), flags)
            except re.error:
                print(f"[WARN] Invalid regex pattern in rule {rule.rule_id}: {pattern}")
                continue
            for col in rule.columns:
                if col not in df.columns:
                    print(f"[WARN] Column '{col}' not in entity '{rule.entity}' for rule {rule.rule_id}")
                    continue
                series = df[col].astype(str)
                # Consider empty strings/nulls as violations for pattern rules only if value is not null
                non_null_mask = df[col].notna() & (series.str.strip() != "")
                matches = series.str.match(regex)
                viol_mask = (~matches) & non_null_mask
                for idx in df[viol_mask].index:
                    actual = df.loc[idx, col]
                    issue = {
                        "row_number": int(idx) + EXCEL_HEADER_OFFSET,
                        "entity": rule.entity,
                        "column_name": col,
                        "rule_id": rule.rule_id,
                        "rule_violated": rule_type,
                        "issue_description": rule.description or f"Value does not match required pattern for '{col}'.",
                        "actual_value": actual,
                        "expected_value": f"Pattern: {pattern}",
                        "severity": severity,
                    }
                    issue["remediation_suggestion"] = _maybe_remediate(remediation_llm, issue)
                    all_issues.append(issue)
        else:
            print(f"[WARN] Unsupported rule type encountered: {rule_type}")

    # Create outputs
    issues_df = pd.DataFrame(all_issues)
    if not issues_df.empty:
        # Severity sort
        issues_df["_sev_order"] = issues_df["severity"].map(lambda s: SEVERITY_ORDER.get(str(s).upper(), 99))
        issues_df.sort_values(by=["_sev_order", "entity", "row_number", "rule_id"], inplace=True)
        issues_df.drop(columns=["_sev_order"], inplace=True)

    # Per-entity CSVs
    issues_dir = os.path.join(output_dir, "issues")
    _safe_ensure_dir(issues_dir)
    if not issues_df.empty:
        for entity, grp in issues_df.groupby("entity", sort=True):
            entity_file = os.path.join(issues_dir, f"{entity}_issues.csv")
            grp.to_csv(entity_file, index=False)
    else:
        # Touch the directory; no files if no issues
        pass

    # Combined report
    combined_file = os.path.join(output_dir, "quality_issues_with_remediation.csv")
    if not issues_df.empty:
        issues_df.to_csv(combined_file, index=False)
    else:
        # Write an empty file with header for consistency
        empty_cols = [
            "row_number",
            "entity",
            "column_name",
            "rule_id",
            "rule_violated",
            "issue_description",
            "actual_value",
            "expected_value",
            "severity",
            "remediation_suggestion",
        ]
        pd.DataFrame(columns=empty_cols).to_csv(combined_file, index=False)

    return issues_df


def _maybe_remediate(remediation_llm: Optional[RemediationLLM], issue: Dict[str, Any]) -> str:
    if remediation_llm is None:
        return "Review SOPs and correct the value per rule."

    prompt_key, prompt_text = build_remediation_prompt(
        entity=str(issue.get("entity")),
        column_name=str(issue.get("column_name")),
        rule_type=str(issue.get("rule_violated")),
        description=str(issue.get("issue_description")),
        actual_value=issue.get("actual_value"),
        expected_value=issue.get("expected_value"),
        severity=str(issue.get("severity")),
    )
    return remediation_llm.suggest(prompt_key, prompt_text)


# --------------------------- Profiling JSON Conversion ------------------------ #

def load_profiling_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Profiling JSON not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _infer_entity_blocks(profile_obj: Any) -> Dict[str, Any]:
    # Try common shapes:
    # 1) { "entities": { "Batch": {"columns": {...}} } }
    # 2) { "Batch": {"columns": {...}}, "API": { ... } }
    if isinstance(profile_obj, dict):
        if "entities" in profile_obj and isinstance(profile_obj["entities"], dict):
            return profile_obj["entities"]
        # assume entity-level mapping
        return profile_obj
    return {}


def convert_profiling_to_csv(
    profiling_json_path: str,
    output_dir: str,
    sheets: Optional[Dict[str, pd.DataFrame]] = None,
) -> pd.DataFrame:
    profiles_dir = os.path.join(output_dir, "profiles")
    _safe_ensure_dir(profiles_dir)

    obj = load_profiling_json(profiling_json_path)
    entity_blocks = _infer_entity_blocks(obj)

    summary_rows: List[Dict[str, Any]] = []

    for entity_name, ent in entity_blocks.items():
        # Attempt to extract column stats mapping
        columns_block = None
        total_rows: Optional[int] = None
        if isinstance(ent, dict):
            # Flexible keys: 'columns', 'stats', 'metrics', etc.
            for key in ["columns", "stats", "metrics", "profile", "columns_profile"]:
                if key in ent and isinstance(ent[key], (dict, list)):
                    columns_block = ent[key]
                    break
            if "total_rows" in ent:
                try:
                    total_rows = int(ent["total_rows"])  # type: ignore
                except Exception:
                    total_rows = None

        rows: List[Dict[str, Any]] = []
        # Case 1: dict mapping column->stats
        if isinstance(columns_block, dict):
            for col_name, col_stats in columns_block.items():
                row = _profile_row_from_stats(
                    entity_name, str(col_name), col_stats, total_rows, sheets
                )
                rows.append(row)
        # Case 2: list of {column_name, ...}
        elif isinstance(columns_block, list):
            for item in columns_block:
                if isinstance(item, dict):
                    col_name = item.get("column_name") or item.get("name") or item.get("column")
                    if col_name is None:
                        continue
                    row = _profile_row_from_stats(
                        entity_name, str(col_name), item, total_rows, sheets
                    )
                    rows.append(row)
        else:
            # No columns block; compute minimal stats from sheets if available
            if sheets and entity_name in sheets:
                df = sheets[entity_name]
                total_rows = int(len(df))
                for col in df.columns:
                    row = _compute_profile_row_from_df(entity_name, col, df[col])
                    rows.append(row)

        if rows:
            df_entity = pd.DataFrame(rows)
            df_entity = df_entity[
                [
                    "column_name",
                    "data_type",
                    "total_rows",
                    "null_count",
                    "null_percentage",
                    "completeness_score",
                    "distinct_count",
                    "distinct_percentage",
                    "min_value",
                    "max_value",
                    "mean_value",
                    "median_value",
                    "std_dev",
                    "most_common_value",
                    "most_common_count",
                    "most_common_percentage",
                    "detected_pattern",
                ]
            ]
            df_entity.to_csv(os.path.join(profiles_dir, f"{entity_name}_profile.csv"), index=False)

            # Build summary for this entity
            summary_rows.append(
                {
                    "entity": entity_name,
                    "total_rows": int(df_entity["total_rows"].max() or 0),
                    "total_columns": int(len(df_entity)),
                    "total_nulls": int(df_entity["null_count"].sum()),
                    "avg_completeness": float(df_entity["completeness_score"].mean()),
                }
            )

    # Summary CSV
    summary_df = pd.DataFrame(summary_rows)
    if not summary_df.empty:
        summary_df.sort_values(by=["entity"], inplace=True)
    summary_df.to_csv(os.path.join(profiles_dir, "profile_summary.csv"), index=False)

    return summary_df


def _profile_row_from_stats(
    entity: str,
    column_name: str,
    stats: Dict[str, Any],
    total_rows_hint: Optional[int],
    sheets: Optional[Dict[str, pd.DataFrame]],
) -> Dict[str, Any]:
    # Try to map a variety of JSON shapes into canonical columns
    data_type = str(
        stats.get("data_type")
        or stats.get("dtype")
        or stats.get("type")
        or "object"
    )

    # nulls
    null_count = stats.get("null_count") or stats.get("missing_count") or 0
    try:
        null_count = int(null_count)
    except Exception:
        null_count = 0

    # distinct
    distinct_count = stats.get("distinct_count") or stats.get("unique_count") or 0
    try:
        distinct_count = int(distinct_count)
    except Exception:
        distinct_count = 0

    # totals
    total_rows = total_rows_hint
    if total_rows is None and sheets and entity in sheets:
        total_rows = int(len(sheets[entity]))
    if total_rows is None:
        total_rows = 0

    null_percentage = (float(null_count) / float(total_rows) * 100.0) if total_rows else 0.0
    completeness_score = 100.0 - null_percentage

    # Numeric stats
    min_value = stats.get("min") or stats.get("min_value")
    max_value = stats.get("max") or stats.get("max_value")
    mean_value = stats.get("mean") or stats.get("avg") or stats.get("average")
    median_value = stats.get("median")
    std_dev = stats.get("std") or stats.get("std_dev") or stats.get("stdev")

    # Categorical stats
    most_common_value = None
    most_common_count = None
    most_common_percentage = None
    top = stats.get("top") or stats.get("mode")
    top_freq = stats.get("freq") or stats.get("top_freq")
    if top is not None:
        most_common_value = top
        try:
            most_common_count = int(top_freq)
        except Exception:
            most_common_count = None
        if most_common_count and total_rows:
            most_common_percentage = round((most_common_count / total_rows) * 100.0, 2)

    detected_pattern = stats.get("detected_pattern") or stats.get("pattern")

    # Distinct percentage
    distinct_percentage = (float(distinct_count) / float(total_rows) * 100.0) if total_rows else 0.0

    return {
        "column_name": column_name,
        "data_type": data_type,
        "total_rows": int(total_rows),
        "null_count": int(null_count),
        "null_percentage": round(null_percentage, 2),
        "completeness_score": round(completeness_score, 2),
        "distinct_count": int(distinct_count),
        "distinct_percentage": round(distinct_percentage, 2),
        "min_value": min_value,
        "max_value": max_value,
        "mean_value": mean_value,
        "median_value": median_value,
        "std_dev": std_dev,
        "most_common_value": most_common_value,
        "most_common_count": most_common_count,
        "most_common_percentage": most_common_percentage,
        "detected_pattern": detected_pattern,
    }


def _compute_profile_row_from_df(entity: str, column_name: str, series: pd.Series) -> Dict[str, Any]:
    total_rows = int(len(series))
    null_mask = series.isna() | (series.astype(str).str.strip() == "")
    null_count = int(null_mask.sum())
    null_percentage = (null_count / total_rows * 100.0) if total_rows else 0.0
    completeness_score = 100.0 - null_percentage

    distinct_count = int(series.nunique(dropna=True))
    distinct_percentage = (distinct_count / total_rows * 100.0) if total_rows else 0.0

    data_type = str(series.dtype)
    min_value = max_value = mean_value = median_value = std_dev = None
    most_common_value = most_common_count = most_common_percentage = None
    detected_pattern = None

    if pd.api.types.is_numeric_dtype(series):
        min_value = float(series.min(skipna=True)) if series.notna().any() else None
        max_value = float(series.max(skipna=True)) if series.notna().any() else None
        mean_value = float(series.mean(skipna=True)) if series.notna().any() else None
        median_value = float(series.median(skipna=True)) if series.notna().any() else None
        std_val = series.std(skipna=True)
        std_dev = float(std_val) if pd.notna(std_val) else None
    else:
        # Categorical stats
        vc = series.dropna().astype(str).value_counts()
        if not vc.empty:
            most_common_value = vc.index[0]
            most_common_count = int(vc.iloc[0])
            most_common_percentage = round((most_common_count / total_rows) * 100.0, 2) if total_rows else 0.0
        # Simple pattern heuristic (e.g., 'ABC-123' -> 'AAA-999')
        sample = series.dropna().astype(str).head(50).tolist()
        if sample:
            detected_pattern = _detect_simple_pattern(sample)

    return {
        "column_name": column_name,
        "data_type": data_type,
        "total_rows": total_rows,
        "null_count": null_count,
        "null_percentage": round(null_percentage, 2),
        "completeness_score": round(completeness_score, 2),
        "distinct_count": distinct_count,
        "distinct_percentage": round(distinct_percentage, 2),
        "min_value": min_value,
        "max_value": max_value,
        "mean_value": mean_value,
        "median_value": median_value,
        "std_dev": std_dev,
        "most_common_value": most_common_value,
        "most_common_count": most_common_count,
        "most_common_percentage": most_common_percentage,
        "detected_pattern": detected_pattern,
    }


def _detect_simple_pattern(values: List[str]) -> Optional[str]:
    # Convert characters to 'X' for letters, '#' for digits, keep some punctuation
    def transform(s: str) -> str:
        out = []
        for ch in s:
            if ch.isalpha():
                out.append('X')
            elif ch.isdigit():
                out.append('#')
            elif ch in {'-', '_', '/', '.', ' '}:  # keep simple separators
                out.append(ch)
            else:
                out.append('?')
        return ''.join(out)

    patterns = [transform(s) for s in values if s]
    if not patterns:
        return None
    # Return most frequent pattern
    vc = pd.Series(patterns).value_counts()
    return vc.index[0] if not vc.empty else None


# --------------------------------- CLI Runner -------------------------------- #

def print_execution_summary(output_dir: str) -> None:
    print("\n📊 GENERATED OUTPUTS:")

    # Profile CSVs
    print("\n   📈 Per-Entity Profile CSVs:")
    profile_dir = os.path.join(output_dir, "profiles")
    if os.path.exists(profile_dir):
        profile_files = [f for f in os.listdir(profile_dir) if f.endswith('_profile.csv')]
        if profile_files:
            for f in sorted(profile_files):
                file_path_full = os.path.join(profile_dir, f)
                file_size = os.path.getsize(file_path_full)
                print(f"      ✓ {f} ({file_size} bytes)")
        else:
            print("      (No profile files generated)")

    # Issue CSVs
    print("\n   🔍 Per-Entity Issue CSVs:")
    issues_dir = os.path.join(output_dir, "issues")
    total_issues = 0
    if os.path.exists(issues_dir):
        issue_files = [f for f in os.listdir(issues_dir) if f.endswith('_issues.csv')]
        if issue_files:
            for f in sorted(issue_files):
                file_path_full = os.path.join(issues_dir, f)
                try:
                    df = pd.read_csv(file_path_full)
                    total_issues += int(len(df))
                    print(f"      ✓ {f} ({len(df)} issues)")
                except Exception:
                    print(f"      ✓ {f}")
        else:
            print("      ✓ No issues found - data quality is excellent!")

    # Combined report
    print("\n   📋 Combined Reports:")
    combined_file = os.path.join(output_dir, "quality_issues_with_remediation.csv")
    if os.path.exists(combined_file):
        try:
            df = pd.read_csv(combined_file)
            print(f"      ✓ quality_issues_with_remediation.csv ({len(df)} total issues)")
            if len(df) > 0 and "severity" in df.columns:
                print("\n   📊 Issue Severity Breakdown:")
                severity_counts = df["severity"].value_counts()
                for severity in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
                    if severity in severity_counts:
                        count = severity_counts[severity]
                        percentage = (count / len(df)) * 100
                        print(f"      • {severity}: {count} ({percentage:.1f}%)")
                if "rule_violated" in df.columns:
                    print("\n   🔍 Top Issue Types:")
                    rule_counts = df["rule_violated"].value_counts().head(5)
                    for rule, count in rule_counts.items():
                        print(f"      • {rule}: {count}")
                if "entity" in df.columns:
                    print("\n   📊 Issues by Entity:")
                    entity_counts = df["entity"].value_counts()
                    for entity, count in entity_counts.items():
                        percentage = (count / len(df)) * 100
                        print(f"      • {entity}: {count} ({percentage:.1f}%)")
        except Exception:
            print("      ✓ quality_issues_with_remediation.csv")

    print("\n" + "=" * 80)
    print("📁 ALL FILES SAVED IN: outputs/")
    print("   - outputs/issues/ (issue CSVs)")
    print("   - outputs/profiles/ (profile CSVs)")
    print("=" * 80)
    print("\n✅ Agent 2 outputs are ready for Agent 3 (Reporting)")
    print("=" * 80)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Agent 2 - DQ Execution & Enrichment Expert")
    parser.add_argument("--excel", required=True, help="Path to Excel file with entities as sheets")
    parser.add_argument("--rules", default="outputs/quality_validation_instructions.yaml", help="Path to rules YAML from Agent 1")
    parser.add_argument("--profiling", default="outputs/profiling_results.json", help="Path to profiling JSON from Agent 1")
    parser.add_argument("--output", default="outputs", help="Output directory")
    parser.add_argument("--no-llm", action="store_true", help="Disable LLM remediation suggestions")
    parser.add_argument("--groq-model", default="llama-3.1-8b-instant", help="Groq model (default: llama-3.1-8b-instant)")
    parser.add_argument("--groq-api-key", default=None, help="Groq API key (fallback to env GROQ_API_KEY)")
    parser.add_argument("--timeout", type=int, default=30, help="LLM timeout seconds")

    args = parser.parse_args(argv)

    output_dir = args.output
    _safe_ensure_dir(output_dir)
    _safe_ensure_dir(os.path.join(output_dir, "issues"))
    _safe_ensure_dir(os.path.join(output_dir, "profiles"))

    print("=" * 80)
    print("AGENT 2: DQ EXECUTION & ENRICHMENT EXPERT")
    print("=" * 80)
    print("\nWHAT AGENT 2 DOES:")
    print("  1. Executes validation rules against your data")
    print("  2. Finds every row with data quality issues")
    print("  3. Generates issue CSVs with remediation suggestions")
    print("  4. Converts profiling JSON to user-friendly CSVs")
    print("=" * 80)

    # Verify inputs
    missing: List[str] = []
    if not os.path.exists(args.rules):
        missing.append(args.rules)
    if not os.path.exists(args.profiling):
        missing.append(args.profiling)
    for m in missing:
        print(f"   ❌ Missing: {m}")
    if missing:
        print("\n❌ ERROR: Required inputs from Agent 1 not found! Run Agent 1 first.")
        return 2

    if not os.path.exists(args.excel):
        print(f"\n❌ ERROR: Excel file not found: {args.excel}")
        return 2
    else:
        print(f"   ✓ Found: {args.excel}")

    # Load rules
    print("\n📋 Loading validation rules...")
    rules = load_rules_from_yaml(args.rules)
    if not rules:
        print("   ⚠️ No rules found. Exiting without validation.")
        return 0
    print(f"   ✓ Loaded {len(rules)} rules")

    # Prepare LLM
    remediation_llm: Optional[RemediationLLM] = None
    if not args.no_llm:
        cache_path = os.path.join(output_dir, "remediation_cache.json")
        groq_api_key = args.groq_api_key or os.getenv("GROQ_API_KEY")
        remediation_llm = RemediationLLM(
            groq_api_key=groq_api_key,
            model=args.groq_model,
            temperature=0.2,
            timeout_sec=args.timeout,
            cache_path=cache_path,
        )
        if groq_api_key:
            print(f"   ✓ Remediation LLM via Groq model: {args.groq_model}")
        else:
            print("   ⚠️ GROQ_API_KEY not set; using fallback static remediation text")
    else:
        print("   ✓ LLM remediation disabled by flag --no-llm")

    # Execute validation
    print("\n🚀 Executing data quality rules...")
    issues_df = execute_rules(
        excel_path=args.excel,
        rules=rules,
        output_dir=output_dir,
        remediation_llm=remediation_llm,
    )
    total_issues = int(len(issues_df)) if issues_df is not None else 0
    print(f"   ✓ Validation complete. Issues found: {total_issues}")

    # Convert profiling
    print("\n🧮 Generating profile CSVs from JSON...")
    # Pass sheets for fallback stats
    try:
        sheets = pd.read_excel(args.excel, sheet_name=None)
    except Exception:
        sheets = None
    _ = convert_profiling_to_csv(args.profiling, output_dir=output_dir, sheets=sheets)
    print("   ✓ Profile CSVs generated")

    # Summary
    print_execution_summary(output_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
