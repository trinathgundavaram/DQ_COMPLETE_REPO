import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict

logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    _DOTENV_AVAILABLE = True
except ImportError:
    _DOTENV_AVAILABLE = False


def _base_log_name(default: str) -> str:
    name = os.getenv("GRE_LOG_FILE", default)
    if name.lower().endswith(".log"):
        name = name[: -len(".log")]
    return name or default


def _build_run_log_path(log_dir: Path, base_name: str) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    return log_dir / f"{base_name}_{stamp}_{os.getpid()}.log"


def _prune_old_run_logs(log_dir: Path, base_name: str, retention_days: int) -> None:
    if not retention_days:
        return
    cutoff = datetime.now() - timedelta(days=retention_days)
    prefix = f"{base_name}_"
    for candidate in log_dir.glob(f"{prefix}*.log"):
        stem = candidate.name[len(prefix): -len(".log")]


        parts = stem.split("_")
        if len(parts) < 3:
            continue
        try:
            file_time = datetime.strptime("_".join(parts[:3]), "%Y%m%d_%H%M%S_%f")
        except ValueError:
            continue
        if file_time < cutoff:
            try:
                candidate.unlink()
            except OSError:
                pass


def configure_logging(level=None) -> None:
    resolved = level or os.getenv("GRE_LOG_LEVEL", "DEBUG")
    if not logging.getLogger().handlers:
        log_dir = Path(os.getenv("GRE_LOG_DIR") or (Path(__file__).resolve().parent.parent / "logs"))
        log_dir.mkdir(parents=True, exist_ok=True)
        base_name = _base_log_name("rules_engine")
        try:
            retention_days = int(os.getenv("GRE_LOG_RETENTION_DAYS", "30"))
        except ValueError:
            retention_days = 30


        _prune_old_run_logs(log_dir, base_name, retention_days)
        log_path = _build_run_log_path(log_dir, base_name)
        handler = logging.FileHandler(log_path, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        logging.basicConfig(level=resolved, handlers=[handler])
        logging.getLogger(__name__).info(
            "rules_engine logging to %s (level=%s, one file per run, retention=%s).",
            log_path, resolved, f"{retention_days} day(s)" if retention_days else "unlimited",
        )


def _load_env_file() -> None:
    if not _DOTENV_AVAILABLE:
        logger.debug("python-dotenv not installed -- skipping local .env load.")
        return

    env_path = Path(__file__).resolve().parent.parent / os.getenv("GRE_ENV_FILE", "dev.env")
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
        logger.info("Loaded local credentials from %s.", env_path)
    else:
        logger.debug("No local env file at %s -- skipping (expected in a real deployment).", env_path)


_load_env_file()


META_CONNECTION = os.getenv("GRE_META_CONNECTION", "teradata")
META_DB = os.getenv("GRE_META_DB", "CMSUNIV_FILELAND_DEV_T")


def get_meta_connection_name() -> str:
    return META_CONNECTION


def get_meta_db() -> str:
    return META_DB


GRE_ENVIRONMENT = os.getenv("GRE_ENVIRONMENT", "DEV").upper()


def get_environment() -> str:
    return GRE_ENVIRONMENT


_DEFAULT_ENV_TOKEN_VALUES = {"DEV": "dev", "QA": "qa", "INT": "int", "UAT": "", "PROD": ""}


_ENV_TOKEN_RE = re.compile(r"\$env", re.IGNORECASE)


def _env_token_value() -> str:
    override = os.getenv(f"GRE_ENV_VALUE_{GRE_ENVIRONMENT}")
    if override is not None:
        return override
    return _DEFAULT_ENV_TOKEN_VALUES.get(GRE_ENVIRONMENT, GRE_ENVIRONMENT.lower())


def resolve_database_name(authored_name: str) -> str:
    if not authored_name:
        return authored_name


    env_var = f"GRE_DB_MAP_{authored_name.upper()}"
    mapping_str = os.getenv(env_var)
    if mapping_str:
        mapping = {}
        for pair in mapping_str.split(","):
            pair = pair.strip()
            if not pair or "=" not in pair:
                continue
            env_key, db_name = pair.split("=", 1)
            mapping[env_key.strip().upper()] = db_name.strip()
        resolved = mapping.get(GRE_ENVIRONMENT)
        if resolved:
            return resolved
        logger.warning(
            "resolve_database_name: %s has no entry for GRE_ENVIRONMENT=%s -- "
            "falling through to $env/$ENV token substitution (if present).",
            env_var, GRE_ENVIRONMENT,
        )


    substituted = _apply_env_token_substitution(authored_name)
    if substituted != authored_name:
        return substituted


    return authored_name


def _apply_env_token_substitution(text: str) -> str:
    if not text or not _ENV_TOKEN_RE.search(text):
        return text

    lower_val = _env_token_value()
    upper_val = lower_val.upper()
    title_val = lower_val.capitalize()

    def _sub(match: "re.Match") -> str:
        token = match.group(0)[1:]
        if token.isupper():
            return upper_val
        if token.islower():
            return lower_val
        return title_val

    resolved = _ENV_TOKEN_RE.sub(_sub, text)
    return re.sub(r"_{2,}", "_", resolved)


def resolve_env_tokens(text: str) -> str:
    return _apply_env_token_substitution(text)


def get_max_parallel_rules() -> int:
    return max(1, int(os.getenv("GRE_MAX_PARALLEL_RULES", "1")))


def get_max_parallel_for_connection(source_type: str) -> int:
    return max(1, int(os.getenv(f"GRE_{source_type.upper()}_MAX_PARALLEL", "1")))


_READINESS_CHECKS: Dict[str, Callable[[str, object], bool]] = {}


def register_readiness_check(rule_group: str, fn: Callable[[str, object], bool]) -> None:
    _READINESS_CHECKS[rule_group] = fn


def check_run_ready(rule_group: str, run_key: str, meta_conn=None) -> bool:
    fn = _READINESS_CHECKS.get(rule_group)
    if fn is None:
        return True
    return bool(fn(run_key, meta_conn))
