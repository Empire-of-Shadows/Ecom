from datetime import datetime, timezone


def utc_now_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


def utc_today_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def utc_week_key() -> str:
    iso = datetime.now(timezone.utc).isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def utc_month_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def ctx(**kwargs) -> str:
    """
    Enhanced context helper with performance metrics and additional data points
    """
    parts = []
    gid = kwargs.get("guild_id")
    uid = kwargs.get("user_id")
    lvl = kwargs.get("level")
    extra = kwargs.get("extra")
    perf = kwargs.get("performance_ms")
    prestige = kwargs.get("prestige_level")

    if gid is not None:
        parts.append(f"G:{gid}")
    if uid is not None:
        parts.append(f"U:{uid}")
    if lvl is not None:
        parts.append(f"L:{lvl}")
    if prestige is not None:
        parts.append(f"P:{prestige}")
    if perf is not None:
        parts.append(f"⏱️{perf:.2f}ms")
    if extra:
        parts.append(str(extra))
    return " ".join(parts)
