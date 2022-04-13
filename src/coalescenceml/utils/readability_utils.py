
def get_human_readable_time(seconds: float) -> str:
    """Convert seconds into a human-readable string.
    
    Args:
        seconds: number of seconds
    
    Returns:
        String that displays time in Days Hours, Minutes, Seconds format
    """
    prefix = "-" if seconds < 0 else ""
    seconds = abs(seconds)
    int_seconds = int(seconds)
    days, int_seconds = divmod(int_seconds, 86400)
    hours, int_seconds = divmod(int_seconds, 3600)
    minutes, int_seconds = divmod(int_seconds, 60)
    if days > 0:
        time_string = f"{days}d{hours}h{minutes}m{int_seconds}s"
    elif hours > 0:
        time_string = f"{hours}h{minutes}m{int_seconds}s"
    elif minutes > 0:
        time_string = f"{minutes}m{int_seconds}s"
    else:
        time_string = f"{seconds:.3f}s"

    return prefix + time_string