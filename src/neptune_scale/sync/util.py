import signal


def safe_signal_name(signum: int) -> str:
    try:
        signame = signal.Signals(signum).name
    except ValueError:
        signame = str(signum)

    return signame
