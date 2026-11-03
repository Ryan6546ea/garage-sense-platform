"""Container readiness probe."""

from urllib.request import urlopen

from event_recorder.config import RecorderSettings


def main() -> None:
    """Exit nonzero unless the recorder is fully ready."""

    settings = RecorderSettings()
    with urlopen(
        f"http://127.0.0.1:{settings.api_port}/health/ready",
        timeout=2,
    ) as response:
        if response.status != 200:
            raise RuntimeError(f"readiness probe returned HTTP {response.status}")


if __name__ == "__main__":
    main()
